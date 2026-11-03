# Plan A 部分对象缓存验收与成本（2026-11-03 更新）

本报告按[规格 #37](https://github.com/Monsterwi/razor/issues/37)、[验收任务 #46](https://github.com/Monsterwi/razor/issues/46)和[最新已接受 ADR](../adr/0001-partial-object-caching-plan-a.md)更新。当前集成源码为 `6260620573d0fe601e51d83249a03878dd120ed8`；新专机测量正在执行，**此版不宣称新契约已完成最终验收或 PR 已可审查**。

该版本已完成全仓 Go、cache/server 完整 race、真实进程保守模式 20 项、Otter/S3-FIFO 各 18 项和 34 项工具测试（无 skip），并通过独立集成的 13 个定向 race 入口各两轮。本机二进制 SHA256 为 `3f4b34f4aed103199c3b827ab73c25c49c13a40768fc0852799821fe1b1c8481`。最终整分支审查、CI 与新专机结果仍待汇总；本机通过不代替专机测量。

[15 场景 / 34 Stories / 35 实施决策矩阵](partial_object_cache_2026-11-02-matrix.md)已按新契约更新。现有 [JSON](partial_object_cache_2026-11-02.json)和 [CSV](partial_object_cache_2026-11-02.csv)仍是 **2026-11-02、`a37f325` 的历史结果**；其旧末字节保护与取消行为不能证明当前实现，旧数值没有改名为新测量。旧归档 `plan-a46-20261102T150920Z` 保留；新批次 `plan-a46-final-20261103T023131Z` 实际完成后，才能替换正文表格和附件原值。

## 当前行为与验收边界

固定布局为 1 MiB/chunk、64 chunk/segment；普通 GET 与单 Range 共用内部对象元数据、完成位图、数据和在途回源，无独立 manifest 文件。回源可以对齐扩大，客户端只收到自身范围和正确状态、头部。

**客户端响应完成与缓存块提交分开。** 校验上游头部后，已到达的字节可以立即输出，所需字节到齐即可正常结束，包括单字节 Range。当前块独立接收对齐尾部并验证，只有完整有效的块才发布目录和完成位。尾部随后失败不能撤回已经成功结束的响应；该块不得成为有效命中，仍缺少所需字节的读者必须失败。

最后读者正常结束或断开后，只继续已经开始的当前块；没有已开始块时取消，无需求时不启动下一块或新回源。两个块的合并 Range、冷普通 GET 都不是后台下载完整对象的许可。后来的相容读者可加入仍在填充但暂时零订阅者的任务。任务期限、服务关闭、存储错误、PURGE 围栏和不因换版在当前请求内重试的规则保持有效。

`tools/partial_byte_test.py` 的四项真实进程测试分别证明：

- 源站只给 16 KiB 时，单字节响应已返回完整正确正文及连接 EOF；后来的小范围读者加入同一源流，全部读者退出后仍完成当前 1 MiB，整块后续热读零新增回源。
- 最后读者在当前块中途 RST，HTTP handler 已退出，当前块仍完整填充；没有额外源请求，后续整块可复用。
- 第一块已完整、第二块仍在途时，第二块内的单字节响应先成功；对齐尾部截断使尚未满足的大范围读者失败，完整第一块保留，失败第二块由下一独立请求重取。
- 客户端已完成并有真实响应记录时，统计组仍等待当前块收尾；最后计入完整 1 MiB，下一热组源请求和正文均为零。

这四项先在核心候选 `3615f28` 上通过，随后全 34 项工具测试在上述最终本机二进制通过。Go 测试另覆盖无需求后续块停止、任务超时、服务关闭，以及独立旧块跨 PURGE/新版本不能重新发布。

## 本轮发现与修复

初轮组合验收发现重复回源：8 个客户端访问四个相交的 16 MiB 范围，联合为 40 MiB，正文均正确，但源站收到 22 GET /44 MiB，超过严格预期的 20 GET /40 MiB。segment metadata 替换时，未与发布持同一锁的 coverage 读取可能把临时不可读当作缺块。`59351d3` 使读取和发布共用已有 segment 条带锁，不增加常驻对象表或逐块探测。原 RED/GREEN 记录保留，新版本仍须验证严格联合范围，不放宽流量断言。

整分支审查发现：最后一次 `Read` 同时返回恰好填满块的字节和非 EOF 错误，后续 `Read` 再返回 EOF 时，Range 原先可能遗漏前一个错误并提交块。`58b3e2d` 保留并返回该终态错误。新契约允许已经收到全部所需字节的客户端成功，但该错误仍必须阻止缓存提交；客户端成功不能替代块完整性验证。回归使用既有 `WithTransport` 与真实前端 HTTP，没有声称标准网络 transport 必然产生同一 Reader 序列。

独立当前块填充还暴露同代次刷新窗口：刷新客户端已经拿到所需字节，而替换块仍在提交；旧响应读取该块时可遇到 `ErrWriteInProgress` 或 `ErrNotResident`。已复现的错误链是旧块暂时不可读、repair 返回 nil、后续共享任务 claim 因请求 identity 不相容而被拒绝；**不是旧对象 Revision 直接拒绝 repair**。`22c4e2e` 对已开始响应做有界的完成等待和一次原 key 的 committed 重读，保留原表示、取消和身份边界，不订阅另一请求的 writer，也不切换新版。确定性双 gate 测试在刷新尾部未放行时恢复旧响应，避免用“等待刷新完成”的测试同步掩盖此窗口。

两处旧工具前提也已修正：客户端 If-None-Match 正确透传到源站并得到 304/零正文；合格部分对象的 no-cache 刷新记 MISS，旧完整对象路径才可能记 REVALIDATE。验收仍检查源站次数、转发头、正文和具体访问日志序列。

## PURGE、恢复与错误

PURGE 后新请求不得进入旧代次；已开始旧响应若仍能取得原版本字节，可以继续。当前旧块独立填充仍受发布围栏约束，晚结束不能恢复旧代次。不无界保留整个旧对象的历史块；慢读者可能按原 ETag 补取已经产出但不可访问的 chunk。补取须是已产出范围内的对齐单块、原版本、每块至多一次；额外流量单独统计，不能宣称跨 PURGE 永远没有额外源正文。旧版具体补取数值留在历史附件，新版必须重测。

正常恢复用例先明确等待块可靠完成，再保存与重启，不把小 Range 的客户端 EOF 当作块已提交。稀疏对象保留洞，重启只补缺块；普通 GET 写入的块可被后续 Range 复用。SIGKILL 和发布/保存窗口验证不能确认的数据安全 miss，不承诺保留全部命中。

data/segment 短写、EIO、ENOSPC、data Sync 和 checkpoint Rename 故障有缓存接口测试。临时 checkpoint 文件 Sync、父目录 Sync 未分别注入；进程 SIGKILL 也不是物理断电实验。健康 stripe 恢复的覆盖提示仍需由实际 chunk 读取验证，不能把置位当作绝对可读保证。

## 新测量的时间与计量窗口

每组开始前用现有 `drain(product)` 排空前组或预热的独立尾部，再取基线。客户端完成后先记录真实响应窗口，继续资源采样，直到本组填充结束后才取源站、IO 和缓存计数。这样不会漏算尾部流量或把它算入下一热组。

| 字段 | 定义与用途 |
| --- | --- |
| `seconds` | 本组客户端 workers 完成的墙钟时间；QPS、客户端 MiB/s 使用此分母。 |
| `ttfb_ms` / `completion_ms` | 实际 HTTP 请求到第一个正文 byte / 完整客户端响应的时间，不计独立填充等待。 |
| `fill_settle_seconds` | 客户端全部结束到 `drain` 检查完成的时间；包含轮询、采样和调度开销，**不是纯落盘延迟**。 |
| `observation_seconds` | 客户端窗口加填充收尾窗口，说明完整观测时长。 |
| `origin` / 源与客户端正文比 | 完整观测窗口结束后计源 HEAD/GET/正文，除以成功校验的客户端 bytes；小 Range 表示对齐放大，并发组也含在途共享效果。 |
| `logical_byte_hit_fraction` | `RangeHitBytes / (RangeHitBytes + RangeMissBytes)`；与 RAM lookup hit 分开，不能替代实际源站流量。 |
| `harness_cpu_cores` | 客户端窗口内源站/校验客户端进程 CPU 时间除以 `seconds`。 |
| 服务 CPU、IO、RSS 与缓存计数 | 观测到填充收尾结束；RSS 每 100 ms 采样，是整体进程，非 bitmap 专属内存或硬上限。 |

真实闸门回归证明旧计量在单字节响应完成时只统计 16 KiB；修正后统计完整 1 MiB，下一热组为零。闸门人为控制收尾时间，其耗时不作为正式性能数值。

新专机测量继续固定源码与二进制、256 KiB/100 MiB fixture、GOMAXPROCS=8、10 MiB RAM 和 1 GiB stripe。HDD/SSD 分开顺序执行，各 23 类场景三轮，不与 race 或其他压力任务并行。源站、服务和校验客户端同机 loopback，buffered I/O，不清全局 OS page cache。应用冷 URL 不等于物理冷盘，介质标签仅说明缓存文件位置；实际磁盘读写是否发生须以新样本为准。

三轮分位数的中位数不等于合并全部请求后的分位数。每响应 SHA、访问日志及同机客户端/源站会影响吞吐。单客户端冷样本每轮只有一次请求，不能由其 P99 推断尾延迟分布。没有匹配旧基线测量，不宣称性能改善。

元数据复用 `BenchmarkPartialCoverage`：`object-B`、`segment-B` 是编码载荷，`record-B` 还包含 HTTP metadata 和 Doc 头；固定目录、内存 identity 载荷、key/selector 字符串分别计量，均不代表全部 Go 分配或 RSS。32 MiB stripe 样例只有一个 chunk 与一个 segment 的两条索引，对象 metadata 仅测编码。各介质须设置同介质 `TMPDIR`；最终原值到齐后重填，不沿用旧源码的 ns/op。

## 最终复现与待填证据

最终版本应记录源码 SHA/clean 状态、Razor/cache.test SHA256、构建 Go/CGO、运行 Python、配置、fixture 摘要、背景负载和每条命令退出状态。公开工作路径以变量代替，不修改私有原始归档。

```sh
go test ./... -count=1 -timeout 180s
go test -race ./... -count=1 -timeout 300s
go build -o "$BIN" .
RAZOR_ACCEPTANCE_BINARY="$BIN" RAZOR_CHECKED_HEY="$HEY" \
  python3 -m unittest discover -s tools -p '*test.py' -v
python3 tools/acceptance.py --binary "$BIN" --output "$OUT/conservative" \
  --products razor --mode functional --large-mib 100
python3 tools/acceptance.py --binary "$BIN" --output "$OUT/public" \
  --products razor --public-static --ram-algorithm "$ALG" --mode public-functional --large-mib 100

CGO_ENABLED=0 go build -o "$BIN_STATIC" .
CGO_ENABLED=0 go test -c -o "$CACHE_TEST" ./cache
python3 tools/acceptance.py --binary "$BIN_STATIC" --output "$OUT/benchmark" \
  --cache-root "$CACHE_ROOT" --products razor --public-static --mode benchmark \
  --large-mib 100 --ram-bytes 10485760 --ram-algorithm otter --seconds 1 --repeats 3
GOMAXPROCS=8 TMPDIR="$META_TMP" "$CACHE_TEST" -test.run '^$' \
  -test.bench '^BenchmarkPartialCoverage$' -test.benchmem -test.count 3 -test.benchtime 1s
```

`ALG` 在本机功能验收分别取 Otter/S3-FIFO，专机按冻结配置执行；`CACHE_ROOT`/`META_TMP` 是对应介质上的全新批次目录。结果到齐后填入每介质 69 组数据、实际响应数、源计数、客户端和观测窗口、TTFB、吞吐、RSS、IO、metadata 三轮原值，并重新验证矩阵和附件。

## 限制与交付状态

格式升级会让不兼容旧缓存失效，不做迁移，部署需考虑冷回源。多 Range、缺少强 ETag/总长或客户端条件请求按约定旁路。

当前阶段没有新契约下的最终专机测量、匹配旧基线收益、ATS 对比、WAN/TLS/H2 压力、两小时长测或硬件断电结果。历史附件不能补齐这些空缺。旧 CI 断连用例通过明确等待缓存提交修正测试同步，也不等同当前候选所有 CI 已通过。完整分支两轴审查与合并批准由交付流程单独记录，本报告不表示默认分支已合并、部署或发布。
