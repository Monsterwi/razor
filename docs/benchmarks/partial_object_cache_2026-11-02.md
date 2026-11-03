# Plan A 部分对象缓存验收与成本（2026-11-03 更新）

本报告按[规格 #37](https://github.com/Monsterwi/razor/issues/37)、[验收任务 #46](https://github.com/Monsterwi/razor/issues/46)和[最新已接受 ADR](../adr/0001-partial-object-caching-plan-a.md)更新。专机测量固定源码为 `6260620573d0fe601e51d83249a03878dd120ed8`，HDD/SSD 各 18 项功能验收及 69 组测量通过，合计 **40,468 个校验响应、0 错误**。最终测试修正为 `ef41a509d4c8d9b6a20de6ce9dc703eb20adc2c1`，仅修改测试同步，构建产物与 6260620 逐字节相同；不把测量写成对任意后续 HEAD 的实测。

该版本已完成全仓 Go、cache/server 完整 race、真实进程保守模式 20 项、Otter/S3-FIFO 各 18 项和 34 项工具测试（无 skip），并通过独立集成的 13 个定向 race 入口各两轮。本机二进制 SHA256 为 `3f4b34f4aed103199c3b827ab73c25c49c13a40768fc0852799821fe1b1c8481`。最终整分支 Standards/Spec 审查与远端 CI 由主交付流程另行汇总，本报告不预先声明 PR 可审查。

[15 场景 / 34 Stories / 35 实施决策矩阵](partial_object_cache_2026-11-02-matrix.md)已按新契约更新。[紧凑 JSON](partial_object_cache_2026-11-02.json)保存当前版本的命令、退出码、完整样本、目录/metadata 与功能结果；[CSV](partial_object_cache_2026-11-02.csv)保存全部 138 组新测量。新原始归档为 `plan-a46-final-20261103T023131Z`；旧 `a37f325` 归档 `plan-a46-20261102T150920Z` 独立保留，不用于当前数值。沿用附件文件名是为保留已有链接。

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

初轮组合验收发现重复回源：8 个客户端访问四个相交的 16 MiB 范围，联合为 40 MiB，正文均正确，但源站收到 22 GET /44 MiB，超过严格预期的 20 GET /40 MiB。segment metadata 替换时，未与发布持同一锁的 coverage 读取可能把临时不可读当作缺块。`59351d3` 使读取和发布共用已有 segment 条带锁，不增加常驻对象表或逐块探测。原 RED/GREEN 记录保留，本轮保守真实进程用例仍验证严格 20 GET /40 MiB，后续热读零回源，没有放宽流量断言。

整分支审查发现：最后一次 `Read` 同时返回恰好填满块的字节和非 EOF 错误，后续 `Read` 再返回 EOF 时，Range 原先可能遗漏前一个错误并提交块。`58b3e2d` 保留并返回该终态错误。新契约允许已经收到全部所需字节的客户端成功，但该错误仍必须阻止缓存提交；客户端成功不能替代块完整性验证。回归使用既有 `WithTransport` 与真实前端 HTTP，没有声称标准网络 transport 必然产生同一 Reader 序列。

独立当前块填充还暴露同代次刷新窗口：刷新客户端已经拿到所需字节，而替换块仍在提交；旧响应读取该块时可遇到 `ErrWriteInProgress` 或 `ErrNotResident`。已复现的错误链是旧块暂时不可读、repair 返回 nil、后续共享任务 claim 因请求 identity 不相容而被拒绝；**不是旧对象 Revision 直接拒绝 repair**。`22c4e2e` 对已开始响应做有界的完成等待和一次原 key 的 committed 重读，保留原表示、取消和身份边界，不订阅另一请求的 writer，也不切换新版。确定性双 gate 测试在刷新尾部未放行时恢复旧响应，避免用“等待刷新完成”的测试同步掩盖此窗口。

两处旧工具前提也已修正：客户端 If-None-Match 正确透传到源站并得到 304/零正文；合格部分对象的 no-cache 刷新记 MISS，旧完整对象路径才可能记 REVALIDATE。验收仍检查源站次数、转发头、正文和具体访问日志序列。

## PURGE、恢复与错误

PURGE 后新请求不得进入旧代次；已开始旧响应若仍能取得原版本字节，可以继续。当前旧块独立填充仍受发布围栏约束，晚结束不能恢复旧代次。不无界保留整个旧对象的历史块；慢读者可能按原 ETag 补取已经产出但不可访问的 chunk。补取须是已产出范围内的对齐单块、原版本、每块至多一次；额外流量单独统计，不能宣称跨 PURGE 永远没有额外源正文。本轮专机两介质的 exact/prefix 慢旧 GET 样例各有 2 个普通 GET 加 96 个对齐单块 repair，0 HEAD，源正文共 296 MiB（两个 100 MiB 正文加 96 MiB 历史补取）。这是受控慢读者样例的代价，不是每次 PURGE 必有的额外量；新代次后续 GET/Range 仍零回源。原始功能 JSON 保留逐例结果。

正常恢复用例先明确等待块可靠完成，再保存与重启，不把小 Range 的客户端 EOF 当作块已提交。稀疏对象保留洞，重启只补缺块；普通 GET 写入的块可被后续 Range 复用。SIGKILL 和发布/保存窗口验证不能确认的数据安全 miss，不承诺保留全部命中。

data/segment 短写、EIO、ENOSPC、data Sync 和 checkpoint Rename 故障有缓存接口测试。临时 checkpoint 文件 Sync、父目录 Sync 未分别注入；进程 SIGKILL 也不是物理断电实验。健康 stripe 恢复的覆盖提示仍需由实际 chunk 读取验证，不能把置位当作绝对可读保证。

## 时间与计量窗口

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

本次专机测量固定源码与二进制、256 KiB/100 MiB fixture、GOMAXPROCS=8、10 MiB RAM 和 1 GiB stripe。HDD/SSD 分开顺序执行，各 23 类场景三轮，不与 race 或其他压力任务并行。源站、服务和校验客户端同机 loopback，buffered I/O，不清全局 OS page cache。应用冷 URL 不等于物理冷盘，介质标签仅说明缓存文件位置；本批实际磁盘读取为零，具体写入与资源开销见下表和附件。

三轮分位数的中位数不等于合并全部请求后的分位数。每响应 SHA、访问日志及同机客户端/源站会影响吞吐。单客户端冷样本每轮只有一次请求，不能由其 P99 推断尾延迟分布。没有匹配旧基线测量，不宣称性能改善。

元数据复用 `BenchmarkPartialCoverage`：`object-B`、`segment-B` 是编码载荷，`record-B` 还包含 HTTP metadata 和 Doc 头；固定目录、内存 identity 载荷、key/selector 字符串分别计量，均不代表全部 Go 分配或 RSS。32 MiB stripe 样例只有一个 chunk 与一个 segment 的两条索引，对象 metadata 仅测编码。本次各介质均设置同介质 `TMPDIR`，下表使用新源码的三轮原值。

## 专机测量结果

专机使用 CentOS 7 / XFS，2 × Xeon E5-2620 v4、16 物理核 /32 逻辑核、约 62.64 GiB 内存。HDD 是 7200 rpm SAS `ST4000NM0295`，SSD 是 SATA `INTEL SSDSC2BB800G4`。Go 1.26.10 / CGO_ENABLED=0 在开发机静态构建，专机 Python 3.12.13 执行；HDD、SSD 各类组顺序运行，hot 组至少 1 秒，重复三轮。

专机 Razor SHA256：`eadb92bbc84d1aea219016f0fe0cce1e4284ddb002c3099dbd4efdf6c7a6599e`；cache.test：`2d4e6e92f27c887a8852b18099f73cae87088c549f1788651fe0a4c940ffd840`。

**138 组样本的 Razor 进程实际磁盘读 bytes 与设备采样读 bytes 均为 0。** 下表差异不能解释为 HDD/SSD 物理冷盘读取差异。进程写 bytes 合计 HDD 1,574,928,384、SSD 1,561,448,448；buffered I/O 的设备写入时刻可能落在采样窗口之外，不能将设备增量当作逐组同步耐久性计量。harness CPU 峰值约 16.70/16.77 核，源站/客户端 SHA 校验与服务同机竞争，吞吐不是服务器容量上限。

以下指标为三轮各自指标的中位数，RSS 使用三轮最大采样值。单次冷样本的 P50/P99 相等，不代表尾延迟分布。小 Range 从 offset 123 开始。

| 介质 | Range | 冷/热 | 每轮 GET / HEAD | 源/客户端正文比（倍） | 逻辑 byte hit | 正文 TTFB P50 / P99 ms | 客户端窗口 ms | 填充收尾 ms | 观测窗口 ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| HDD | 1 B | 冷 | 1 / 1 | 1,048,576 | 0% | 5.031 / 5.031 | 8.489 | 45.541 | 66.730 |
| HDD | 1 B | 热 | 0 / 0 | 0 | 100% | 1.396 / 2.962 | 1001.551 | 1.477 | 1003.028 |
| HDD | 4096 B | 冷 | 1 / 1 | 256 | 0% | 3.824 / 3.824 | 4.471 | 63.206 | 66.882 |
| HDD | 4096 B | 热 | 0 / 0 | 0 | 100% | 1.261 / 2.023 | 1001.353 | 2.341 | 1003.765 |
| SSD | 1 B | 冷 | 1 / 1 | 1,048,576 | 0% | 3.854 / 3.854 | 4.353 | 62.691 | 67.043 |
| SSD | 1 B | 热 | 0 / 0 | 0 | 100% | 1.239 / 2.170 | 1001.139 | 1.770 | 1003.022 |
| SSD | 4096 B | 冷 | 1 / 1 | 256 | 0% | 3.521 / 3.521 | 4.149 | 43.652 | 47.800 |
| SSD | 4096 B | 热 | 0 / 0 | 0 | 100% | 1.202 / 2.312 | 1000.720 | 1.503 | 1002.479 |

客户端窗口列是整组 workers 的墙钟时间，热组运行约一秒，不能作为单请求耗时。三列分别取中位数，展示值不必相加相等；每条原始样本的时间恒等式已逐行验证。收尾含轮询/采样开销，即使无在途填充也可能非零。

冷 1 B 与 4 KiB 每轮都回源完整 1 MiB，对齐放大分别为 **1,048,576 倍**与 **256 倍**；热组源请求/正文均为零。新契约下 1 B 和 4 KiB 都可在对齐尾部完成前结束，真实 gate 用例证明了该时序；没有把提前响应误记为少回源。HDD 冷 1 B 三轮正文 TTFB 为 **5.031、3.083、115.124 ms**，115 ms 样本完整保留，未删异常值；本测量不能单独归因其产生原因。

| 介质 | 100 MiB GET | 并发 | 客户端 MiB/s | 正文 TTFB P50 / P99 ms | RSS 最大采样 MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| HDD | 冷 | 1 | 127.49 | 3.420 / 3.420 | 103.68 |
| HDD | 冷 | 8 | 786.40 | 30.104 / 32.119 | 94.52 |
| HDD | 冷 | 32 | 1174.48 | 84.941 / 91.870 | 123.56 |
| HDD | 热 | 1 | 130.10 | 2.290 / 2.877 | 70.06 |
| HDD | 热 | 8 | 882.40 | 4.771 / 17.358 | 99.38 |
| HDD | 热 | 32 | 1226.02 | 38.952 / 57.345 | 114.45 |
| SSD | 冷 | 1 | 126.32 | 3.447 / 3.447 | 97.16 |
| SSD | 冷 | 8 | 845.50 | 25.648 / 27.186 | 99.97 |
| SSD | 冷 | 32 | 1178.35 | 86.440 / 93.671 | 118.21 |
| SSD | 热 | 1 | 125.22 | 2.793 / 2.991 | 66.48 |
| SSD | 热 | 8 | 884.99 | 4.796 / 14.827 | 96.83 |
| SSD | 热 | 32 | 1236.66 | 34.917 / 52.114 | 112.87 |

每个冷 100 MiB GET 组，无论并发 1/8/32 都只有 1 个普通源 GET、0 HEAD、100 MiB 源正文。源/客户端正文比分别为 1、1/8、1/32，逻辑 byte hit 均为 0，收益来自在途共享而不是已完成缓存命中。对应热组零回源、逻辑 byte hit 为 100%。冷并发 gate 等全部订阅者加入后才放行首字节，TTFB 包含同步等待，不能当作自然到达流量的固有延迟。

benchmark overlap 组是 0/4/8/12 MiB 起点、各 16 MiB、8 个客户端，联合 **28 MiB**；每介质三轮均 14 GET +1 HEAD、28 MiB 源正文、128 MiB 校验客户端正文，源/客户端比 0.21875。保守功能验收的 **40 MiB** union 使用另一组偏移，两者没有混淆。包含 drag、慢源站、慢客户端、mixed、longtail、小对象等在内的全部 **23 类场景 ×3×2** 原值均保存在附件。

整体 RSS 峰值 HDD **123.56 MiB**、SSD **118.21 MiB**，是逐组运行同一服务进程的整体采样，不能归因于 bitmap 或当作任意负载的硬上限。

| metadata 操作 | HDD ns/op 中位数 | SSD ns/op 中位数 | HDD / SSD B/op 中位数 | HDD / SSD allocs/op 范围 |
| --- | ---: | ---: | ---: | --- |
| read | 23,138 | 22,733 | 3,272 / 3,272 | 44–44 / 44–44 |
| publish | 88,980 | 88,067 | 14,386 / 14,390 | 155–156 / 156–156 |

两介质编码成本相同：object metadata **158 B**，segment payload **155 B**，含 HTTP metadata 与 Doc 头的 record **300 B**。32 MiB stripe 的固定目录 **40,962 B /4,096 slots**，内存 identity 载荷 **65,536 B**，两条索引的 key/selector 文本 **432 B**。8 B coverage 位图只是完整成本的一部分；没有匹配旧版本基准，不能宣称锁或位图带来净性能改善。

专机 13 条 runner 命令全部 exit 0；结束时测试进程为空、源码 clean，两个二进制 SHA256 复核一致。各介质约 1.7 GiB 隔离测试缓存保留，未覆盖原缓存。完整归档保留请求、源站日志、配置、背景负载和原始 metadata 输出；JSON 保存归档 SHA256。

## 已完成验证与复现

| 检查 | 固定版本与结果 | 证据日志 |
| --- | --- | --- |
| 全仓 Go | 6260620，exit 0 | `stream-fill-final-go2.log` |
| cache/server 完整 race | 6260620，exit 0；157.408 s 墙钟 | `stream-fill-final-race2.log` |
| 工具与真实进程 | 34 tools、0 skip；保守20、Otter18、S3-FIFO18 全通过 | `stream-fill-final-tools.log`、对应 functional 日志 |
| 独立集成 race | 13 个 stream-fill/终态入口，各2轮，exit 0 | `merge-stream-fill-race-test.log` |
| CI 原全仓 coverage 命令 | ef41a50，exit 0；测试同步修正后全仓通过 | `stream-fill-ci-coverage-fixed.log` |
| 资源统计日志修复 | `b521691`；38 tools、0 skip、exit 0；短 soak 5 轮通过 | `stats-log-full-tools.log`、`stats-log-short-soak-repeat.log` |
| 专机 | 两介质各18 functional、69 benchmark、metadata read/publish各3轮；13命令exit 0 | 新批次 `command-exits.json` |

`ef41a50` 修复的是缓存已提交断言的测试前提：旧 fixture 在客户端 EOF 后立即把尾块当成已提交缓存，coverage 重复出现 8/50 次失败；诊断的 9/100 次失败均仍有一个 fill task 和 writer。只在保留缓存断言前等待已有公开 Stats 排空，保持在途旧 412、不重试、响应字节和 GET=3/4 断言；三种模式 ×100 coverage、相关 race ×20 及全仓 coverage 随后通过。该四行测试修改未改变生产或测量工具，ef41a50 与 6260620 构建逐字节相同。最终远端 CI 和整分支审查结果仍由主交付记录确认。

JSON 已记录源码 clean 状态、Razor/cache.test SHA256、构建 Go/CGO、运行 Python、配置、fixture 摘要、背景负载和每条命令退出状态。公开工作路径以变量代替，私有原始归档不变。

```sh
export GOMAXPROCS=8
go test ./... -count=1 -timeout 180s
go test -race ./cache ./server -count=1 -timeout 360s
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

`ALG` 在本机功能验收分别取 Otter/S3-FIFO，专机按冻结配置执行；`CACHE_ROOT`/`META_TMP` 是对应介质上的全新批次目录。附件包含每介质 69 组数据及 metadata 各操作三轮原值；138 行的 `observation_seconds = seconds + fill_settle_seconds` 精确成立，源/客户端正文比例与吞吐分母均已逐行复算。

## 限制与交付状态

格式升级会让不兼容旧缓存失效，不做迁移，部署需考虑冷回源。多 Range、缺少强 ETag/总长或客户端条件请求按约定旁路。

本批没有匹配旧基线收益、ATS 对比、WAN/TLS/H2 压力、两小时长测或硬件断电结果。历史附件不能补齐这些空缺。完整分支两轴审查与合并批准由交付流程单独记录，本报告不表示默认分支已合并、部署或发布。

资源统计日志读取修复已提交为 `b521691d1fe73f2c264e6414ee48976aa95980ca`：两个解析入口只处理以换行结束的完整记录，保留未完成尾部的字节偏移，完整坏 JSON 仍报错。4 个新增测试使工具套件从原 34 项增为 38 项；完整套件通过（0 skip、35.585 s），原分段写入复现与定向套件各重复 20 次通过，真实进程七阶段短 soak 重复 5 次通过。日志及实际退出结果见 JSON 的 `resource_stats_log_followup`。生产代码、指标定义和 6260620 专机测量数值未变；原 34 项证据继续保留。最终远端 CI 与两轴审查以 [PR #47](https://github.com/Monsterwi/razor/pull/47) 最新 checks 和主交付流程汇总为准。
