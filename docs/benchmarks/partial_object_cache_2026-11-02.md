# Plan A 部分对象缓存验收与成本（2026-11-02）

本报告对应 [规格 #37](https://github.com/Monsterwi/razor/issues/37) 和 [组合验收 #46](https://github.com/Monsterwi/razor/issues/46)。固定评审基线为 `cc4e7b6330df455d00476b9b412247e61b49ac6a`；功能采用 1 MiB chunk、每 segment 64 个 chunk 的内部覆盖位图，普通 GET 与单 Range 共用数据与在途回源，无独立 manifest 文件。

本机正确性验收已完成：保守模式 20 项、Otter 与 S3-FIFO public 模式各 18 项、工具测试 32 项全部通过且没有 skip。完整 Go 与 race 结果及退出码见附件。专机 HDD/SSD 各 18 项功能与 69 组成本测量全部通过，合计 40,147 个校验响应、0 错误；本机 overlay 上的功能用例不作为性能结果。

附件：[15 场景 / 34 Stories / 35 实施决策矩阵](partial_object_cache_2026-11-02-matrix.md) · [紧凑原值与命令 JSON](partial_object_cache_2026-11-02.json) · [138 组原值 CSV](partial_object_cache_2026-11-02.csv)。运行日志保存在任务归档 `46-final-*`；JSON 包含完整 Go 结果、工具汇总、逐场景结果及精确源站计数，审阅无需访问临时目录。

## 版本与复现

| 用途 | 固定版本 |
| --- | --- |
| 本机功能二进制源码 | `5c8699c3382794f750c5eeed1f9717ed77624e3d` |
| 本机二进制 SHA256 | `304f5d4d09892773f2aae90e71dc34136b7060151b9ea2526405ee9daec66b9f` |
| 功能工具与最终 Go/race 源码 | `a37f325504d661469bbb4b68af736f8a79d1ec3a`；相对上一行仅工具日志断言变更 |
| 本机运行时 | Go 1.26.10、Python 3.12.3、服务 GOMAXPROCS=8；overlay、HTTP/1.1 loopback |
| 专机静态二进制源码 | `a37f325504d661469bbb4b68af736f8a79d1ec3a`；Go 1.26.10、CGO_ENABLED=0 |
| 专机 Razor SHA256 | `bde4f61bf35db0c69dd6530b215abb91a8631eb2d5d5b715c62a1ce3c6bc457a` |
| 专机 cache.test SHA256 | `3d9213a0cc3be323761836b3981c94dc9568c8bc32443dd95e1e948325da60ab` |
| checked hey SHA256 | `b61a5fddd268a30e269f46f11485513243a9b408bddcb220587299288b4bac41` |

以下命令从仓库根目录执行，`BIN` 为构建出的绝对二进制路径，`HEY` 为 `tools/build_hey.py` 产物，`OUT` 为全新输出目录。测试源码和生成的配置均随对应提交可复现。

```sh
go test ./... -count=1 -timeout 180s
go test -race ./... -count=1 -timeout 300s
go build -o "$BIN" .
RAZOR_ACCEPTANCE_BINARY="$BIN" RAZOR_CHECKED_HEY="$HEY" \
  python3 -m unittest discover -s tools -p '*_test.py' -v
python3 tools/acceptance.py --binary "$BIN" --output "$OUT/conservative" \
  --products razor --mode functional --large-mib 100
python3 tools/acceptance.py --binary "$BIN" --output "$OUT/otter" \
  --products razor --public-static --ram-algorithm otter --mode public-functional --large-mib 100
python3 tools/acceptance.py --binary "$BIN" --output "$OUT/s3-fifo" \
  --products razor --public-static --ram-algorithm s3-fifo --mode public-functional --large-mib 100
```

公开功能验收逐响应校验 SHA256、实际长度及状态；针对 Range、恢复与 RWW 的专门用例还检查 Content-Range、Content-Length、ETag、精确回源区间及闸门时序。Python `Product` 启动独立 Razor 进程；Go 的 TCP 用例经 `httptest.NewServer` 和真实 HTTP 客户端，但 handler 与测试同进程。Recorder 用例仅补充边界。磁盘故障、并发位更新和保存窗口使用已确认的缓存接口，不新增生产调试端点。

## 本轮发现与修复

组合验收发现了实际重复回源：8 个客户端访问 4 个相交的 16 MiB 区间，联合范围为 40 MiB，客户端正文均正确，但源站收到 22 次 GET、44 MiB，超过严格预期的 20 次 GET、40 MiB。segment metadata 替换期间，未加同一锁的 coverage 读取会把活跃 writer 或临时目录空窗当作 miss。

修复 `59351d3c1264b4c4b5893cf820d2e9ec0e85943c` 使公开 coverage 读取与发布共用已有 segment 条带锁；发布调用内部读取助手，避免重入。没有增加常驻对象表或逐 chunk 查询。缓存接口回归旧实现 5/5 失败，修复后 20 轮通过；原始 100 MiB fixture 的真实进程场景连续 5 轮均为 20 GET +1 HEAD、41,943,040 源站 bytes，134,217,728 客户端 bytes 全部正确。最终保守验收再验证冷并发 8 的同样计数，以及热并发 32 的零回源。

两处旧工具断言也按规格修正：客户端 If-None-Match 应透传，源站 304 零正文，不能要求其不回源；合格部分对象的 no-cache 刷新记录 MISS，旧完整对象路径才记录 REVALIDATE。最终工具检查具体的 MISS/HIT/MISS/BYPASS 序列、3 次源站 GET、转发条件头和 304 零正文，没有放松字节或回源合并要求。

单字节场景复用了已有生产能力并补两项真实进程测试。源站闸门只放出 16 KiB 时，多字节 follower 已收到正确的 8191-byte 前缀，但 `bytes=7-7` 尚无正文；完整 1 MiB 校验后才返回准确的一个 byte。若源站在 32 KiB 截断，单字节响应无正文而失败，follower 最后 byte 也失败；同一请求不重试，下次独立请求补取后方可成为热命中。

## PURGE 与恢复的成本边界

有界旧私有流不会保留整个被 PURGE 对象的所有历史 chunk 指针。慢读者落后于已完成的旧生产者时，可以按原 ETag 补取已经产出但当前不可访问的旧 chunk；新请求仍只能进入新代次。这是已开始旧响应续流与有界缓存之间的成本，不能宣称跨 PURGE 的冷 GET 永远只下载一次。

本机双闸门功能场景中，旧、新两个 100 MiB GET 同时跨 exact/prefix PURGE，旧响应保留完整旧 SHA，新响应与后续热请求均为新版。两种 RAM 策略各场景实际得到 2 个普通 GET +96 个单块旧版 repair、0 HEAD，源正文 310,378,496 bytes，其中额外 repair 为 100,663,296 bytes（96 MiB）。验收要求每次 repair 仅为原 ETag、对齐单块、范围在请求开始时旧普通 GET 已产出的字节内、每块至多一次；源 bytes 必须精确等于两份完整对象加合法 repair 长度之和。不是允许任意重复或新版重试。

Range 跨两个 run 的旧流在源站仍提供原版本时继续；原 If-Match 被拒绝时，在已发送旧前缀后失败，没有版本重试，新代次仍可复用。正常关闭后稀疏对象依旧保留洞，重启仅补缺少的 chunk；完整 GET 的数据重启后可被 Range 零回源复用。旧私有流晚于新流完成也不能写回 checkpoint。

异常 SIGKILL 和 7 个数据/位图/保存窗口证明无法确认的数据安全 miss；不要求保留命中。跨 stripe 保存失败可以留下健康 metadata 的覆盖提示，实际 chunk 读取仍必须验证并修复。data/segment 短写、EIO、ENOSPC、data Sync 和 checkpoint Rename 失败已覆盖；临时 checkpoint 文件 Sync、父目录 Sync 未分别注入故障，不能把错误返回源码当作这些故障的实测。进程 SIGKILL 也不是物理断电实验。

## 专机实测结果

专机为 CentOS 7 / XFS，2 × Xeon E5-2620 v4、16 物理核 /32 逻辑核、约 62.64 GiB 内存。HDD 为 7200 rpm SAS `ST4000NM0295`，SSD 为 SATA `INTEL SSDSC2BB800G4`。使用 buffered I/O；顺序执行 HDD 功能与测量，再 SSD 功能与测量，最后分别在两介质 TMPDIR 中运行 metadata 基准。各 timed hot 组至少 1 秒，三轮；冷组每客户端一次请求。

**两介质的 138 组样本中，Razor 进程实际磁盘读 bytes 均为 0，设备采样读 bytes 也为 0。** 因而介质标签表示缓存文件放置位置，不能将表中差异解释为 HDD/SSD 随机读或物理冷盘性能。应用冷对象仍发生写入；未清 OS page cache。Python 源站、校验客户端和 Razor 同机，harness CPU 峰值见 JSON，HDD 达 16.76 核；这些数值不是服务器容量上限。

以下为三轮指标中位数；RSS 是各组三轮的最大采样值。TTFB 单位 ms，小 Range 都从 offset 123 开始。源站 GET/HEAD 和 bytes 为每轮的实际增量。

| 介质 | Range | 冷/热 | GET / HEAD | 源 bytes / 客户端 bytes | 逻辑 byte hit | TTFB P50 / P99 ms | QPS |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| HDD | 1 B | 冷 | 1 / 1 | 1,048,576 | 0% | 11.458 / 11.458 | 78.3 |
| HDD | 1 B | 热 | 0 / 0 | 0 | 100% | 1.284 / 2.587 | 680.1 |
| HDD | 4096 B | 冷 | 1 / 1 | 256 | 0% | 2.866 / 2.866 | 111.4 |
| HDD | 4096 B | 热 | 0 / 0 | 0 | 100% | 1.401 / 2.928 | 610.2 |
| SSD | 1 B | 冷 | 1 / 1 | 1,048,576 | 0% | 9.131 / 9.131 | 103.2 |
| SSD | 1 B | 热 | 0 / 0 | 0 | 100% | 1.305 / 2.648 | 684.6 |
| SSD | 4096 B | 冷 | 1 / 1 | 256 | 0% | 3.460 / 3.460 | 93.4 |
| SSD | 4096 B | 热 | 0 / 0 | 0 | 100% | 1.346 / 2.365 | 659.6 |

冷 1 B 与 4 KiB 每轮均下载一个完整 1 MiB 对齐块，放大分别为 **1,048,576 倍**与 **256 倍**；热请求源正文为零。冷样本每轮只有一次请求，其 P50/P99 相等，不具备尾延迟分布意义。1 B 等待完整块验证，多字节可先收到前缀，差异有独立闸门测试支持，不能仅由上表延迟推断。

| 介质 | 100 MiB GET | 并发 | MiB/s | TTFB P50 / P99 ms | RSS 最大采样 MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| HDD | 冷 | 1 | 126.89 | 3.221 / 3.221 | 104.07 |
| HDD | 冷 | 8 | 831.74 | 48.439 / 50.283 | 97.14 |
| HDD | 冷 | 32 | 1167.39 | 88.051 / 94.333 | 125.82 |
| HDD | 热 | 1 | 126.42 | 2.612 / 2.821 | 69.24 |
| HDD | 热 | 8 | 877.57 | 5.167 / 12.435 | 95.63 |
| HDD | 热 | 32 | 1247.00 | 36.570 / 49.946 | 119.71 |
| SSD | 冷 | 1 | 123.70 | 3.415 / 3.415 | 102.18 |
| SSD | 冷 | 8 | 806.57 | 30.900 / 32.281 | 103.53 |
| SSD | 冷 | 32 | 1150.57 | 93.052 / 100.450 | 123.04 |
| SSD | 热 | 1 | 129.20 | 2.394 / 2.504 | 65.38 |
| SSD | 热 | 8 | 866.50 | 3.446 / 11.670 | 88.52 |
| SSD | 热 | 32 | 1173.04 | 31.579 / 59.082 | 108.07 |

每个冷 GET 组，不论并发 1/8/32，都只发生 **1 个普通源 GET、0 HEAD、100 MiB 源正文**；源/客户端正文比例分别为 1、1/8、1/32，逻辑 byte hit 均为 0，体现的是在途共享。对应热组源请求及正文为 0，逻辑 byte hit 为 100%。冷并发组用闸门等待全部订阅者加入才释放首字节，TTFB 包含这段同步等待，不能当作自然到达流量的固有延迟。

benchmark 的 overlap 组采用 0/4/8/12 MiB 起点、各 16 MiB、8 个客户端，联合 28 MiB：两介质三轮均 14 GET +1 HEAD、28 MiB 源正文，128 MiB 客户端正文正确；源/客户端比为 0.21875，逻辑 byte hit 仍为 0。它与前述保守功能验收的 40 MiB union 是两个不同样例。drag、慢源站、慢客户端、mixed、longtail 和 256 KiB 小对象等全部 23 类场景三轮原值全部保存在 CSV/JSON，没有用选取的表格替代完整结果。

整体 RSS 峰值：HDD **125.82 MiB**，SSD **123.04 MiB**。这是同一进程逐组累积运行后的整体采样值，不能归因于某一个对象或当作最大内存保证。

| metadata 操作 | HDD ns/op 中位数 | SSD ns/op 中位数 | B/op 中位数 | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| read | 21,442 | 20,773 | 3,272 | 44 |
| publish | 88,616 | 96,811 | 14,387 | 156 |

两介质样例编码大小相同：对象 metadata **158 B**，segment payload **155 B**，含 HTTP 元数据与 Doc 头的 segment record **300 B**。32 MiB stripe 的固定目录 **40,962 B /4,096 slots**，内存 identity 载荷 **65,536 B**，两条索引记录的 key/selector 文本 **432 B**。这说明 8 B 位图只占完整成本的一部分；read/publish 的分配与锁成本也必须计入。无匹配旧版本微基准，不能宣称此次锁或位图带来净收益。

复现专机测量（`CACHE_ROOT` 与 `META_TMP` 必须是对应介质上的全新批次目录；两介质顺序运行）：

```sh
CGO_ENABLED=0 go build -o "$BIN" .
CGO_ENABLED=0 go test -c -o "$CACHE_TEST" ./cache
python3 tools/acceptance.py --binary "$BIN" --output "$OUT" \
  --cache-root "$CACHE_ROOT" --products razor --public-static --mode benchmark \
  --large-mib 100 --ram-bytes 10485760 --ram-algorithm otter --seconds 1 --repeats 3
GOMAXPROCS=8 TMPDIR="$META_TMP" "$CACHE_TEST" -test.run '^$' \
  -test.bench '^BenchmarkPartialCoverage$' -test.benchmem -test.count 3 -test.benchtime 1s
```

原始归档批次 `plan-a46-20261102T150920Z` 保留全部请求、源站日志、配置、环境、背景 vmstat/iostat、命令退出状态和完整 metadata 输出。全部 14 条 runner 命令以及两介质 metadata 命令 exit 0；结束时测试进程为空，源码 clean，静态二进制摘要复核一致。每介质约 1.7 GiB 的隔离测试缓存保留，未覆盖已有缓存或服务。

## 测量定义与解释

正式测量使用同一固定源码、配置和 fixture，逐组顺序施压、每场景三轮；不与 race 或另一压力任务并行。源站、Razor 与校验客户端同机、在仅含 loopback 的隔离 network namespace 中运行，服务 GOMAXPROCS=8，应用 RAM 10 MiB、stripe 1 GiB。热样本先预热，冷样本使用全新 URL；没有清全局 OS page cache。“冷”指应用对象不存在，不代表物理冷盘。

- 源站请求分别计 HEAD、GET，正文 bytes 不含 HTTP 头。对齐放大为源站正文字节 / 客户端成功校验的正文字节；冷单客户端小 Range 可直接解释为对齐成本，并发组该值还包含共享回源节约。
- 逻辑 byte hit 为 `RangeHitBytes / (RangeHitBytes + RangeMissBytes)`，与 RAM lookup hit 分开。在途共享不必被记作已完成缓存命中，因此该比例不能替代实际源站流量。
- TTFB 从请求开始到第一个正文 byte；响应最后一个 byte 等待对应 chunk 完整校验。1-byte 冷 Range 因而等待整块，4 KiB 等多字节请求可提前收到前缀。正文完整 SHA 和时序闸门分别验证正确性与 RWW。
- 吞吐包含客户端逐响应 SHA 校验、访问日志与 loopback 开销，不是独立服务极限。报告三轮各自指标的中位数，不把三轮 P99 的中位数当作所有请求合并后的 P99。
- RSS 每 100 ms 采样，表示整个进程，既不是 bitmap 专属成本，也不是峰值硬上限。设备 I/O 如存在，包含该设备同期其他进程，不是 Razor 独占物理读写。

元数据基准复用 `BenchmarkPartialCoverage`。`segment-B` 是编码的整条 segment payload，不只是 8-byte 位图；`record-B` 加入该样例 HTTP 元数据和 72-byte Doc 头，仍不含分配对齐或索引；`object-B` 是对象 metadata 编码长度。固定目录 `directory-B` 按 `BinarySize()` 统计，`identity-payload-B` 是内存 identity 数组载荷，`index-text-B` 只计实际 key/selector 字符串，不含 Go map/trie/allocator。样例为 32 MiB stripe、一个 chunk + 一个 segment 的两条索引记录；对象 metadata 测量编码但没有作为第三条记录写入该微基准。不得将其直接乘以对象数推算总 RSS。

## 限制与交付边界

格式升级会让不兼容旧缓存失效，不做迁移，部署时需要考虑冷回源；测试确认不会清理无关文件或其他 cache store。测试覆盖固定布局、支持的共享缓存准入与单 Range；多 Range、缺少强 ETag/总长或客户端条件请求按约定旁路。

本轮没有匹配旧基线吞吐收益、ATS 对比、WAN/TLS/H2 压力、两小时长测或硬件断电数据。短 soak 仅为工具烟测，历史 2026-10-24 结果不能补齐这些空缺。完整分支审查与合并批准由交付流程另行执行；本报告本身不代表默认分支已合并、部署或发布。

本报告冻结时，源码 `a37f325` 的一轮远端 CI（run `41469320226`）在 `TestClientFailureDoesNotDiscardCompletedObject` 出现源调用次数 2 的失败，同 SHA 另一次 build 成功。本机完整 Go/race 和本报告专机测量均已完成，但该间歇性 CI 失败仍需后续修复、复验与全分支审查；本报告不宣称全部 CI 绿色或 PR 已可审查。
