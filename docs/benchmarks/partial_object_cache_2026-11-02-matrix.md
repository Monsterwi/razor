# Plan A 规格与验收映射

本附录按 [规格 #37](https://github.com/Monsterwi/razor/issues/37) 的 15 行 HTTP 矩阵、34 条 User Stories 与 35 条 Implementation Decisions 逐项映射测试。固定源码、二进制摘要、完整命令退出码和成本数据见[验收报告](partial_object_cache_2026-11-02.md)与[紧凑证据](partial_object_cache_2026-11-02.json)。测试名称及路径在 `a37f325504d661469bbb4b68af736f8a79d1ec3a` 核对存在；全仓 Go/race 和全部工具测试实际运行通过，不以静态映射本身代替执行结果。

Go TCP 用例通过 `httptest.NewServer`、HTTP 客户端与本地源站传输，但 handler 在测试进程内；Python `Product` 用例启动独立 Razor 进程。缓存 API 仅补故障、竞争与恢复。`TestRangeBoundariesAndIfRange`、`TestRangeRawEncodingVariantsKeepIndependentBlocks`、`TestPurgeFencesOriginRequestBeforeHeaders` 使用前端 Recorder，作为补充，不冒称独立 TCP/进程验收。

以下命令在仓库根目录运行；`BIN` 指向报告中记录源码及 SHA256 的实际二进制。所有 Python 进程用例必须设置 `RAZOR_ACCEPTANCE_BINARY="$BIN"`，skip 不计通过。矩阵中的全仓测试结果对应本次完整运行，单项命令供定向复现。

## 15 行 HTTP 验收矩阵

| ID / 父规格场景 | 实际可运行入口 | 可观察断言与证据边界 | 来源任务 |
| --- | --- | --- | --- |
| M1 Range geometry | `go test ./server -run '^(TestRangeCombinesAdjacentMissingChunksWithOriginalClientBounds\|TestRangeSegmentBoundaryShortTailAndMaximumLength\|TestRangeSmallAndEmptyObjectsDoNotCreateNonexistentChunks\|TestRangeBoundariesAndIfRange)$' -count=1` | 前三组 TCP HTTP 验证原请求精确 bytes、206、Content-Length/Range；跨 64 MiB segment 的 17-byte 请求；suffix/open/clamped 短尾；最大长度安全旁路；0/17-byte 对象、416 且零虚构块。边界 Recorder 测试补 If-Range。新增独立进程 `RAZOR_ACCEPTANCE_BINARY="$BIN" PYTHONPATH=tools python3 -m unittest partial_byte_test -v` 验证 1-byte 冷 Range 精确成功和截断 gate（2 tests，exit 0）；见主报告的单字节场景。 | #39、#40、#42、#46 |
| M2 Range reuse | `go test ./server -run '^(TestRangeRunsReuseOutOfOrderCoverageWithoutFetchingAcrossHits\|TestRangeRunJoinsOverlappingInFlightIntervalWithoutDuplicateBytes\|TestConcurrentRunsPublishDifferentBitsInSameSegment)$' -count=1` | TCP HTTP 验证乱序缓存后按缺口补取；源站 Range 列表精确匹配已知区间，不跨已有 HIT 补取；覆盖在途区间的第二请求已拿到自己正确前缀；同 segment 并发完成后全部内容仍可复用。无重复正文、不能仅依赖统计 HIT。 | #40 |
| M3 GET to Range | `go test ./server -run '^TestColdGETStreamsOnceAndFeedsRangeCache$' -count=1`；`RAZOR_ACCEPTANCE_BINARY="$BIN" PYTHONPATH=tools python3 -m unittest partial_recovery_test.PartialRecoveryTest.test_clean_restart_reuses_sparse_ranges_and_get_chunks -v` | TCP HTTP 的完整 GET 后多个非对齐/尾部 Range 字节正确，GET 始终 1、HEAD 0。Python 另验证真实进程重启后原 GET 内容被跨块 Range 复用，精确响应几何且无新源站请求。 | #41、#45 |
| M4 Range to GET | `go test ./server -run '^TestGETReusesInFlightRangeAndOnlyFetchesRemainingChunks$' -count=1`；同 M3 Python 重启用例 | TCP HTTP：已有 chunk 1 的在途 Range，GET 复用它，仅补其余明确范围；两个响应为独立 200/206 和正确正文。Python 稀疏对象只存 0、2、短末块，真实重启后的 GET 只补缺少的 chunk 1（1 MiB、If-Match v1），随后相交 Range 零新增回源。 | #41、#45 |
| M5 Cold GET | `go test ./server -run '^(TestColdGETStreamsOnceAndFeedsRangeCache\|TestGETAfterNoCompletedColdChunksUsesOneOrdinaryGET\|TestColdGETUnsupportedResponseUsesItsSingleOriginResponse)$' -count=1` | TCP HTTP：源站仅先发 6 KiB 时客户端已收 4096 bytes，源站普通 GET=1/HEAD=0；全部返回精确完整数据。仅 metadata、没有完整块的后续冷 GET 仍单普通 GET。无准入条件时正确复用同一原始响应做旁路，而非另发一个 GET。 | #41 |
| M6 Shared RWW | `go test ./server -run '^(TestRangeStreamsWithinIncompleteChunkAndReusesCompletedChunk\|TestRangeJoinsColdGETBeforeOriginHeaders\|TestGETJoinsColdRangeDiscoveryBeforeHEADCompletes\|TestGETReusesInFlightRangeAndOnlyFetchesRemainingChunks)$' -count=1` | TCP HTTP：源站闸门在 chunk 未收齐处保持关闭，各订阅者已经拿到正确前缀；各自状态和范围独立，源站请求/字节计数排除重复抓取。覆盖 GET 尚未返回头时 Range 加入、Range HEAD 未返回时 GET 接管、在途 Range→GET。**末 byte 保护另见下方边界说明。** | #38、#39、#41 |
| M7 Slow/disconnected readers | `go test ./server -run '^(TestSharedGETSlowCreatorDoesNotBackpressureOtherSubscriber\|TestColdGETCancellationKeepsFutureRangeAndCommittedChunks\|TestSharedGETAllStreamingSubscribersLeaveCancelsOrigin\|TestCancelledRangeRunKeepsCommittedPrefix)$' -count=1` | TCP HTTP：慢 creator 未读完时另一读者及后续 committed HIT 能完成；creator 离开时未来 Range 订阅仍完整；最后需求消失触发源站 context cancellation；已完整前缀随后可复用且不增加源站 GET。检查任务/读者/writer/buffer 排空，慢读者测试约束 response buffer allocation 不超容量。 | #38、#40、#41 |
| M8 Truncation and errors | `go test ./server -run '^(TestIncompleteOrInvalidChunkIsNeverReused\|TestTruncatedRangeRunKeepsCompletedChunkAndRefillsOnlyIncompleteTail\|TestSharedGETTruncationAfterPrefixFailsEveryReaderAndNextRequestRefills)$' -count=1`；`go test ./cache -run '^(TestPartialStorageFailureNeverPublishesReadableCoverage\|TestStorageFailuresCannotCommitObjects)$' -count=1` | TCP HTTP：截断、错误 Content-Range、200 版别变化不能成功；失败请求只有一次源站 GET，后续正确请求重建；合并 run 的完整首块保留、只补不完整尾块；共享读者错误退出而非成功 EOF。cache API 对 data/segment 短写、EIO、ENOSPC 验证错误传播、不可读和未发布 coverage。 | #38–#40、#43 |
| M9 Version change | `go test ./server -run '^(TestKnownObjectReplacementKeepsOldHTTPStream\|TestUnchangedObjectRefreshKeepsOldHTTPStream\|TestRangeVersionChangeRejectsMixedBodyAndRecovers\|TestLateOldRangeFailureCannotFenceNewGeneration)$' -count=1` | TCP HTTP：较新 metadata 出现后，已有可取得旧 bytes 的响应完整返回旧 ETag/旧正文且零新增源站 GET；缺少旧尾块时只发送旧前缀后中断，精确 GET=2 排除当前请求重试，下一请求完整新版。晚到旧失败不得撤销已经开始的新代次。 | #42 |
| M10 Cache admission | `go test ./server -run '^(TestPartialIneligibleResponsesNeverBecomeSharedHits\|TestPartialUnsupportedRequestsRetainOriginSemantics\|TestPartialAuthorityAndLanguageVariantsDoNotShareBytes\|TestPartialQueryOrderIdentityAndExactPrefixPurge\|TestPartialMetadataRefreshDoesNotExtendChunkFreshness\|TestRangeRawEncodingVariantsKeepIndependentBlocks)$' -count=1` | 主要 TCP HTTP：缺失/弱 ETag、unknown length、private/no-store、不支持 Vary 重复请求仍回源；条件、Authorization、Cookie、gzip、多 Range 保留传入头与源站语义，不能进入新部分缓存路径。authority、查询项顺序、language 及 encoding 隔离；同 ETag metadata 刷新不延长旧 chunk freshness。 | #41、#42 |
| M11 PURGE race | `go test ./server -run '^(TestPartialPurgeKeepsStartedResponseAndSeparatesNewGeneration\|TestPartialPurgeOldRangeVersionFailureDoesNotRetryOrRetireNew\|TestPartialPurgeOldCursorsShareSubsequentPrivateRun\|TestPartialPurgeSparseExactAndPrefixLeaveOtherObjects\|TestPartialPurgeDoesNotInterruptNonTargetActiveGET\|TestPartialPurgeFailureCannotReturnSuccess\|TestPurgeFencesOriginRequestBeforeHeaders)$' -count=1`；`RAZOR_ACCEPTANCE_BINARY="$BIN" PYTHONPATH=tools python3 -m unittest public_acceptance_test.PublicToolsTest.test_real_binary_purge_keeps_started_stream_and_fences_late_publication -v` | TCP/独立进程双 gate 均先读到客户端旧前缀，再 PURGE、新版独立开始；旧任务退出不能取消新版；旧可取得数据跨 run 继续，旧 If-Match 412 时中断且不重试；后续新 GET/Range 仍新版并零回源。稀疏 exact/prefix、不相关活跃对象、inflight-only、失败不能返回成功均有测试。额外历史 chunk repair 严格按旧版、已产出范围、对齐单块、每块最多一次计数。 | #44 |
| M12 Eviction/repair race | `go test ./server -run '^(TestRangeRepairsMissingCoverageEvenWhenRefillFails\|TestRangeCorruptChunkRefillsWithoutFetchingOtherHits\|TestRangeMissingSegmentUsesBoundedMissWithoutReconstructingChunks\|TestRangeCompletedSlotCorruptionRefillsAfterResponseStarted)$' -count=1`；`go test ./cache -run '^(TestPartialLateRepairPreservesRefilledChunk\|TestPartialConcurrentRepairAndPublicationsPreserveOtherChunks\|TestPartialRepairAfterMissingSegmentCannotEraseRecreatedCoverage)$' -count=1` | TCP HTTP 加实际缓存删除/磁盘损坏：只补坏块，不重抓有效块；失败补块也移除旧不可用提示；缺 segment 保守 miss 而非全量 chunk 扫描；已开始响应仍可同版修复。缓存 API 竞争闸门证明旧 repair 不能清掉较新 refill，丢失后重建的 segment 也受 Revision 保护。 | #43 |
| M13 Normal restart | `RAZOR_ACCEPTANCE_BINARY="$BIN" PYTHONPATH=tools python3 -m unittest partial_recovery_test.PartialRecoveryTest.test_clean_restart_reuses_sparse_ranges_and_get_chunks partial_recovery_test.PartialRecoveryTest.test_late_private_stream_cannot_restore_purged_generation -v` | 独立 Razor SIGTERM exit0 后用相同存储重启；有洞稀疏 Range 与 GET 内容 byte/hash/头部正确、已存部分源站零新增；旧私有 GET/Range 晚于新版完成后保存，恢复仍只能新版，零重取。cache clean-close 对照还验证 metadata/coverage/chunk 都能恢复。 | #45 |
| M14 Abnormal restart/storage failure | `RAZOR_ACCEPTANCE_BINARY="$BIN" PYTHONPATH=tools python3 -m unittest partial_recovery_test.PartialRecoveryTest.test_kill_during_second_chunk_restarts_with_safe_miss partial_recovery_test.PartialRecoveryTest.test_exact_and_prefix_purge_survive_clean_and_killed_restart -v`；`go test ./cache -run '^(TestPartialRecoveryPublicationAndCheckpointKillWindows\|TestPartialRecoveryRepairsCoverageWhenOtherStripeCannotSaveData\|TestPartialCheckpointPublishFailureIsReportedAndDoesNotPreventOtherStripeClose\|TestStorageSaveFailureClosesEveryStripe)$' -count=1` | Python 在第二块未完成且客户端已有 prefix 后 SIGKILL，旧连接结束后取 baseline，再启动完整新版且单普通 GET；PURGE exact/prefix × clean/kill 不复活。cache 子进程 7 窗口（before-data、partial-data、chunk-published、partial-bitmap、bitmap-published、before-clean-index、clean-close），未可靠保存全部 miss、clean-close 完整恢复。跨 stripe data Sync EIO 使健康 metadata 可恢复但 data 必须不可读，repair/refill 可恢复；Rename 失败返回错误且不妨碍其他 stripe 关闭。不是断电硬件实验。 | #43、#45 |
| M15 Format upgrade | `go test ./cache -run '^TestPartialRecoveryRejectsOldFormatsWithoutChangingUnrelatedFiles$' -count=1` | cache API 正常关闭/重开后拒绝旧 object version、错误 chunk layout、旧 segment version、legacy namespace、损坏 checkpoint；业务文件、无关 sidecar、另一 cache data/checkpoint 字节完全不变，另一 cache 可正常打开读正确内容。无迁移或递归清理。 | #45 |

以上命令只是针对性复现入口；本轮完整执行结果见主报告，包括全仓 `go test ./...`、全仓 `-race` 和所有 tools unittest 的实际退出结果。独立进程 public-functional 两 RAM 策略、保守 functional、Python 恢复矩阵分别证明组合路径；不需要把 Recorder/缓存 API 测试描述成独立进程 HTTP。

最新只读补查基线：`a37f325504d661469bbb4b68af736f8a79d1ec3a`，包含 `5c8699c` 合入的修复。M2 增补 cache `TestPartialCoverageRemainsReadableDuringPublication`（发布已完成位时，并发读者不能瞬间丢失已有覆盖）；M12 增补 TCP HTTP `TestCompletedRunPrefixCanRefillWhileFollowingChunkIsInFlight`（原 run 第二块仍受源 gate 阻塞，磁盘滚动已覆盖的完整首块可独立补回，后续原 run 仍完成且不重复抓取）。这些是 #46 新回归，不能只引用早先 #40/#43 的测试记录作为它们的执行证据。

## 34 条 User Stories 对照

| Story | 主要映射/补充 | 只读核查结论 |
| --- | --- | --- |
| 1 完整 GET | M3–M5 | 有精确完整正文和 200 契约。 |
| 2 精确 Range bytes/headers | M1、M6 | 有 Content-Length/Range 和精确正文契约。 |
| 3 open/suffix | M1 | 包含非对齐尾和短末块。 |
| 4 unsatisfiable | M1 | 0/17-byte 对象与越界 416，不取虚构块。 |
| 5 overlap/out-of-order | M2 | 源站区间列表排除重抓命中。 |
| 6 Range→GET | M4 | 在途与完成的部分内容均有复用契约。 |
| 7 GET→Range | M3 | 完成普通 GET 的数据在 Range、重启后 Range 中复用。 |
| 8 cold GET 单请求 | M5 | 普通 GET=1、HEAD=0，包括仅 metadata 无完整块。 |
| 9 只补缺块 | M2、M4、M12 | 有洞、坏块及缺 segment 的行为明确。 |
| 10 相邻缺块合并 | M1、M2 | 精确合并后的对齐源站区间，不能跨 HIT。 |
| 11 chunk 未完成先 RWW | M6；单字节测试 | 多字节前缀有确定性闸门；末 byte 必须等对应 chunk 校验。新增单字节独立进程用例验证完整成功和截断拒绝，失败后只能重新补块。 |
| 12 并发 GET/Range 共用在途 | M6 | 双向订阅及 headers/HEAD 尚未完成时交接均有契约。 |
| 13 独立位置和边界 | M1、M6 | 同一源流上的不同 Range 和 GET/Range 精确切片。 |
| 14 其他客户端取消不破坏自己 | M7 | creator 离开、future Range/其他订阅继续。 |
| 15 无需求即取消 | M7 | 源站 context cancellation 与资源排空。 |
| 16 取消保留完整块 | M7、M8 | 下一请求可复用已完整前缀；不完整尾需新回源。 |
| 17 不混版本 | M9 | 对 old/new bytes 做精确比较，不能只看 ETag。 |
| 18 仍可取得旧 bytes 则完成 | M9、M11 | metadata 替换和 PURGE 都覆盖旧响应继续。 |
| 19 旧缺块不可得则失败 | M9、M11 | 流式 206 截断，只有旧前缀，无替代错误正文或新 bytes。 |
| 20 当前请求不因版本重试 | M8、M9、M11 | 精确源站次数/原 If-Match 412；后续新请求独立重建。 |
| 21 完整验证才复用 | M8 | 截断下一请求重取，已完整块可复用。 |
| 22 错误通知 RWW 读者 | M8 | 所有已读 prefix 的共享读者都失败而非正常 EOF。 |
| 23 淘汰/覆盖变 miss | M12 | 实际 CRC 损坏、目录丢失、滚盘与补取。 |
| 24 旧 repair 不抹新 refill | M12 | 明确闸门、Revision 和并发位更新测试。 |
| 25 PURGE 不复活旧代次 | M11、M13、M14 | 新请求隔离、晚到旧流、保存后重启均覆盖。 |
| 26 PURGE 允许已发响应续流 | M11 | 当前旧源流和后续私有旧 Range 可继续；旧源拒绝则失败。 |
| 27 正常重启保留可靠数据 | M13 | 稀疏仍有洞、完整 GET、late private flow 四种组合。 |
| 28 异常退出不优先保 HIT | M14 | 7 个发布/保存窗口与真实 Razor SIGKILL。 |
| 29 升级失效不迁移 | M15 | 格式/namespace 拒绝且无关文件不改。 |
| 30 无准入/不支持请求正确透传 | M10、M5 | 头部保留、正确正文/状态、源请求次数排除错误缓存。 |
| 31 identity/admission | M10 | Host、原始 query 顺序、origin、language/encoding、freshness。 |
| 32 有界缓冲/受控工作 | `TestPublicGET100MiBThroughMinimumBudgetAndDiskWindow`、`TestSharedGETSlowCreatorDoesNotBackpressureOtherSubscriber`、`TestS3ConcurrentSlowRangeSurvivesRepeatedDiskWrap`、`TestSharedFillBudgetsRejectWithoutOriginRetry` | 100 MiB 经 32 MiB 磁盘与最低 writer reservation 完整 SHA；response buffer allocation 不超容量；慢读并发滚盘以及超预算不额外回源。只能证明这些有界配置/负载，不是任意规模 heap 上限证明。 |
| 33 HTTP 级正确性证据 | M1–M15 | TCP bytes/headers/origin/gate 和独立进程验证齐备；必须填入本轮实际执行结果。 |
| 34 流量与缓存开销分别测量 | `public_benchmark`、`Client.batch`、`BenchmarkPartialCoverage`；#46 专用机正式测量 | 专机 HDD/SSD 各69组、三轮固定样例及metadata三轮已完成；源字节、对齐放大、逻辑命中、TTFB、吞吐、RSS与编码/目录成本见主报告及JSON/CSV。 |

## Implementation Decisions 全量核对（I01–I35）

编号严格按父规格 Implementation Decisions 各小节内的原始条目顺序分配：Domain and layout 为 I01–I06，Unified HTTP 为 I07–I15，Version consistency 为 I16–I19，RWW and publication 为 I20–I28，Eviction/PURGE/recovery 为 I29–I35。共 35 条，之前 15 场景与 34 stories 的映射不能替代这些设计约束。以下“源码落点”只解释实现与契约的联系，不单独构成验收 PASS。

| ID / 要求 | 源码/文档落点 | 验收映射与证据限制 |
| --- | --- | --- |
| I01 segment 是对象逻辑范围；不是目录 partition，chunk 不是 fragment | `GLOSSARY.md`、ADR 0001；`cache/partial_object.go` 按对象 index 派生 key，底层仍走 CacheVC/fragment | 结构/术语约束；M1 跨 segment 精确内容佐证逻辑分组。不能由它推导数据连续落盘。 |
| I02 固定 1 MiB × 64，短尾按已知长度完整校验 | `PartialChunkBytes`、`PartialSegmentChunks`；`config/limits.go` 拒绝非 1 MiB；`fillGETChunk`/`fetchRangeRun` 使用对象末端长度 | `TestRangeBlockConfiguration` 检查配置；M1 几何/短尾、小空对象；M8/单字节测试 短体不能成为完整缓存。保留旧配置项但仅允许固定值，不是新增调参能力。 |
| I03 versioned object/segment 元数据，数据可归属 generation/index/有效长度 | `PartialObject` 保存 version/geometry/total/ETag/origin/variant/generation；segment 保存 identity/index/coverage；`ChunkKey` + 每块 Content-Range/Length | M9、M11、M15 验证版别/代次/格式隔离；M1/M8 验证长度。覆盖不是客户端请求相对分片号。 |
| I04 64 bit 只算 coverage；metadata 按需、有界 | `PartialCoverage` 每次读一个有界记录；`readPartialRecord` 限 64 KiB；`Cache.partialLocks` 固定 64 把；复用有容量的 RAM/disk cache | 无每对象常驻 bitmap map 的源码约束可确认。Story 32 只证明选定负载下缓冲受控；固定样例的目录/metadata/RSS 成本见专机测量；大规模趋势未测，不能声称总开销 8 bytes/segment。 |
| I05 object/segment/data namespace 分离；generation 与 ETag 不同 | `PartialObjectKey`、`segmentKey`、`ChunkKey` 使用各自 versioned namespace，identity 含独立随机 Generation；底层 `docKey` 哈希完整身份 | M3/M4 复用同 chunk 体系；M11/M15 保证同 URL/ETag 失效隔离及旧 namespace 不被误读。不宣称沿用旧整对象记录。 |
| I06 拒绝不兼容格式，不迁移、不删无关文件/store | `ReadPartialObject`、`readPartialCoverage` 与 checkpoint 解码校验 | M15 `TestPartialRecoveryRejectsOldFormatsWithoutChangingUnrelatedFiles`。 |
| I07 强 ETag、已知长度、共享资格才准入；open/suffix 支持，其他透传 | `rangeEligible`、`strongRangeTag`、`ensureRangeObject`、`fetchFill` | M1/M5/M10；重复请求和源站记录证明不误缓存。 |
| I08 authority、URL/query、variant、Vary/freshness/encoding 隔离 | `server/handler.go`、`server/cache_policy.go` 的请求身份与策略；`rangeIdentity` 保存 encoding/language；`identityEncoding` | M10 的 TCP identity/policy 测试；`TestRangeRawEncodingVariantsKeepIndependentBlocks` 只是 Recorder 补充。 |
| I09 GET/Range 共 coverage/在途，不要求额外完整对象副本 | `fillPartialGET` 只提交 object metadata 与 chunk；`serveKnownPartialGET`、`rangeCursor` 共用 chunk key/coverage | M3/M4/M6 以源站增量与正文证明双向复用；无重复完整对象是实现结构约束，不以 HIT 字样证明。 |
| I10 冷 GET 一个普通源 GET，并逐块发布；不准入正确透传 | `fetchFill` 从首个源响应选择 `fillPartialGET` 或原响应旁路 | M5 精确 GET=1、HEAD=0 和客户端前缀；`TestColdGETUnsupportedResponseUsesItsSingleOriginResponse`。 |
| I11 复用已有块/在途，缺口对齐且有界合并、不跨 HIT | `rangeCursor.openChunk`、`produceRangeRun`；`maxRangeRunChunks=2`，另受 segment 边界限制 | M2，新增 `TestCompletedRunPrefixCanRefillWhileFollowingChunkIsInFlight` 处理旧区间已有完整前缀不再可复用的情况；同一可用在途块不能独立重抓。 |
| I12 冷启动协调、GET/Range 双向订阅，绝对对象位置 | `partialGET`、`partialReady`、`rangeCursor`、`claimRange` | M4/M6 的响应头未完成 gate、偏移各异读者及重复源请求断言。 |
| I13 每请求独立状态、Content-Range/Length 与正文 | `serveBlocks` 按当前请求构建头部并裁剪字节；普通 GET/Range 分别 200/206 | M1、M3–M6 精确 bytes/头；416 小空对象无虚构 chunk。 |
| I14 源状态/CR/长度/强版本先验证再 RWW；200 不能当偏移块 | `fetchRangeRun` 在 `chunk.ready` 前核对 206、唯一 CR/ETag、total/length、encoding；错误返回 bypass/changed | M8 `TestIncompleteOrInvalidChunkIsNeverReused` 的 content-range/version-200；M9 已发响应失败不能附加替代 bytes；M10 不支持行为正确旁路。 |
| I15 metadata 可从响应发现/按 freshness 更新，不每请求 HEAD；ETag 不能代替 freshness | `fillPartialGET` 从普通 GET 建 metadata；`ensureRangeObject` 只对缺失/过期 metadata 发现；`requestFresh` 逐块检查 | M5 的 HEAD=0、M3/M4/单字节测试 hot 零新增请求；`TestPartialMetadataRefreshDoesNotExtendChunkFreshness`；`acceptance.functional` 新刷新序列检查属于 a37f325。 |
| I16 单响应固定版本，缺块请求约束/验证版本 | `rangeCursor.object`；`fetchRangeRun` 发 If-Match，并验精确 ETag | M9；M11 旧代次私有补取只使用旧 ETag。 |
| I17 新版出现不撤销仍可取得旧 bytes 的响应 | `checkRangeVersion` 检查缓存 fence，不以新 metadata 自动否决旧选择 | `TestKnownObjectReplacementKeepsOldHTTPStream`、`TestUnchangedObjectRefreshKeepsOldHTTPStream`；M11 旧源仍支持 If-Match 时跨 run 续流。 |
| I18 旧 bytes 不可得则当前失败，不重试、不追加错误或新版正文 | `serveBlocks`/`completedRangeReadError` 区分本地已完成坏块修复与源流终态失败 | M8/M9/M11；旧 If-Match 412 明确一次失败，后续独立请求可重建；单字节测试 截断不能成功完成。 |
| I19 URL/ETag 相同也不能跨代次复用晚到任务 | generation 参与 chunk/segment identity；`FillToken` fence；`InvalidatePartialObject` 比较 Generation/Revision | M11/M13/M14；`TestLateOldRangeFailureCannotFenceNewGeneration` 验证晚到旧错误不污染新版。 |
| I20 修复 shared writer/read-position 生命周期、偏移、同步 | `CacheWriter`/`CacheReader` 订阅引用、`partialGET` 与 `rangeCursor` 共用 CacheVC | M6/M7；cache `TestWriterSubscribersHaveIndependentProgressAndCancellation`，直接验证独立位置与取消。 |
| I21 在途可读与完整 coverage 分离，验证后才置位 | `chunk.ready` 暴露 RWW writer；`commitPartialChunk` 完整 commit 后才调用 `PublishPartialChunk` | M6/M8/单字节测试；多字节前缀先输出，响应最后 byte 等该 chunk 校验。 |
| I22 安全可读→data directory→coverage；运行态内存命中不等于 durable | `commitPartialChunk` 顺序 `fill.commit`、`CommittedRevision`、`PublishPartialChunk`；checkpoint 单独负责保存 | M8 的 data/segment write 故障、M14 发布/保存 kill 窗口；正常内存命中不作为断电恢复证据。 |
| I23 并发位更新不丢失，在途另跟踪、零 bit 不重复起源 | `updatePartialCoverage`/`PartialCoverage` 同分片锁串行读写；`claimRange` 管理在途区间 | `TestConcurrentRunsPublishDifferentBitsInSameSegment`、`TestPartialConcurrentRepairAndPublicationsPreserveOtherChunks`、新增 `TestPartialCoverageRemainsReadableDuringPublication`；M2/M6 共享在途源计数。 |
| I24 源任务独立于首客户端；特定 generation 的订阅有取消和完整通知 | fill group 任务 ctx/leave/finish 与 writer changed/done 通知 | M6/M7；cache `TestWriterAbortWakesEveryBlockedSubscriberWithCause`、`TestWriterSubscriberReceivesFinalBytesWhenWriteAndCloseRaceRead`。 |
| I25 引用配对，晚 close 不删新任务；发布/终态一致同步 | writer ownership、OpenDir entry 与 task identity 的条件删除/引用释放 | cache `TestLateWriterSubscriptionCannotJoinOrReleaseReplacement`、`TestConcurrentCloseAbortRetainsWriterAndReleasesEachReaderOnce`、`TestWriterTerminalStateReleasesOwnershipBeforeReadersObserveIt`；M11 新旧并存。 |
| I26 首慢读者不阻塞所有生产；缓冲有界，落后者可用已完成缓存 | `partialGET` 只保留当前 writer，run 固定窗口，reader/writer budget；已完成块走缓存读取 | M7/Story 32；`TestPublicGET100MiBThroughMinimumBudgetAndDiskWindow`、`TestCompletedRunPrefixCanRefillWhileFollowingChunkIsInFlight`。PURGE 后历史私有块 repair 有额外流量，不增加无界历史指针。 |
| I27 有需求继续，无需求取消；完整保留，不完整丢弃，失败通知 | fill group 订阅生命周期与逐 chunk commit/Abort | M7/M8；`TestCancelledRangeRunKeepsCommittedPrefix`、`TestSharedGETTruncationAfterPrefixFailsEveryReaderAndNextRequestRefills`。 |
| I28 Abort/storage error 穿透共享路径，Close 不提交失败数据 | `CacheWriter.Abort`、stripe failure 广播；私有已 PURGE writer 的 `watchStorage` | M8；cache `TestAbortedWriteWakesReadersAndCannotCommit`、`TestPurgedPartialBodyWaiterStopsOnOtherWriterStorageFailure`、`TestPurgedPartialHeaderWaiterStopsWhenCacheCloses`。 |
| I29 coverage 是提示；目录/data 有效性读时验证，坏数据 miss/repair/refill | `readRangeBlock` 验长度/身份与缓存读取 CRC；`repairCoverage` | M12；`TestRangeCorruptChunkRefillsWithoutFetchingOtherHits` 与滚盘/坏 committed slot；不能把恢复的 bit 当作正确数据证明。 |
| I30 repair 不清较新 refill；缺 segment 可整体 miss，不要求逐块扫描 | `PartialCoverage.Revision` 与 observed 完整快照比较；`readRangeCoverage` 缺失时保守 miss | M12；`TestPartialLateRepairPreservesRefilledChunk`、`TestPartialRepairAfterMissingSegmentCannotEraseRecreatedCoverage`、`TestRangeMissingSegmentUsesBoundedMissWithoutReconstructingChunks`。 |
| I31 PURGE 新请求隔离/旧 task 不发布，已开始响应可继续 | cache epoch fence、`OpenWritePartialChunk` 私有旧 writer、started `rangeCursor` 私有 run | M11 旧/新双 gate、M13/M14 晚流/重启；旧数据回收可推迟，不能误称磁盘已物理抹除。 |
| I32 exact/prefix 包括稀疏/仅在途对象；失败不报成功 | selector invalidation 与原子 fence，不按连续 chunk 编号删到首洞为止 | `TestPartialPurgeSparseExactAndPrefixLeaveOtherObjects`、`TestPartialPurgeFailureCannotReturnSuccess`；独立进程 purge/恢复矩阵。通用 pre-header case 是 Recorder 补充。 |
| I33 正常保存/恢复可靠，处理 user buffer、顺序、durability 错误、内容验证 | `Stripe.Close` 排空→data Sync→checkpoint temp write/Sync→Rename→dir Sync；load/record 校验 | M13/M14；跨 stripe 保存失败时 metadata 仅保留 coverage 提示，required data 读验证失败并 repair，不当有效 HIT。data Sync/Rename 已注入；temp-file Sync/dir Sync 未分别注入，是明确覆盖边界。 |
| I34 异常退出不确定状态可整体失效，无需逐块抢救/同步 durability | checkpoint clean/session/CRC/格式校验，异常 session 不接受旧 clean checkpoint | M14 的 7 窗口与真实进程 kill；不声称断电硬件级验证或保留全部 HIT。 |
| I35 handler 至持久化整个正确性链在范围内，复用 RAM/aggregate/disk | 当前修改贯穿 handler、fill group、reader/writer、OpenDir/DirMgr、invalidation/checkpoint；继续使用两种 RAM 策略与现有 stripe | M1–M15；两 RAM 的真实进程矩阵、全仓 Go/race 已在本次完整执行中通过，日志摘要见 JSON。无新增生产调试端点；本轮不做无关引擎重写。 |

核对结果：35 条都已映射。原 15 场景表未显式表达的 I01–I06、I20/I24/I25/I28 由上表补齐，并未据此发现新的整类实现空缺。I04 的固定样例成本与 Story 34 的正式测量已记录；任意规模趋势未测；I33 的两个独立 Sync 故障点只可标“未分别注入”。这些边界不能由源码、测试名称存在或全仓构建成功替代。

