# 块内 RWW 与 writer 生命周期

RWW 让请求读取确切 writer 已收到的字节；回源合并决定哪些请求共享生产者，两者职责不同。对象模型和 GET/Range 流程见 [Partial Object Cache](partial_object_cache.md)，共享身份及区间所有权见 [缓存填充](cache_fill_design.md)。

## 管线时序

1. 验证源站响应头的范围、强 ETag、长度、编码和共享准入，显式发送下游响应头，再读取可能失败的正文，避免复用连接误判为可自动重试的请求。
2. `CacheVC.OpenReader` 绑定确切 writer；读者按各自绝对偏移读取已到达字节，不等待整块、不保留最后一个字节。合格 GET 返回 200，单 Range 返回 206；不合格请求沿用普通代理语义。
3. 客户端所需字节到齐即可结束并释放订阅；当前 chunk 继续独立填充、校验，数据与目录提交后才置完成位。末块可以短于 1 MiB。
4. 每块有独立就绪和完成通知；无人需要时完成当前块后释放源响应及任务，不开始下一块，包括两块合并响应和冷 GET 的剩余正文。

## 提交与资源

| 事项 | 约束 |
| --- | --- |
| 写入 | 复用 CacheVC/cacheFill；一个 chunk 正文对应一个 Doc fragment，CRC、所有权和发布权围栏均有效。普通整对象可含多个 fragment，头片段最后写 |
| 完成/取消 | 块开始与最后订阅者离开使用同一同步边界；已开始范围包括等待 writer 额度。无已开始块则立即取消，其他订阅者不受单个客户端退出影响 |
| 当前块收尾 | 等价请求仍可加入；`SharedFillTasks > 0` 且 `SharedFillWaiters = 0` 可短暂出现。期限不重置，超时、关闭及存储失败仍终止任务 |
| 读取 | 完成位仅用于规划，命中仍校验内容；旧 reader 持有原 writer，关闭旧订阅不会移除同 key 新任务 |
| 淘汰 | 已完成块不永久驻留；若后续本地片段丢失，最多补取一次同代次单块，跳过已发送偏移，并保留后块需求。源站失败或版本变化不触发此补取 |

写者槽数为 `min(max_writers, write_buffer_bytes / CacheWriterReservation())`，读者另有限额。预算包含块、fragment、元数据等，不是进程 RSS 上限；[HTTP 发送缓冲](ram_cache.md#http-发送缓冲)另计。

PURGE 后私有块仍通过 `CacheVC.OpenReader` 读取，每个 writer 长度为 1 字节至 1 MiB；GET 仅持当前 slot，Range run 最多两个 slot。已完成 writer 的当前块字节保留到确切 reader/slot 释放，不保存整对象历史。慢读者离开该窗口后若缓存也不可读，可能需要上述一次同版本补取；有界内存不保证零额外回源。

## 失败边界

| 事件 | 行为 |
| --- | --- |
| 头发送前失败 | 返回 5xx 或安全旁路；所有权冲突不能旁路重复生产 |
| 响应未完成时失败 | 仍缺少所需字节的读者中断响应/H2 流，不追加错误正文、不混版或重试为新版 |
| 响应完成后的尾部失败 | 已完成响应无法撤回；失败块不发布，仍缺字节的其他读者接收错误 |
| PURGE | 每块发送前检查失效；仅已开始响应可继续原 writer/已校验快照，必要时按原源站和强 ETag 私有补块，不发布目录或完成位 |
| 版本变化 | 新元数据不打断仍可获得的旧字节；旧缺块无法取得时失败。元数据淘汰只表示版本未知 |
| HTTP 边界 | 客户端条件请求透传，非法区间返回 416；合成 200/206 使用 Cache-Control: no-cache，内部逐块判断 freshness |

PURGE 私有续流仍使用同一有界协调器，每次 run 最多两块；已开始的旧响应可共享同代次、相容身份的重叠区间，新请求不能加入。存储错误、关闭和其他强制失效仍失败，未发头请求可明确失败。提交不等于逐块 fsync，见 [存储与恢复](cache_storage_format.md)。

## 配置与回归

块布局固定为 1 MiB/chunk、64 chunk/segment；兼容配置项 `proxy.config.cache.range_block_bytes` 只接受 1048576。响应完成与当前块收尾的约束见 [ADR 澄清](adr/0001-partial-object-caching-plan-a.md#2026-11-03响应完成与当前块填充分开)。

代码：[serveBlocks](../server/range_cache.go)、[区间规划](../server/range_plan.go)。回归：[块内 RWW 与失败](../server/partial_chunk_test.go)、[GET/Range 共享](../server/partial_get_test.go)、[跨块/segment/取消](../server/partial_range_test.go)、[PURGE 私有共享](../server/partial_purge_test.go)、[淘汰](../server/rww_eviction_test.go)、[版本围栏](../server/range_version_fence_test.go)。
