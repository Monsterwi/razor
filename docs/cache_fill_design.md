# 合并回源与范围规划

[Partial Object Cache](partial_object_cache.md) 说明对象模型和 GET/Range 全流程；本文记录共享身份、任务协调和范围规划。[RWW](rww_implementation.md) 负责 writer 生命周期，[存储](cache_storage_format.md) 负责提交与恢复。

## 整对象合并时序

等价并发 MISS 通过 [fillGroup](../server/fill_group.go) 原子加入一个独立生产者，无人为聚合等待。生产者启动后再查缓存，避免竞争期间重复填充；缓存层 OpenDir 另管写入所有权。共享头部通过准入后，订阅者绑定确切 writer，各自读取在途字节；writer 终态释放活跃 key，已有 reader 仍持有原 writer。

| 规则 | 行为 |
| --- | --- |
| 期限/额度 | 默认 300 秒，自创建起不续期；最多 16 个任务、每任务 256 个订阅者（含创建者）；过载受控拒绝 |
| 取消 | 首客户端不拥有共享 Context；最后订阅者离开时，分块任务只完成已开始的当前 chunk，其余任务立即取消 |
| 私有响应 | 仅交给仍等待的创建者；其他订阅者失败，不立即各自重试回源 |

## 共享身份

| 身份 | 组成 |
| --- | --- |
| URL 根键 v4 | 配置命名空间、有效 scheme/Host、原始转义路径、原始 query（保留顺序） |
| 整对象表示键 | 根键、受支持的 Vary 字段和值 |
| 在途任务键 | 填充键、方法、Host、peer IP、请求头、TLS 状态、runtime version |
| 块键 | 根键、完整源站、变体、强 ETag、随机缓存代次 Generation、布局/块大小、总长、块号 |
| FillToken | 回源前失效凭证；块从父凭证派生，不重新捕获代次 |

默认模式保留 IP/请求头差异。显式 `shared_fill_public_static=1` 后，任务键与实际回源使用同一规范化：

| 头或身份 | 公共模式 |
| --- | --- |
| IP / 转发身份 | 移除 Forwarded、X-Forwarded-For、X-Real-IP；不重新注入首客户端 IP |
| UA / 追踪 | UA 固定 Razor；移除 Traceparent、Tracestate、Baggage、X-Request-Id、X-Correlation-Id |
| scheme / Host | 保留有效值；本地关联日志只保留允许字段的哈希 |
| 变体 / 未知头 | Accept-Encoding、Accept-Language 保留原值；未知头仍参与隔离 |
| Authorization / Cookie / 请求正文 | 不进入公共共享路径；响应 private/no-store/Set-Cookie 等不共享 |

源站须保证公共资源不随客户端身份变化，并正确返回 Vary。仅支持 Accept-Encoding/Accept-Language 及其组合；不同源站、版本、变体仍隔离。Accept-Encoding 缺省与显式 identity 也区分。见 [规范化代码](../proxy/public_request.go)。

Query 保留原始顺序、数量、空项、值和编码，不排序或解码重编；重复参数换序、`flag`/`flag=`、`+`/`%20`、编码大小写、有/无空 query 均分别缓存。缓存、任务、PURGE 共用根键，回源保留原 query；v4 隔离旧键，旧文件安全 MISS。精确 PURGE 匹配原始 query，无 query 的路径前缀 PURGE 覆盖全部 query 变体。

## 缺块合并与 RWW 时序

请求按对象绝对偏移裁剪。块大小 B=1 MiB，区间 `[a,b)` 对应块号 `floor(a/B)` 至 `floor((b-1)/B)`；对齐回源在对象尾截断，转换成 HTTP 闭区间 Range。

| 阶段 | 协调规则 |
| --- | --- |
| 发现 | Range 元数据缺失/过期时共享 HEAD；完全冷 GET 用一次普通 GET、无 HEAD。GET/Range 共用发现任务，首个区间取得需求后才释放发现订阅 |
| 规划 | 按需读取 segment 位图；每窗口最多合并两个连续缺块，命中块、已有任务和 segment 边界切断合并；长请求逐窗口推进 |
| join | 同一对象代次按绝对块区间原子认领；相容身份加入重叠区间。未知头等造成身份不相容时可失败，不能绕过所有权重复回源 |
| 回源/校验 | 固定源站、强 ETag，发送 Range、If-Match、identity；校验精确 206/Content-Range、长度、编码、实际 EOF 及无 Trailer |
| 读取/提交 | 合法头部后读取在途字节；完整块独立提交并退出在途排他范围，不等待后块。当前块收尾见 [RWW](rww_implementation.md) |

每个发现或合并回源任务单独计时，一次回源复用一个 writer 额度、逐块写入。元数据 revision 仅用于条件退役；迟到失败须匹配原 Generation、revision 和目录快照，不能清除新记录或中断新版任务。覆盖修复不能误清并发新填充；块丢失时的有界补取见 RWW，不能把源站失败或换版当作淘汰补取。

## 失效与离线

PURGE 配置 Bearer token 后启用；无 query 且以 `/` 结尾为前缀，否则精确匹配，同时失效普通表示与块家族。旧 token 不得发布元数据、目录或完成位，新请求不加入旧任务；已发头响应按原版本有界私有续流，详见 [RWW 失败边界](rww_implementation.md#失败边界)。

每个 stripe 保留 64 条 PURGE 事件；历史窗口外的旧父凭证保守失去发布权，已知非目标活跃写者推进事件位置。该限制可能增加长请求的私有补取，不改变内容版本。PURGE 修改任一 stripe 前检查关闭/存储错误，失败不得返回成功；磁盘可延后回收，检查点不包含已撤销目录。

离线读取要求同一仍配置源站的元数据及整个所需区间 fresh、完整可读；先查健康候选，再按稳定配置顺序查找。不提供 stale-if-error，缺块且无可用源站不能保证成功。版本、缓存代次和恢复规则见 [Partial Object Cache](partial_object_cache.md)。

配置与统计口径见 [configuration.md](configuration.md)。代码：[任务](../server/fill_group.go)、[整对象](../server/shared_fill.go)、[块](../server/range_cache.go)、[规划](../server/range_plan.go)；回归：[合并](../server/coalescing_test.go)、[公共身份](../server/public_static_test.go)、[范围](../server/partial_range_test.go)、[离线](../server/range_offline_test.go)。
