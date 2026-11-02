# 合并回源与原生部分对象缓存

[架构](architecture.md) · [RWW 管线](rww_implementation.md) · [存储提交](cache_storage_format.md)。

## 整对象合并时序

等价并发 MISS 共享一个独立生产者。头部通过共享准入后，各订阅者绑定确切 writer，独立读取在途字节。

共享 writer 在终态释放活跃 key 所有权；已经订阅的 reader 独立持有原 writer。

| 规则 | 行为 |
| --- | --- |
| 聚合窗口 | 无人为延迟；首请求立即启动，后来的加入在途任务 |
| 等待边界 | 头部验证及所需字节到达后即可读取，不以整对象完成作为首字节屏障 |
| 期限 | 默认 300 秒，从任务创建计时；加入不续期，其他超时可能更早失败 |
| 取消 | 独立取消；最后订阅者离开才取消生产者；首客户端不拥有共享 Context |
| 额度 | 默认 16 个任务、每任务 256 个订阅者（含创建者）；过载受控拒绝 |
| 私有响应 | 仅交给仍等待的创建者；其他订阅者失败，不立即各自重试回源 |

[fillGroup](../server/fill_group.go) 原子 join；生产者启动后再查缓存，避免竞争期间重复填充。它协调尚未有 Writer 的请求；缓存层 OpenDir 另管写入所有权。

## 共享身份

| 身份 | 组成 |
| --- | --- |
| URL 根键 v4 | 配置命名空间、有效 scheme/Host、原始转义路径、原始 query（保留顺序） |
| 整对象表示键 | 根键、受支持的 Vary 字段和值 |
| 在途任务键 | 填充键、方法、Host、peer IP、请求头、TLS 状态、runtime version |
| 块键 | 根键、完整源站、变体、强 ETag、随机缓存代次 Generation、布局/块大小、块号 |
| FillToken | 回源前失效凭证；块从父凭证派生，不重新捕获代次 |

默认模式保留 IP/请求头差异。显式 `shared_fill_public_static=1` 后，任务键与实际回源使用同一规范化：

| 头或身份 | 公共模式 |
| --- | --- |
| IP / 转发身份 | 移除 Forwarded、X-Forwarded-For、X-Real-IP；不重新注入首客户端 IP |
| UA / 追踪 | UA 固定 Razor；移除 Traceparent、Tracestate、Baggage、X-Request-Id、X-Correlation-Id |
| scheme / Host | 保留有效值；本地关联日志只保留允许字段的哈希 |
| 变体 / 未知头 | Accept-Encoding、Accept-Language 保留原值；未知头仍参与隔离 |
| Authorization / Cookie / 请求正文 | 不进入公共共享路径；响应 private/no-store/Set-Cookie 等不共享 |

源站须保证公共资源不随客户端身份变化，并正确返回 Vary。仅支持 Accept-Encoding/Accept-Language 及其组合；不同源站、版本、变体仍隔离。见 [规范化代码](../proxy/public_request.go)。

Query 身份保留原始顺序、数量、空项、值和编码，不排序、不解码/重编码。`a=1&b=2` 与换序项分别缓存，重复参数顺序也区分；`flag`/`flag=`、`+`/`%20`、编码大小写、有/无空 query 均保持原样。缓存、任务与 PURGE 共用根键，回源保留原 query；v4 隔离旧键，旧文件安全 MISS。精确 PURGE 匹配原始 query，无 query 的路径前缀 PURGE 覆盖其全部 query 变体。

## 缺块合并与 RWW 时序

假设 fresh 对象元数据 已存在，块 1 MiB；A 读 `[0,2 MiB)`，B 读 `[0.5,1.5 MiB)`，其他身份等价。区间左闭右开；各自发送速度独立。

块内流式和完成位发布顺序见 [RWW 管线](rww_implementation.md)。

| 步骤 | 契约 |
| --- | --- |
| 对象元数据 | Range 缺失/过期时共享 HEAD；完全冷 GET 从单次普通 GET 响应发现；固定总长、强 ETag、完整源站、变体、Generation 和格式 v3 |
| 区间映射 | 首尾对齐固定块，尾块按总长截断；只补所需缺块，不后台补全对象 |
| join | 同对象代次按绝对块区间原子认领；等价请求加入重叠在途区间，未知头等不兼容身份不能绕过所有权另发请求 |
| 回源 | 根据 segment 位图，每个窗口最多合并两个连续缺块；命中块、已有在途任务及 segment 边界切断合并。固定源站，发送对齐 Range、If-Match、identity |
| 校验 | 精确 206/Content-Range、强 ETag、长度、编码、实际 EOF、无 Trailer；错误不发布 |
| 发布/读取 | 合法头部后 RWW；完整块数据与目录提交后更新 segment 完成位，客户端只收自身切片 |
| 期限/资源 | 每个对象元数据/合并回源任务分别计时；一次回源复用一个 writer 额度，逐块写入，客户端只保留当前窗口和 reader |

对象元数据的存储记录 revision 只用于条件退役，不参与内容身份；迟到旧失败须同时匹配原 Generation、原 revision 和目录快照，不能清除新的元数据或中断新版回源。对象元数据存在不代表块存在。每64个chunk对应一份按需读取的segment完成位图；实际读取仍检查目录和内容。块可独立淘汰；对象元数据同 ETag/总长刷新可沿用 Generation，丢失重建则换标识。新格式不会把旧记录解释为新的元数据。未知头造成不同任务身份时，写入所有权竞争可失败，不能绕过所有权重复回源。

一个合并响应按绝对块编号独立提交和置位。前块完成通知不等待后块；后块截断或无人需要后取消，只丢弃未完成块，已提交前块仍可命中。已完成块退出在途排他范围；慢读者遇到本地片段被滚盘淘汰时，最多补取一次同代次块并跳过已经发送的偏移，期间仍保留后块需求。回源失败或版本变化不触发此补取。长范围逐窗口推进，不一次建立全对象任务列表，也不逐块读取正文来发现覆盖。规划和区间所有权见 [range_plan.go](../server/range_plan.go)，真实 HTTP 回归见 [partial_range_test.go](../server/partial_range_test.go)。

符合准入条件的普通 GET（含小对象）与 Range 共用块、完成位和在途任务，不再保存重复的完整对象副本。无完成块或可复用任务的冷 GET 只发一次普通 GET，不发 HEAD；从同一响应确认强 ETag、已知长度、identity 和共享策略，再逐块提供 RWW。已有部分内容时，GET 只按上述规则补缺块。GET/Range 在源站响应头到来前共用发现任务，并在首个区间取得需求后释放发现任务，避免交接时误取消。长 GET 只保留当前 writer；过去完整块按需读缓存，读者取消互不影响，最后需求离开即停止回源。无强 ETag、长度未知或不可共享响应使用收到的同一响应继续现有整对象缓存或透传。Accept-Encoding 缺省与显式 identity 仍区分。

## 失效与离线

| 情况 | 行为 |
| --- | --- |
| PURGE | 同 URL 普通表示及稀疏 Range 家族一起失效；旧 token 不得发布元数据、目录或完成位；新请求不加入旧任务 |
| PURGE 范围 | 配置 Bearer token 后启用；无 query 且以 `/` 结尾为前缀，否则精确匹配；正常范围内保留非目标对象及其在途写者 |
| 版本/换源 | 响应固定所选源站/版本；新版元数据不打断仍可获得的旧字节；旧缺块无法取得则失败，本次请求不换版重试 |
| 离线读取 | 同一仍配置源站的对象元数据 和所需整区间均 fresh、完整可读时零回源；优先健康候选，再按稳定配置顺序查找 |
| 缺块/损坏/淘汰 | 可受控重取；无可用源站不能保证成功；不提供 stale-if-error |
| 重启 | 仅恢复完整验证的干净检查点；异常退出可冷 MISS，不承诺每块 fsync |

PURGE 撤销缓存发布权，与当前响应已选择的字节版本分开。已发头的 GET/Range 可继续消费确切 writer 或已校验快照；后续缺块仍可按原源站、原强 ETag 发必要的私有 Range 回源，每个 run 仍最多两块。私有任务沿用同一有界任务协调器，不允许新请求加入，块 writer 限制为1..1MiB，不发布目录或完成位。源站拒绝旧 ETag、存储失败、取消或关闭仍终止当前响应，不换版重试。未发头的请求可明确失败。

缓存层用独立 PURGE 事件围栏匹配精确/前缀目标，不再每次递增全 stripe 硬代次。每个 stripe 只保留64条事件；超出历史窗口的旧父凭证会保守失去发布权，已知非目标活跃写者会推进其事件位置。该有界历史边界可能增加长期请求的私有补取，不影响内容版本正确性。PURGE 在修改任一 stripe 前检查关闭和存储错误，失败不得返回成功；物理磁盘回收可延后，检查点不包含已撤销的旧目录。

例：100 MiB 对象依次读 `[0,16 MiB)`、`[8,24 MiB)`，若同版本、fresh、未淘汰，累计正文回源 24 MiB，第二次仅 8 MiB；HEAD 另计。非对齐请求有边界块放大。

配置见 [configuration.md](configuration.md)，Stats 区分请求、块、源站 HEAD/GET/正文口径。多 Range 缓存和跨节点合并/失效不支持。代码：[任务](../server/fill_group.go)、[整对象](../server/shared_fill.go)、[块](../server/range_cache.go)；回归：[合并](../server/coalescing_test.go)、[公共身份](../server/public_static_test.go)、[RWW](../server/rww_test.go)、[离线](../server/range_offline_test.go)。
