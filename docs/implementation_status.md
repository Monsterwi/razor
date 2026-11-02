# 实现状态

本页描述当前源码；发布、合并与任务验收以 [GitHub Issues](https://github.com/Monsterwi/razor/issues) 和 Git 记录为准，#1 保持打开。设计入口：[UML 架构](architecture.md)。

## 能力

| 能力 | 当前行为 / 边界 |
| --- | --- |
| HTTP | 标准库代理、TLS/H2、可信转发、源站证书校验、健康探测、取消和排空 |
| 共享缓存语义 | 显式 freshness、Age/Date、s-maxage、请求约束、条件 GET/HEAD、304 更新；不提供启发式 freshness |
| Vary / 准入 | 仅 Accept-Encoding/Accept-Language 及组合，每 URL 最多 16 普通表示；私有/未知语义保守旁路 |
| 合并回源 | 回源前原子 join；独立生产者/取消/期限；合法头部后绑定确切 writer，独立读取在途字节 |
| 部分对象 | 内部对象元数据 + segment 完成位图 + 独立固定块，Range/合格普通 GET 共用（含小对象）；冷 GET 一次普通回源、无 HEAD；强 ETag、固定源站和布局，不混版 |
| RWW | 响应头校验后可发送块内前缀，完整校验提交后才置完成位；固定 1 MiB/chunk、64 chunk/segment |
| 离线 Range | 同一仍配置源站的 对象元数据和整个所需区间均 fresh、完整可读时零回源；不是 stale-if-error |
| RAM | Otter 默认、S3-FIFO 可选；序列化 value + 解码元数据估算计费，非 RSS 硬上限 |
| 磁盘 | CRC/身份/覆盖代次校验；头片段最后发布；只恢复验证通过的干净检查点 |
| Query | 保留原始参数顺序，缓存/任务/PURGE 同规则；重复参数换序分别缓存，实际回源不重排 |
| 重载 | 不可变 runtime 租约，原子替换，最多一个未排空旧 runtime；部分设置须重启 |

## 运行边界

| 类别 | 限制 |
| --- | --- |
| PURGE | 默认关闭；Bearer token 启用；精确/路径前缀失效普通表示与 Range 家族；旧 token 不复活；已开始响应可按原版本有界私有续流；非目标对象保留，事件历史窗口见失效文档 |
| Range | Range 通过 HEAD、冷 GET 通过单次普通响应发现总长/强 ETag/identity；多 Range、弱 ETag 等旁路；合成下游响应 no-cache；后块失败中断流 |
| 容量 | max_object_bytes 默认 64 MiB；大对象测试须调大；块按必要窗口，不要求磁盘容纳整文件 |
| 索引 | 每 stripe 至多 min(目录容量, 65,536) 映射、16 MiB 键/selector 字符串、每键/selector 16 KiB；URL 对象元数据快照上限 64 |
| 任务/资源 | 默认 16 生产任务、每任务 256 订阅者、300 秒；请求/源站/写者/读者/队列另限额，非总 RSS 预算 |
| 持久性 | 提交不等于逐块 fsync；异常退出可冷 MISS，不截断旧文件；同缓存 inode 不跨进程共享 |
| 未支持 | 多 Range 块缓存、跨节点合并/一致性/PURGE、HTTP/3、io_uring/native AIO |

完整边界：[填充/失效](cache_fill_design.md)、[RWW](rww_implementation.md)、[RAM](ram_cache.md)、[存储](cache_storage_format.md)。

## Plan A 验收

[2026-11-02 验收与成本报告](benchmarks/partial_object_cache_2026-11-02.md)关联父规格逐项测试、真实进程结果、固定版本与测量原值。单字节响应等待对应块完整校验；PURGE 后慢读旧流可能按原版本有界补取历史块，报告分别计量。

## 历史压测

以下保留 [2026-10-24 结果](benchmarks/ats_comparison_2026-10-24.md)，测量源码 `8581267`。

| 范围 | 结果 |
| --- | --- |
| 256 KiB 热 HIT，C32/C100 | Razor 吞吐为 ATS 10.2.1 native 的 74.5% / 75.1%；P99 仍更高 |
| 18 组、5,507,780 次请求 | 正文错误、回源、进程实际读盘均为 0 |
| 未覆盖 | 大文件、HDD、冷缓存、TLS/H2 压力及长测；不引用历史跑分补齐 |

功能与边界依据当前代码；压测仅证明上述范围，不等于全功能或长期生产验收。
