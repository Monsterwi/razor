# 缓存配置

写入 records.config，统一前缀 `proxy.config.cache.`。下表变更须重启；无效重载保留当前服务。

| 键 | 默认 | 范围 / 含义 |
| --- | --- | --- |
| shared_fill_public_static | 0 | INT 0/1；开启前须确认内容不随客户端身份变化 |
| range_block_bytes | 1048576 | 固定 1 MiB；其他值拒绝加载 |
| ram_cache_algorithm | otter | otter / s3-fifo |
| ram_cache_bytes | 10485760 | 每 stripe 权重预算；0 禁用，非零至多 1 TiB |

| 其他配置 | 默认 / 含义 |
| --- | --- |
| shared_fill_max_tasks | 16 个活动任务 |
| shared_fill_max_waiters | 每任务 256 个订阅者，含创建者 |
| shared_fill_timeout | 每任务总期限 300 秒，加入不续期 |
| max_object_bytes | 67108864（64 MiB）；100/200 MiB 对象须调大 |

RAM 权重包含序列化 value 和解码元数据估算，不等于 RSS。身份、源站、版本与失效契约见 [缓存填充](cache_fill_design.md)，预算见 [RAM](ram_cache.md)。完整字段及校验见 [config](../config/config.go)、[coalescing](../config/coalescing.go)、[limits](../config/limits.go)；任务和验收使用 [GitHub Issues](https://github.com/Monsterwi/razor/issues)。
