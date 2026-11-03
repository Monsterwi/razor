# Razor

Go HTTP 缓存代理，重点是**合并回源**和**原生部分对象缓存**。

| 能力 | 行为 |
| --- | --- |
| 合并回源 | 等价并发 MISS 共享生产者，独立取消、限额与超时 |
| 部分对象 / RWW | GET/单 Range 复用固定块；Plan A 位图规划，块内流式发送，当前块独立完成填充 |
| 存储 | CRC、检查点恢复、PURGE；RAM 默认 Otter，可选 S3-FIFO |
| HTTP | 缓存新鲜度/重验证、TLS/H2、配置重载和排空 |

## 运行

要求 Go **1.26.10**。

```bash
make build
./razor -c /path/to/conf
```

目录包含 [records.config](conf/records.config) 和 [storage.config](conf/storage.config)；先设置端口、源站、日志和缓存文件路径。缓存身份保留原始 Query 顺序，包括重复参数；换序请求分别缓存。

符合准入条件的普通 GET 与单 Range 共用分块缓存。若静态资源可跨客户端身份共享回源，可启用公共模式：

```text
CONFIG proxy.config.cache.shared_fill_public_static INT 1
CONFIG proxy.config.cache.max_object_bytes INT 268435456
CONFIG proxy.config.cache.range_block_bytes INT 1048576
```

公共模式要求资源不随客户端身份变化；公共模式和 RAM 策略修改须重启，块大小固定为 1 MiB。[配置与约束](docs/configuration.md)。

## 文档与验证

[UML 架构](docs/architecture.md) · [缓存设计](docs/cache_design.md) · [合并回源时序](docs/cache_fill_design.md) · [RWW](docs/rww_implementation.md) · [实现状态](docs/implementation_status.md)

```bash
make test
make test-race
go vet ./...
```

[Plan A 验收与成本](docs/benchmarks/partial_object_cache_2026-11-02.md)：GET/Range 双向复用、块内 RWW、版本/PURGE/恢复，以及源站流量和元数据成本的固定样例。

[2026-10-24 历史压测](docs/benchmarks/ats_comparison_2026-10-24.md)：源码 `8581267`，256 KiB 热命中，C32/C100；三轮原值和配置随报告附带。该次测量未覆盖大文件、HDD 或长测，同机结果不代表公网生产容量。

块路径要求强 ETag、总长和 identity。头部校验后可读取在途字节；完整校验提交后才更新覆盖位图。不支持多 Range 块缓存、跨节点一致性或 io_uring；提交不等于逐块 fsync。
