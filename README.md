# Razor

Razor 是用 Go 编写的 HTTP 缓存代理，用于静态资源缓存和文件下载。它部署在客户端与源站之间，支持完整下载和 Range 分段读取。

- **部分对象缓存**：符合缓存条件的 GET 与单区间 Range 请求可复用已缓存的内容。
- **合并回源**：可共享的并发请求共用回源，数据到达后即可流式返回。
- **内存与磁盘缓存**：支持缓存新鲜度检查、重验证和 PURGE。
- **HTTP 服务**：支持 TLS、HTTP/2、配置重载和优雅退出。

## 快速开始

需要 Go 1.26.10。

```bash
git clone https://github.com/Monsterwi/razor.git
cd razor
make build
```

启动前编辑这两个配置文件：

- [conf/records.config](conf/records.config)：监听端口、源站地址和日志目录。
- [conf/storage.config](conf/storage.config)：缓存文件路径和容量。

本地试用可将监听端口设为 `8080`，源站设为 `http://127.0.0.1:8081`。确保源站已启动，日志和缓存目录对运行用户可写，然后启动代理：

```bash
./razor -c ./conf
```

在另一个终端中访问源站上的文件，将 `/file.bin` 换成实际路径：

```bash
curl -i http://127.0.0.1:8080/file.bin
curl -i -H 'Range: bytes=0-1023' http://127.0.0.1:8080/file.bin
```

缓存容量、共享回源和其他选项见[配置说明](docs/configuration.md)。

## 开发

```bash
make test
make test-race
make test-integration
go vet ./...
```

`test-race` 需要 C 编译器；`test-integration` 会启动真实代理进程验证缓存请求。问题和功能建议请提交到 [GitHub Issues](https://github.com/Monsterwi/razor/issues)。

## 文档

- [部分对象缓存设计](docs/partial_object_cache.md)：GET/Range 复用、对齐回源与边读边写。
- [系统架构](docs/architecture.md) · [缓存存储设计](docs/cache_design.md)
- [实现状态](docs/implementation_status.md)：已实现能力与限制。
- [部分对象缓存验收与测量](docs/benchmarks/partial_object_cache_2026-11-02.md) · [历史压测](docs/benchmarks/ats_comparison_2026-10-24.md)

## 许可证

[Apache License 2.0](LICENSE)
