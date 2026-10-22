# SSF 实现验证报告

验证日期：2026-10-19。环境：macOS amd64，Go 1.26.3；测试主机 CPU 为 Intel Core i7-7700HQ。代码位于 [SSF](../sandbox-storage/README.md)。

## 已执行

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 17 个测试通过，0 失败，3 个环境测试跳过 |
| `go test -race ./...` | 17 个测试通过，0 失败，同样 3 项跳过 |
| `go vet ./...` | 通过 |
| `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/...` | 所有命令入口交叉构建通过 |
| Linux `go test -c ./node` | 包含真实 Linux 挂载测试代码的测试二进制编译通过；未执行该二进制 |
| protobuf 重新生成一致性 | 通过；生成前后 `.pb.go` 哈希一致 |
| Kubernetes 清单类型检查 | base 10 个对象、webhook 1 个、消费 RBAC 1 个对象通过严格字段反序列化；CA 模板使用测试值替换后检查 |
| Python / shell | fixture JSON 生成、Python 编译及所有 shell 脚本 `bash -n` 通过 |
| 完整部署图 | Mermaid 解析、179 个部署元素、文本布局检查通过；已检查节点部分渲染图 |
| 参考原文 / 参考代码 | CSI 分析文档哈希不变；AgentENV、CubeSandbox、Substrate 三个仓库工作区保持干净 |

测试覆盖身份/签名/过期、CSI Mount/Block oneof 持久化、UID 重建、静态预留、WFFC 节点不漂移、未授权删除与 Pod 抢用、选点许可、未知 Publish 重试、共享 Stage 的末次释放、取消重放、节点重启保留 pin、WAL 重开、活动索引与 tombstone，以及 Gateway 拒绝管理 RPC。

## 性能样本

最终 `BenchmarkPreparedReplay-8`：**151,079 ns/op（约 0.151 ms）**，10,360 B/op、166 allocs/op，共 6,914 次迭代。

该样本使用 mock CSI 和测试资源检查器，只测同一已 Prepared 请求的本地重试。它不包含真实 Kubernetes、PostgreSQL、CSI Driver、挂载系统调用、VMM 或 guest，不是 p50/p99，也不是沙箱创建耗时。冷路径的同步 WAL 成本和真实存储开销仍需 Linux 环境测量。

## 未实跑

| 项目 | 原因 / 入口 |
| --- | --- |
| PostgreSQL 并发 writer、消费与共享 attachment 生命周期 | 未提供数据库；`scripts/integration.sh` 创建独立 PostgreSQL 后运行 |
| Kubernetes 四种动态供卷组合 | 本机无 Docker/kind/kubectl；`scripts/kind-fixture.sh` 与 `TestClusterNativeProvisioning` |
| 真实文件/块设备 Stage/Publish | 本机非 Linux；`scripts/linux-mount-tests.sh` 或特权测试容器 |
| Guard 的 API-server webhook、实际证书/RBAC、原生快照/扩容/回收故障矩阵 | 需要专用测试集群；单元测试不能替代 |
| Rust SDK 编译 | 本机无 Rust 工具链；执行 `cargo check --manifest-path sdk/rust/Cargo.toml` |
| 真实 VM、guest 就绪、应用一致性 checkpoint | 本次明确不开发项目/VMM 适配器；需后续 Runtime 集成 |

Go 测试跳过的具体用例：`TestPostgresConsumptionLifecycle`、`TestPostgresConcurrentWriterAndDrain`、`TestClusterNativeProvisioning`。Linux 真实挂载测试通过 build tag 在本机不运行，不计入上述三个 SKIP。

## 复现与证据

普通环境执行 `SSF/scripts/test.sh`。本次因网络代理不稳定，运行时临时指定了 `/private/tmp` 下的模块/构建缓存与本地文件代理；代码和 go.mod 不依赖这些绝对路径，没有本地 replace。

原始记录：[单测 JSON](../sandbox-storage/test-results/unit.json)、[race JSON](../sandbox-storage/test-results/race.json)、[基准输出](../sandbox-storage/test-results/benchmark.txt)。测试脚本会重新生成这些文件；它们不属于源代码。

用户指定分析文档 SHA256：`9733935a2c0e2d144b67c8404361c835e7f8ed45d2b508992cb230da31b3d5dc`。
