# SSF — Sandbox Storage Framework

当前实现与验证见 [26 独立公共协调服务](../design/26-independent-coordinator-implementation.md)，部署使用框架仓库的联合构建入口和 overlay。

Kubernetes 创建 PVC 后，原 AgentENV 创建接口通过 `volumeMounts[].pvc` 引用。自动准备的此前实施记录见 [22](../design/22-volume-preparation-implementation.md)，本轮独立服务的验证结果以 [26](../design/26-independent-coordinator-implementation.md) 和 `validation/control-service` 为准。

SSF 使用 Kubernetes 管理 PVC/PV、供卷、快照、克隆、扩容与回收。沙箱消费由公共 Go 协调库直接执行 Attach；当前 AgentENV 由独立 `ssf-coordinator` 承载，并通过签名计划驱动公共 Node Agent 的 Stage/Publish。没有逐沙箱 Pod，当前 AgentENV 具有独立协调服务 Deployment，保留原 Scheduler 的计算选点。

公共核心提供框架与验证工程；`runtime/verification` 明确拒绝宣称真实 guest 已就绪。[AgentENV 原生插件](adapter/agentenvadapter/README.md) 保留原 Gateway、Scheduler、Orchestrator 和沙箱 API，实现存储接入、完整生命周期与交付工具；旧独立沙箱控制面已移除。当前架构见 [23](../design/23-current-implementation-architecture.md)，完整回归及真实环境未执行项见 [14](../design/14-agentenvadapter-real-environment-test-plan.md)。SSF 核心不依赖项目适配器。

## 构建与检查

```sh
go mod download
make generate       # 固定 Go parser + 官方生成插件，不要求系统 protoc
make build
make test
make race
make vet
make linux
make check-generated
```

Go 1.26；生产节点运行 Linux。`go.sum` 固定依赖校验值。Node Rust 客户端使用 `sdk/rust/Cargo.toml`；原生运行时 adapter crate 使用 `cargo test --locked --manifest-path adapter/agentenvadapter/runtime/Cargo.toml`。主 AgentENV 的 Linux 类型检查与本机可移植单测分开记录，当前结果见 `validation/control-service`。

## 组件

| 入口 | 形态与职责 |
| --- | --- |
| `ssf-coordinator` / `consume.Coordinator` | 独立服务承载公共库；授权、绑定、事务预留、Attach、签名计划、释放、恢复 |
| `ssf-node` | Linux 常驻进程 / DaemonSet；本地 CSI、bbolt 日志、实际资源检查 |
| `ssf-guard` | Kubernetes validating webhook；受管资源删除与权威保护 |
| `ssf-csi-gateway` | 可选厂商 Controller 同 Pod 代理；只开放消费 RPC |
| `ssfctl` | `migrate`、`keygen`、`inspect` |
| `ssf-test-driver` | 仅限一次性 Linux 测试环境的文件/loop 块设备 CSI fixture |

`api/ssf/v1/node.proto` 是实际线协议；`sdk/go` 与 `sdk/rust` 是节点客户端。控制面服务使用 `ssf.control.v1` 私有 mTLS JSON 协议，轻量客户端位于 `sdk/go/control`；不开放替代沙箱 API。

## 接入流程

1. 提供 `consume.Authorizer`、`runtime.PlacementProvider`、Driver Registry、共享 PostgreSQL 和签名密钥。Sandbox 必须使用不可复用的实例 UID。Placement 的 `Reserve` 必须按 operation 幂等，并能恢复未知结果；过期预留由项目恢复同一次预留，框架不擅自换节点。
2. 调用 `Coordinator.Prepare`，得到 operation、placement 和完整签名计划。任何错误都保留 operation，按同一 request ID 恢复。
3. 原创建 RPC 携带计划，节点服务调用 `PrepareVolumes`；每个卷得到 FileTree 或 BlockDevice。
4. Runtime 检查能力、接入资源并确认 guest 就绪，通过可信证据服务供 Node Agent 验证，然后调用 Ready 和 Commit。
5. 停止 Runtime 使用后调用 Release；最后一个 holder 才 Unstage/Detach。释放沙箱不删除 PVC。
6. 删除卷先 `Drain`；已有使用需要逐一停止并释放，`Reclaim` 通过原生回收流程执行。未知结果保留 pin，不提供按超时强制解锁的捷径。

可编译示例见 [examples/integration/create.go](examples/integration/create.go)。示例返回失败 operation，调用方把它存入项目生命周期日志，并调用 `Cleanup` 或 `Resume`。`RunRecovery` 重入同一项目处理器，不会伪造计算预留或 Runtime 停止证据。

## 部署顺序

1. 准备 PostgreSQL 和角色权限，执行 `SSF_DATABASE_URL=... go run ./cmd/ssfctl -action migrate`。Guard 使用受限读取角色；消费账号需要读写消费表。协调服务另会幂等迁移 adapter 专用表。账本不可被每个项目独立复制。
2. 生成签名密钥并分发公钥；配置 mTLS 证书及 URI SAN allowlist。生产证书由部署方管理。Guard 证书 SAN 必须包含 `ssf-guard.ssf-system.svc`。
3. 按 `deploy/*.example.json` 建立 ConfigMap 和 TLS Secret，部署 Guard，检查 `/readyz`，替换 `deploy/webhook.yaml` 的 `CA_BUNDLE` 再注册 webhook。
4. 部署 Node Agent。CSI Node Driver 必须看到同一绝对路径 `/var/lib/ssf`，具备正确 mount propagation；仅挂载 socket 不足以完成集成。Node、Driver、Runtime 的路径与可见域必须一致。
5. 需要网络访问 Controller UDS 时，将 Gateway 加到厂商 Controller Pod，共享 `/csi` socket，使用 `deploy/gateway.example.json` 和共享账本只读账号（通过 `SSF_DATABASE_URL`）；它验证 Binding 元数据与 attachment 状态，不替换原生 provisioner/resizer/snapshotter。
6. 原生 StorageClass 使用 Retain；PVC 标记 `ssf.dev/managed=true`；静态候选 CSI PV 同样标记。Resolver 在验证真实 claim UID 后为动态 PV补充标记。Guard 在这之前也检查 PV 引用的受管 PVC。
7. AgentENV 按 [13 部署文档](../design/13-agentenvadapter-installation-and-deployment.md) 安装独立协调服务和原工作负载增量，将消费 RBAC 绑定 `ssf-coordinator`。原 Scheduler 只保留原生发现权限。示例 RBAC 是功能集合，应按已认证租户 namespace、Secret 范围拆分生产 Role；租户不得直接修改 PV、受管标签、选点注解或使用管理 ServiceAccount。

`deploy/base.yaml` 包含部署资源，不包含真实密钥或数据库口令；`ssf-ledger` Secret 的 `url` 提供连接串。Node 以 root/privileged 运行是挂载可见性需要，Guard 无此要求。

## 验证环境与边界

```sh
bash scripts/integration.sh       # PostgreSQL race + 特权 Linux 挂载测试
bash scripts/kind-fixture.sh      # 独立 kind 集群和原生 CSI provisioner/resizer
SSF_TEST_KUBECONFIG=/path/to/config go test -v ./kubernetes -run TestCluster
```

脚本固定基础镜像和 sidecar 版本，测试 cluster/driver 仅用于一次性环境。快照 CRD/controller 的安装见 `testing/README.md`。真实 VM 测试属于后续 Runtime Adapter 验收。

当前实现保守限制同一物理卷跨沙箱写入为独占，即使底层支持 RWX 也不自动放宽；多个只读消费者可以共享兼容 attachment/stage。同一卷多 PVC 别名导入会被拒绝，避免绕开物理卷唯一使用权威。当前没有自动强制 fencing、在线凭证轮换、跨权威自动迁移或热资源池；这类能力不能通过删除账本记录模拟。

消费请求支持 PVCRef 或已登记 `VolumeUID`，两者互斥。删除从未消费的 Pending PVC 使用 `Coordinator.DeletePVC`；其 `ClaimDeleted` 只表示观察到原生 claim 已删除，不伪装成后端 DeleteVolume 的完成证明。`Ledger.Uses` 可列出排空尚未完成的消费者，由项目生命周期负责停止它们。

本轮实现、测试状态和侵入量见 [26](../design/26-independent-coordinator-implementation.md)。[09](../design/09-implementation-log.md)、[10](../design/10-implementation-validation.md)、[11](../design/11-implementation-coverage.md) 保留早期公共框架实施记录，不能替代本轮验收。
