# SSF — Sandbox Storage Framework（NFS-only）

当前实现与验证见 [26 独立公共协调服务](../design/26-independent-coordinator-implementation.md)，独立部署清单见 [deploy/sandboxsched-storage.yaml](deploy/sandboxsched-storage.yaml)。

本仓当前只保留 **NFS 直挂** 存储链路：Kubernetes 创建 PVC 后，SandboxSched 的沙箱创建接口通过 `volumeMounts[].pvc` 引用，控制面插件将每个 claim 解析为 NFS export（in-tree nfs 或 nfs.csi.k8s.io），节点服务按 IP 直挂并以内核态（mountinfo）验证沙箱挂载。任何非 NFS 卷在 bind 阶段即被拒绝。

历史上块卷/OverlayBD 引擎链（consume/preparation/node/引擎协议常量、OverlayBD CSI Driver、ssf-node/ssf-guard/ssf-csi-gateway 等）已从本分支移除；历史设计与验证证据保留在 [design/](design/) 与 [validation/](validation/)。

## 构建与检查

```sh
go mod download
make build
make test
make race
make vet
make linux
```

Go 1.26；生产节点运行 Linux。`go.sum` 固定依赖校验值。

## 组件

| 入口 | 形态与职责 |
| --- | --- |
| `ssf-coordinator` | 独立控制面服务；承载 `sandboxsched` 运行时插件，mTLS JSON 控制协议 |
| `sandboxschedadapter` | DaemonSet 节点服务；NFS 直挂、bbolt 日志、内核级沙箱挂载验证与恢复 |
| `ssfctl` | `migrate`、`keygen` |

控制面服务使用 `ssf.control.v1` 私有 mTLS JSON 协议，轻量客户端位于 `sdk/go/control`。

## 接入流程

1. `ssfctl -action migrate` 初始化共享 PostgreSQL 账本；协调服务另会幂等迁移 `sandboxsched_runs` 表。
2. 沙箱创建携带 `volumeMounts[].pvc`；节点服务调用 `LocalLaunch`，控制面解析每个 claim 为 NFS export 并持久化 dispatch。
3. 节点服务按解析后的 IP 直挂 NFS export（`<root>/mounts/<bindingID>`），代理将宿主路径 bind-mount 进沙箱。
4. Runtime 确认 guest 就绪后调用 Ready；节点服务复核 `/proc/<pid>/mountinfo` 的内核证据。
5. 停止 Runtime 后调用 Release；节点服务卸载 export、账本行置 Released。释放沙箱不删除 PVC。
6. 同名沙箱重建：Released 行被退役，重新规划全新 launch（per-incarnation `RunUID` 隔离）。

## 部署

1. 准备 PostgreSQL（`SSF_DATABASE_URL`）与 NFS provisioner。
2. 建立 TLS Secret（node / coordinator 身份）、api-key 与账本连接 Secret；生产证书由部署方管理。
3. 按 [deploy/sandboxsched-storage.yaml](deploy/sandboxsched-storage.yaml) 部署；节点打标 `ssf.dev/storage-test=true`。
4. `principal.namespaces` 列出允许消费的租户 namespace；租户不得直接修改 PV 或使用管理 ServiceAccount。

## 语义边界

- 只接受 NFS 卷：claim 未绑定或非 NFS 在 bind 即拒绝（400/409 语义与引擎时代一致）。
- 删除走约 10s SIGKILL 窗口，立即重建同名沙箱可能被拒绝（账本仍在 Planned/Ready）。
- 同一 claim 的 dispatch 由 digest 幂等保护；digest 不匹配、kind 不匹配（含遗留引擎行）返回冲突。
- 节点服务以 privileged 运行（挂载与 `/proc` 可见性需要）；卸载失败走重试 + lazy detach。
- 遗留引擎时代的 journal 记录可解析但不可再平衡，显式返回冲突。
