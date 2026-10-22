# OverlayBD 原生节点与 Controller 部署材料

这些材料在本机完成离线渲染；未在 Kubernetes/Linux/ublk 执行。完整设计 27 尚有设备创建/未知 restack 和维护设备恢复等实现缺项，当前不能作为完整端到端验收结果。

故障测试入口 `scripts/native_fault_e2e.py` 使用 `--driver-kind native-overlaybd`
记录原生 Driver 范围。新增阶段包括 `overlaybd-create`、`overlaybd-device-create`、
`overlaybd-restack`、`overlaybd-upload`、`overlaybd-head-commit`、
`overlaybd-daemon-restart`、`overlaybd-expansion-tool`、`overlaybd-expansion-seal`。
每个场景仍需提供真实环境的注入、工作负载、恢复及断言命令；不传 `--execute` 只生成
“未执行”报告。此模式拒绝 fixture 的 `csi_fault`，不能把测试 Driver 的结果记为原生
OverlayBD 故障恢复结果。阶段名称本身也不证明进程已在指定故障点中断。

## 原工作负载中的进程

沿用 `render_agentenv_native.py` 引用的 AgentENV base。原 Gateway、Scheduler、节点 DaemonSet、心跳、headless Service、P2P、代理、探针和 native preStop 都保留。新增的存储配置清单描述后端节点，不替代 Scheduler 的计算发现或选点。

节点按以下顺序启动：

1. `overlaybd-config`：普通 init container，根据 Downward API 的真实 Node 名选择管理员清单；生成私有配置后退出。清单未包括的节点拒绝启动，不默认绑定其他节点仓库。
2. `overlaybd-daemon`：CSI 专属 ublk daemon，独立 UDS、状态目录与 metrics 9106。
3. `overlaybd-native`：复用 AgentENV volume-core 的 Rust 存储宿主。
4. `overlaybd-worker`：mTLS 后端管理入口，hostPort 9455；只接受 `spiffe://ssf/overlaybd-controller`。
5. `overlaybd-csi`：标准 Node RPC 与受管捕获/释放协议，独立 session WAL。
6. `overlaybd-registrar`：固定 v2.13.0，注册真实节点的 CSI 信息。
7. 原 SSF Node 和 AgentENV storage adapter。
8. 原 AgentENV 容器和原生服务。

除配置 init 外，这些存储进程使用 Kubernetes native sidecar。正常退出按相反顺序进行：原生 VMM 和 adapter 先处理真实停止与清理，公共 Node 随后退出，再退出 CSI/worker/宿主/专属 daemon。CSI 收到 SIGTERM 后拒绝新 Stage/Publish，继续服务最终捕获、发布、Unpublish、Unstage；WAL 中非终态会话、借用和未发布捕获都阻止主动退出。不会用退出信号伪造停止证据。

Kubernetes 的 Pod 终止宽限期到期仍可能强制终止容器，节点断电也不受 sidecar 顺序保护。当前材料保留原 3600 秒宽限期；不能据此声称异常停机恢复已完成。

## 配置清单与挂载

复制 `nodes.local.example.json`，将 `node-a` 改成真实 Kubernetes Node 名，将 worker endpoint 改成 Controller 可达且位于证书 SAN 中的节点 DNS/IP。每个 AgentENV 节点都必须有一条清单记录。该清单没有凭据值；凭据由 Secret 文件挂载。

- 本地 POSIX：每个节点使用不同 `repository`，`accessible_nodes` 恰好为该节点；后端目录用独立 hostPath，不能指向 legacy 卷目录。
- 共享 POSIX/NFS：使用原 AgentENV 支持的共享文件系统；将 `repository_volume` 改为真实 NFS 或已挂载共享文件系统的 PVC，并令 `accessible_nodes` 为 `[]`。相同仓库 ID 的后端配置必须一致。
- OSS：`repository_backend` 设为 `oss`，配置采用 `../native/config.oss.example.json` 中的 `oss` 对象，使用独立 prefix。凭据文件挂载 `/etc/ssf-overlaybd/oss`，Secret 名 `ssf-overlaybd-oss`。不能把凭据写进 ConfigMap。

相同共享 repository 的 Controller worker endpoint 固定为清单中第一条，当前渲染器不做自动 worker 故障转移。所有节点的本地 WAL、upper 和宿主状态位于 hostPath `/var/lib/ssf/overlaybd`。Pod 重建保留它们；不得通过清空该目录处理恢复错误。

Node 启动时读取 Kubernetes Node UID，不要求管理员编造或手填 UID。CSINodeID 使用真实 Node 名，拓扑键为 `overlaybd.ssf.dev/node`。

## 凭据与前置公共组件

原 SSF 部署的 PostgreSQL、Guard、CSI Gateway、Coordinator 以及 snapshot-controller/CRD 仍需存在。额外准备：

| Secret | 用途 |
| --- | --- |
| `ssf-overlaybd-node-tls` | `tls.crt`/`tls.key`/`ca.crt`；SPIFFE `spiffe://ssf/overlaybd-node`；服务 SAN 覆盖 inventory worker endpoint |
| `ssf-overlaybd-controller-tls` | Controller 身份 `spiffe://ssf/overlaybd-controller`，验证相同 CA |
| `ssf-overlaybd-database` | `database-url`，独立 Driver schema 权限和所需公共 ledger 权限 |
| `ssf-overlaybd-oss` | 仅 OSS 使用，字段名须与配置的凭据文件名一致 |

节点的证据请求到原 adapter TLS 9444。overlay 自动给其 `evidence_peers` 添加 OverlayBD Node 身份。CSI 只挂载 `plan.pub`，不会挂载 node-control/dispatch 私钥；原生 daemon/宿主不挂载这些控制密钥。公开 sandbox API 和 SDK 不改变。

Guard 与 Coordinator 都需注册以下同一 profile（Coordinator 由 overlay 添加；Guard 按原配置合并，保留已有 profile）：

```json
{"name":"agentenv-overlaybd-ext4-v1","runtime":"agentenv","version":"volume-preparation-v1","volumeMode":"Block","filesystem":"ext4","initializeBlank":false}
```

专用 StorageClass 使用 `provisioner: overlaybd.csi.ssf.dev`，`parameters.repository` 指向 inventory 中的仓库，注解 `ssf.dev/volume-profile: agentenv-overlaybd-ext4-v1`；保留 `Retain`。分别设置 Immediate/WFFC。OverlayBD 内容由原生建卷操作准备，不能启用 generic 空盘格式化。默认不启用离线扩容；显式启用方法与当前限制见下节。

## 离线构建与渲染

从双仓共同父目录构建（本机未执行 Docker）：

```bash
docker build -f sandbox-storage/drivers/overlaybd/Dockerfile -t ssf-overlaybd:dev .
```

从 `sandbox-storage` 渲染；`api-key-file` 使用现有凭据文件，输出包含 Secret，文件权限保持 0600：

```bash
python3 scripts/render_agentenv_native.py \
  --api-key-file /安全路径/agentenv-api-key \
  --driver overlaybd.csi.ssf.dev \
  --controller-endpoint ssf-csi-gateway.ssf-system.svc:9443 \
  --overlaybd-inventory /配置路径/nodes.json \
  --overlaybd-image ssf-overlaybd:dev \
  --output /输出路径/agentenv-overlaybd.yaml

python3 scripts/render_overlaybd_controller.py \
  --namespace agentenv-system --image ssf-overlaybd:dev \
  --inventory /配置路径/nodes.json > /输出路径/overlaybd-controller.json

go run ./tools/check-native-deploy -file /输出路径/agentenv-overlaybd.yaml
```

Controller 镜像内部同时包含 Go Driver、配置生成器、Rust 宿主和专属 daemon；各容器使用显式入口，不依靠 shell trap 杀死子进程。Controller 仅调用 worker 后端管理 RPC，不调用 CSI Node 供卷；业务 I/O 仍经过原 ublk。

上述命令只渲染/检查，没有 apply。`CSIDriver`、RBAC 和 StorageClass 是 cluster-scoped，不能因指定测试 namespace 就认为它们受 namespace 隔离。真实部署时需要结合已有集群资源核对。

### 管理员登记已有受管卷为静态 PV

动态 PVC 不需要下面的登记。静态入口只接受 **CSI 隔离 repository 中已有、空闲、具有已发布 head 的原生卷**。它不接管 legacy 卷，不创建或格式化后端，也不直接应用 PV。`expected-head` 是管理员从受管 repository 发布记录核对的完整摘要，不是镜像标签或文件路径。原生宿主必须协商 `static-registration-v1`。

1. 使用 `reclaimPolicy: Retain` 的 OverlayBD StorageClass，预先创建 Block PVC；显式填写 `spec.volumeName: overlaybd-static-data`、目标 StorageClass、容量和 RWO/RWOP（只读卷使用 ROX）。Guard 应已安装管理标签、保护 finalizer、profile revision 和 StorageClass UID。PVC 暂时 Pending 是预期行为，不要手工伪造这些保护字段。
2. 在对应 repository worker 的受控管理环境中执行镜像内的工具；提供管理员 Kubernetes 只读凭据、Driver 数据库连接文件及本地宿主 UDS。工具没有创建 Kubernetes 对象的行为：

   ```bash
   overlaybd-register \
     --namespace ssf-test --pvc data --pv overlaybd-static-data \
     --repository node-a-repository --native-volume '<受管原生卷 ID>' \
     --expected-head '<已核对的当前 head 摘要>' \
     --backend-socket /run/ssf-overlaybd/private/backend.sock \
     --database-url-file /run/ssf-admin/database-url \
     --kubeconfig /run/ssf-admin/kubeconfig > overlaybd-static-data.json
   ```

   以上 repository、路径及 ID 必须替换为实际配置。只读原生卷同时添加 `--readonly`。不要把数据库凭据或管理 kubeconfig 加入普通运行时容器；这是受限管理员工具。
3. 检查输出的 `claimRef.uid`、Block、容量、CSI handle、Retain 和本地卷 nodeAffinity，然后手工 `kubectl apply -f overlaybd-static-data.json`。继续使用原 AgentENV PVC 挂载接口，无需 `/volumes import`。

登记在 Driver 目录内持久固定 backend、PVC UID、初始 head 和访问模式；同一输入重试不会再读取后续写入后的 head。更换 PVC UID、注册第二个 PV 别名、容量不足、未协商能力、活动原生引用和已删除身份均拒绝。输出前发现 PVC 变化时不返回清单；可能已经保留的 Driver 登记不会自动解除，避免未知结果导致重新接管。已有 PV 必须与生成 spec 完全一致，否则工具拒绝替换。

本机已有 Go/临时 PostgreSQL 登记回放测试及 Rust 原生检查单测；实际静态绑定、ublk 挂载和数据回读仍需 Linux/Kubernetes 环境验证。

### Kubernetes 直接快照与类配置

`render_overlaybd_classes.py` 从同一后端 inventory 输出每仓库的 Immediate/WFFC StorageClass
和 `overlaybd-native-retain` VolumeSnapshotClass，均保留 Retain；不修改现有 StorageClass，
也不自动应用。若该 Driver 尚无默认快照类，管理员确认后可显式添加默认注解：

```bash
python3 scripts/render_overlaybd_classes.py \
  --inventory /配置路径/nodes.json --default-snapshot-class > /输出路径/overlaybd-classes.json
```

原生联合快照涉及同一沙箱的所有参与卷。PVC 直接引用未指定快照类时会查询对应 Driver
的默认 VolumeSnapshotClass；混合 generic CSI 卷也需各自的默认类及真实 snapshot 能力。
不要为同一 Driver 配置多个默认快照类。

Controller 的 `captureCoordinator` 使用独立 mTLS 身份调用公共 `CaptureVolume`，
Coordinator 仅允许 `storage_peers` 中的 `spiffe://ssf/overlaybd-controller`。上述两个
渲染入口已同时配置这条链路；它不获得 Scheduler/Node 的调用权限。standalone 若启用
Kubernetes PVC/直接快照，也需在独立协调服务配置相同 storage peer 和实际 HTTPS 地址。

活动卷固定原 request/owner/run/binding 后，由原生 `/internal/storage/captures` hook
调用原 Orchestrator snapshot，复用原冻结、内存/rootfs/混合卷捕获和 thaw。该内部 hook
除原 API 鉴权外，还通过独立 StorageProvider 验证已登记任务及运行身份，不能由客户
提交任意 barrier。旧节点没有 `storage_capture_requests` 能力时明确拒绝。

任务启动记录先 fsync；HTTP 响应丢失不再次捕获。已封存的部分结果通过原 receipt
继续发布，已完成联合 checkpoint 可在原 run 结束后重放。尚未封存的未知结果不能
创建新 barrier 或改读当前 head，当前会保持 Pending；仍需完善这类中断的恢复调谐。

空闲卷直接固定 published head；暂停卷仅在所有旧 run 已有可信停止事实、binding 和
attachment 已释放后进入同一受保护读取，保留 UseHold，捕获期间阻止 resume。
直接快照从派发前开始保护来源 PVC；完成后独立保留联合 checkpoint，直到直接快照
真正删除。上述路径已添加本机数据库/协议回归，真实 VM/ublk/K8s 快照测试仍未执行。

## 离线扩容配置与手动验证

仅在专用测试 inventory 启用，不修改现有 StorageClass/profile：

```json
{
  "offline_expansion": true,
  "overlaybd_tools_volume": {
    "hostPath": {"path": "/var/lib/aenv/env/overlaybd", "type": "Directory"}
  }
}
```

以上两项合并到现有 inventory，保留 nodes/repository 等字段。目录应包含原 AgentENV
已验证的 `bin/overlaybd-resize`，示例路径须按实际安装位置调整。节点 overlay 只读挂载
该目录并给专属 daemon 提供配置；不重新实现或下载另一版工具。共享节点应各自具备
同一已验证工具版本。Controller、classes 渲染命令都读取同一 inventory。

生成的 Controller 启用 offlineExpansion；resizer 固定为
[csi-resizer v1.13.2](https://github.com/kubernetes-csi/external-resizer/releases/tag/v1.13.2)，
包括 PVC status 权限。仅核对官方发布记录，未完成该镜像在真实集群的兼容验收。
Guard 和 Coordinator 使用同一不可变 profile，增加 `controllerOfflineExpansion: true`；
不要改变已经固定旧 profile revision 的 PVC。新的 StorageClass 才设置 allowVolumeExpansion。

在主场景已写入文件且删除 A、确认最终发布/释放后，手工扩大原 PVC（例：1Gi→2Gi）：

```bash
kubectl -n "$TEST_NAMESPACE" patch pvc "$PVC_NAME" --type merge \
  -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}'
kubectl -n "$TEST_NAMESPACE" get pvc "$PVC_NAME" -w
```

观察 Controller/resizer 日志和 PVC capacity，随后仍用原 AgentENV 创建 API 启动 B 并
引用同一 PVC，检查 `/data` 文件未改变且 `df -h /data` 容量增大。不调用 import，
不创建初始化 Pod，不在宿主手工 mkfs。再分别保持 A 运行和暂停 hold 提交扩容请求，
应保持等待且不修改已发布容量，解除对应占用后才完成。

当前真实扩容未执行；daemon 重启后的维护设备、初始创建和未知 restack 恢复仍有缺项。
此节是测试配置和步骤，不表示生产可用或端到端已通过。

### 原生 OverlayBD 运行证据

主场景工具现在有独立的 `--driver-kind native-overlaybd` 模式。使用专用
`agentenv-overlaybd-ext4-v1` StorageClass，在已有隔离测试 namespace 中运行：

```bash
python3 scripts/native_pvc_e2e.py --execute \
  --context "$KUBE_CONTEXT" --namespace "$TEST_NAMESPACE" \
  --url "$AENV_URL" --image "$AENV_IMAGE" \
  --storage-class "$OVERLAYBD_STORAGE_CLASS" \
  --driver-kind native-overlaybd --output "$TEST_OUTPUT"
```

`AENV_IMAGE` 是当前原生 AgentENV 已验证可启动的沙箱镜像，不是 CSI Driver 镜像。
`AENV_API_KEY` 从环境读取；`TEST_OUTPUT` 必须是尚不存在的目录。Immediate、WFFC
分别选各自的 StorageClass 执行。该命令会创建 PVC 和沙箱并保留资源，不能在本机
没有真实环境时把它记为已执行。`--lifecycle` 额外覆盖 pause/resume、快照、Fork
和模板继承/清空/替换。

工具先校验 profile、PV Driver 和精确 handle。A 写入、B 读回时，分别在原节点 Pod
的 `overlaybd-daemon` 容器运行只读 `overlaybd-evidence`，采集同一卷的原生 WAL
摘要、已记录的层 head，以及匹配 boot/主次设备号的实际 `/dev/ublkb*` 块设备。
普通文件、其他卷、无层记录或 generic CSI 不能通过此项。原生 WAL 中的请求凭据
不导出。该观察仅作为测试证据，不是挂载、清理或恢复授权；设备号观察不能替代
生产路径的 daemon/session/epoch 验证。

也可手动查看一个卷的证据：

```bash
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" exec "$NODE_POD" \
  -c overlaybd-daemon -- overlaybd-evidence -volume-handle "$VOLUME_HANDLE"
```

`VOLUME_HANDLE` 取自该 PVC 绑定 PV 的 `spec.csi.volumeHandle`。新工具已打入 Driver
联合构建镜像。预检使用 `native_preflight.py --native-overlaybd`；主机设备检查需在
真实 Worker 上执行。采集器与验证规则的本机单测通过，不代表 ublk/VM 真机验收通过。

宿主启动现在要求 daemon 显式声明 `durable-restack-v1`；Node Driver 及扩容 worker
同样校验该能力。旧宿主即使声明 `layer-capture-v1` 或 `offline-expansion-v1`，也不能
绕过持久化边界检查。遇到 missing capability 时更新整套联合构建产物，不修改能力列表
或降低校验。该能力仅保证返回封存结果前的同步与配置持久化，不宣称所有中断都可恢复。

完整手动主场景见 [设计 29](../../../../design/29-overlaybd-pvc-manual-validation.md)。

联合构建的 Dockerfile 专属 `.dockerignore` 已提供，按共同父目录作为上下文排除两个
仓库的 target、Git 元数据、验证报告和基线备份。否则本机的 Cargo 产物和基线会被整批
发送到构建器。保留这些文件与对应 Dockerfile 一起使用；本机未实际执行 Docker 构建。
