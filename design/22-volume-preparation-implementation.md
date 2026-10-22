# SSF 自动卷准备与直接 PVC：实现、部署和验证记录

> 2026-10-21 独立化更新：本页保留此前设计/阶段记录。当前 AgentENV 的消费与准备协调、账本事务在 `sandbox-storage` 的独立 `ssf-coordinator` 中执行；Scheduler 为轻量客户端，standalone 同样独立。以 [03](03-framework-design.md)、[23](23-current-implementation-architecture.md)、[26](26-independent-coordinator-implementation.md) 和 [13 部署](13-agentenvadapter-installation-and-deployment.md) 为准。旧内嵌宿主、`go.ssf.mod`、`-tags ssf`、原仓库 SSF overlay 不再适用。本次保留 Block/ext4 和全部原生功能，未新增 OverlayBD CSI Driver。

本次增量实现以 [设计 21](21-ssf-volume-provisioning-and-runtime-adapter-design.md) 为依据。入口是 Kubernetes PVC 和原 AgentENV `volumeMounts[].pvc`；最小使用说明见 [两步验证](20-kubernetes-pvc-agentenv-mount-validation.md)。本文件区分已交付代码、已执行本机检查及未执行真机验收，不能据此宣称用户集群或厂商 Driver 已通过验收。

## 实现对应关系

| 层次 | 实际入口 | 职责 |
| --- | --- | --- |
| Kubernetes 原生存储 | 原 SC/PVC/PV、external-provisioner、CSI Controller/Node | 供卷、绑定、后端快照/克隆/扩容/回收权威不变 |
| 公共准备协调 | `SSF/preparation`，由原 Scheduler / standalone 本地宿主运行 | Observe 固定 PVC UID、profile/source revision；Immediate 后台准备；WFFC 复用原调度选点；持久恢复/排空 |
| 公共使用权 | `SSF/ledger/preparation.go`、`consume.Coordinator` | canonical driver/handle 防别名双写；准备操作和 VM run 分离；CAS；handoff hold 与运行 Acquire 原子接续 |
| 公共节点 | `api/ssf/v1/node.proto`、`node/preparation.go` | 签名准备计划、CSI Stage/Publish、回执验证、释放；不伪造 RuntimeReady 或 VMM 停止证据 |
| AgentENV VolumePreparation | `adapter/agentenvadapter/volume_preparation.go`、`platform_linux.go` | Describe/Plan、Prepare/Inspect/ReleasePreparation；设备身份、全量读取、格式意图 WAL、ext4 UUID |
| AgentENV RuntimeMount | `native_control.go`、原 Rust StorageProvider/PreparedExternalDrives | PVC 直接引用、原 create/pause/resume/snapshot/Fork/template 链路，VMM 始终由原 Orchestrator 管理 |
| Guard | `guard/preparation.go`、`kubernetes/profile.go` | profile 默认化、管理标签/finalizer、源身份固定、状态保护、拒绝普通 Pod 争用；不操作设备 |

[逻辑分工图源](figures/ssf-volume-preparation-adapter.mmd)

准备协议为 `volume-preparation-v1`，原生 hook 升至 **4**；旧 hook 3 仍可用于原功能，但 profile 启用时旧节点不能宣称支持准备能力。新增公共 RPC：`PrepareVolumeContent`、`InspectVolumePreparation`、`ReleaseVolumePreparation`。Go protobuf 已更新；Rust SDK 从同一 proto 构建。Node 到适配器使用 Pod loopback HTTP 与独立 token，Scheduler 到公共 Node API 继续使用 mTLS。

## 身份与数据安全

准备记录以 PVC UID 为入口，Bound 后保存 PV UID 和后端身份；profile、来源对象 UID、格式 UUID 随准备意图固定。Snapshot 来源同时固定 Snapshot 和 Content UID，首次内容就绪前再次核验。恢复来源不允许获得初始化授权。PVC 别名不能绕过现有 driver/handle 去重；新的同名 PVC 不能接替旧请求、恢复或模板引用。

准备状态为 WaitingBinding / WaitingConsumer / PendingPreparation / Preparing / ContentReady / VolumeReady / NeedsRecovery / Failed。`ContentReady` 不是可交付运行的证明，必须先获得进程/FD 释放回执并完成 CSI 清理。节点回执包含 operation、plan digest、binding、PVC、node/boot、epoch 和 UUID；Rejected 释放回执只证明安全撤销，不能成为 Ready。

格式化前打开并验证块设备的 major/minor/容量，在独占准备权下读取完整广告容量。短读、提前 EOF、额外内容、读取异常、分区表、未知非零数据、非 ext4 和只读空盘都拒绝。格式意图先落 bbolt WAL 再执行 mkfs；有未知意图时不能通过新 operation 再格式化。设备 flock 由子进程继承，适配器进程退出后存活的 formatter 仍阻止下一次准备或交接。

已有 ext4 保留数据。未知格式结果恢复执行非破坏性完整检查；已准备运行卷的再次挂载仅核验身份/UUID，不因暂停或快照留下的合法日志状态执行破坏性修复。GuestReady 仍验证设备、挂载、容量和只读属性。

DELETE 只关闭新准入，不替用户停止沙箱。finalizer 等待准备、活动 writer、暂停 hold、attachment、checkpoint 来源 PVC/Snapshot/Content 引用解除。准备已发布但释放失败时，删除流程重试原节点释放；未派发计划的部分 Attach 走公共 AbortUnpublished。未知 formatter 结果或节点失联继续保留 WAL 和 finalizer。

## 原生 API 与生命周期

```json
{"image":"已验证镜像","volumeMounts":[{"pvc":{"namespace":"agentenv-system","name":"data"},"path":"/data"}]}
```

原 Gateway 鉴权、转发及创建成功码 201 保持；每项 `name`/`pvc` 恰选一个。namespace 必须授权，省略只使用明确部署默认值。直接 PVC 内部有 UID 固定的消费描述，但不创建用户可见 `/volumes` 别名、不再创建 PVC，也不要求 import。普通删除沙箱保留用户 PVC。

原 `/volumes` facade、混合 legacy、无卷及关闭插件路径保留。对于启用 profile 的卷，是否来自 import 不决定格式化资格，统一由来源和内容规则决定。未启用 profile 的旧卷保持旧策略，不批量接管或重新格式化。

PVC 来源通过持久 mount 描述进入 pause/resume、v2 connect、联合快照、Fork 和模板恢复；RW 子实例从共同快照创建独立 PVC，RO 保留共享语义。模板省略继承、`[]` 清空、显式替换仍沿用已有实现。直接 PVC 快照无显式类时选择同 Driver 的唯一默认 Retain VolumeSnapshotClass；默认类注解采用 [Kubernetes 官方约定](https://kubernetes.io/docs/concepts/storage/volume-snapshot-classes/)。

Rust 侧只增加通用 PVC 引用、校验、持久 hook 数据和生成模型，不添加 Kubernetes/CSI/共享账本实现。CLI 客户端新增可表达 PVC 的 typed mount 构造方法，原接受命名卷的方法继续可用。

## 部署增量

先按 [13 原生部署](13-agentenvadapter-installation-and-deployment.md) 更新同一套 Gateway/Scheduler/node 工作负载、镜像、数据库和凭据。没有新增沙箱控制面或初始化 Pod。原 Kustomize overlay 与 standalone 配置已包含：

- `control.json.volume_profiles`、`preparation_node_port`、`preparation_server_name`；Scheduler 启动时运行公共协调器。
- SSF Node 的 `PreparationEndpoint=http://127.0.0.1:9445/storage/volume-preparation` 与 `PreparationTokenFile=/etc/agentenv-storage/hook-token`。
- Node mTLS 允许原 Scheduler URI 身份；自定义 namespace 的渲染器同步改 ServerName。
- adapter 的 e2fsprogs/util-linux、原 WAL/设备目录和挂载传播；联合 drain 等待尚未释放的准备操作。

数据库先执行原 `ssfctl migrate`。schema 4 增加准备表，schema 5 幂等回填旧 checkpoint 的来源 PVC pin 并撤销冲突回收许可；角色授权见 `SSF/deploy/ledger-roles.sql`。先迁移、更新公共 Node/适配器/Guard，再启用专用 SC 和直接 PVC 流量。不要让旧节点在启用新 profile 后静默跳过准备。

Guard 需要同时启用原 validating webhook 和新增 mutating webhook。参考 `deploy/guard-preparation.example.json`；profiles 必须与 control 配置一致。`CoordinatorUsers` 应为实际 Scheduler ServiceAccount 的完整用户名。若 standalone 使用不同 Kubernetes 凭据，明确列入该可信身份，不能配置通配普通用户。

从现有厂商 SC 导出 JSON，渲染一个**新名字**的专用 SC（不改原 SC）：

```bash
kubectl get storageclass 你的厂商存储类 -o json > /tmp/vendor-storage-class.json
python3 scripts/render_volume_preparation.py \
  --storage-class-json /tmp/vendor-storage-class.json \
  --name agentenv-block --binding-mode Immediate \
  --namespace agentenv-system --ssf-namespace ssf-system \
  --ca-file /你的Guard-CA证书 --output /tmp/ssf-preparation.json
```

渲染工具不连接集群。输出保留 vendor provisioner、parameters、topology 等配置，去掉原默认类注解并设置 profile/Retain，包含 Guard ConfigMap、命名空间限定的 mutating webhook 和专用 SC。Guard ConfigMap 使用 base 的标准证书路径；已有自定义配置应先合并，保留现有自定义项，不直接覆盖。由操作者审阅后 apply，并滚动重启 Guard 使配置生效；本次没有执行任何集群 apply。

WFFC 使用新名称并将 `--binding-mode` 改为 `WaitForFirstConsumer`。静态供给由管理员原生创建 CSI PV（Block、Retain、正确 driver/handle、容量/拓扑/SC），再创建该 class 的 PVC，可通过 `spec.volumeName` 预绑定。禁止为同一后端创建可并发使用的多个独立身份。

## 验证入口和证据

本机检查报告集中在 `SSF/validation/volume-preparation/`。测试 fixture 与真实 Driver 分开，不把交叉编译当作 Linux 执行。

| 项目 | 工具 / 证据 |
| --- | --- |
| 公共/适配器 Go 单测、race、vet，普通及 SSF 构建，协议/API 兼容 | `scripts/validate_native_foundation.py` → `foundation/report.json` |
| 独立 PostgreSQL，准备竞争/CAS/接续/删除、原生命周期引用 | `scripts/native_database_test.py` → `database/report.json`；仅临时 Unix socket DB |
| OpenAPI 生成一致性 | `scripts/check_native_openapi.py`，固定 generator 7.22.0 |
| Linux Rust 类型检查 | `scripts/check_agentenv_cross.py` → `rust-cross/report.json`；不执行 VMM |
| 原工作负载增量渲染 | `scripts/validate_native_deployment.py` → `deployment/report.json` |
| Linux 实际 loop 设备初始化/拒绝/未知格式意图/遗留进程锁 | `scripts/volume-preparation-device-tests.sh`；只能在专用 Linux 测试机执行，未在本机执行 |
| K8s PVC → 原 API → 数据持久化 | `scripts/native_pvc_e2e.py --execute`；Immediate/WFFC 各一次；本机未执行 |
| pause/resume/Fork/模板 | 同上加 `--lifecycle`；真实环境未执行 |
| 原生故障、证据、引用不变量、清理 | `native_fault_e2e.py`、`native_evidence.py`、`native_invariants.py`、`native_cleanup.py`、`tools/cleanup-pvc` |

### P01～P12 覆盖矩阵

| 项目 | 已有本机/工具覆盖 | 真机状态 |
| --- | --- | --- |
| P01 Immediate 无沙箱准备 | 公共协调器并发 DB 用例；PVC E2E 在创建沙箱前等待 VolumeReady | 未执行 |
| P02 创建、写文件、删建保留 | `native_pvc_e2e.py` 主线，实际 guest 字节比较 | 未执行 |
| P03 facade 与 K8s 共用规则 | 原 facade 测试、profile 准备入口；分别跑原 lifecycle 与 PVC 工具 | 未执行 |
| P04 四种供给/绑定组合 | 现有绑定测试；专用 Immediate/WFFC SC，静态使用 `--pvc` | 未执行 |
| P05 内容拒绝与保留 | 全量读/短读单测；专用 loop 设备测试脚本 | Linux 设备未执行 |
| P06 恢复来源/身份 | Kubernetes 源 UID 校验、恢复禁初始化契约、固定 Snapshot/Content UID | 真 CSI 恢复源损坏未执行 |
| P07 竞争 | 两个协调器共享准备记录的 PostgreSQL race 用例、canonical writer/hold 账本测试 | 多副本/厂商 Driver 未执行 |
| P08 中断/回执/重启 | Node WAL/同 operation 跨 boot 接续、旧 epoch 拒绝、格式意图/继承设备锁测试入口 | 主机重启与厂商故障未执行 |
| P09 接续/释放/删除 | PostgreSQL 原子 handoff；ContentReady 删除重试；维护和删除互斥 | 跨物理节点未执行 |
| P10 原生命周期 | 原生 DB 用例、PVC 工具 `--lifecycle` | Linux/KVM 未执行 |
| P11 原接口兼容 | 普通/SSF 构建、原服务测试、无卷和插件关闭用例 | 原代理/P2P 真机未执行 |
| P12 越权/伪造/重建 | Guard、协议证据、direct PVC UID/namespace 测试 | API server admission 未执行 |

### 必须保留的边界

- 同 boot 进程重启重放原 WAL；boot 改变时仅在相同物理 Node UID 上重签原准备计划，operation、epoch、backend、profile/source 和格式意图不变。公共节点重新 Stage/Publish，已有内容回执必须能在 adapter WAL 中核实，再复用原 UUID。已释放操作不能复活。节点失联、WAL 缺失或不完整格式化不能证明结果时继续拒绝并保留占用；不提供绕过 WAL 的强制重格式化命令。该接续有节点单测、数据库 fixture 和 Linux loop 测试入口，物理主机重启尚未执行。
- 直接修改受管 PVC 的扩容请求需要现有公共离线 maintenance 权限。原 facade 离线扩容保留；尚未新增任意 `kubectl patch pvc` 自动预约扩容协调器。
- 真实 API server 默认化、真实 CSI 的源恢复和四种绑定组合、Linux/KVM 生命周期及故障矩阵没有在本机执行。现有故障工具不能被当作所有准备阶段注入点已实际覆盖。
- 旧未受 profile 保护的 PVC 不会自动变成可格式化新卷；有活动/未知引用时不接管。旧记录按 UID 保留。

因此，本次代码交付及 fixture 检查不等于设计 21 的真机故障矩阵已经通过。以具体报告为准，不通过删除 WAL、finalizer 或引用记录来制造“通过”。

## 本轮实际检查结果（2026-10-21）

汇总证据：[report.json](../sandbox-storage/validation/volume-preparation/report.json)。源码快照由 foundation 的哈希列表固定，`source-stability` 通过。

- Go 普通/SSF 单测、race、vet、Linux 构建、协议重生成和原 API 兼容检查通过；报告保留条件跳过项。
- 独立 PostgreSQL race 集成通过且无 skip，补跑了普通单测中跳过的 34 个数据库用例；包括多协调器竞争、准备删除、丢回执后跨 boot 接续，以及旧 WAL 不可达时不重绑。只读不变量工具对正常库通过，对故意损坏的 writer fixture 正确失败。
- Kubernetes 集群用例及 3 个原生 RedisBindingStore 用例未执行，后者因本机没有 redis-server。它们没有被计为通过。
- AgentENV Linux 目标 `cargo check --all-targets` 通过；原 CLI/SDK 的全部 target 类型检查、SSF Rust Node SDK 的 protobuf 生成/类型检查通过。类型检查不代表 Linux 原生测试或 KVM 执行。
- OpenAPI 使用固定 generator 7.22.0 重生成，12 个 Rust 文件一致；两个 namespace 的原生部署渲染和凭据检查通过。
- AgentENV 补丁精确重放通过，共 78 个变化路径，包含已有用户改动。未执行提交或推送。
- 新 Linux 设备测试已通过交叉编译；实际 loop 设备、主机重启、真实 K8s/CSI/KVM 和完整 P01～P12 故障矩阵未执行。fixture 与真实 Driver 的结果必须分别运行、分别归档。
