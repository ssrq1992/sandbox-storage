# 完整存储部署与模块归属

> 2026-10-21 独立化更新：本页保留此前设计/阶段记录。当前 AgentENV 的消费与准备协调、账本事务在 `sandbox-storage` 的独立 `ssf-coordinator` 中执行；Scheduler 为轻量客户端，standalone 同样独立。以 [03](03-framework-design.md)、[23](23-current-implementation-architecture.md)、[26](26-independent-coordinator-implementation.md) 和 [13 部署](13-agentenvadapter-installation-and-deployment.md) 为准。旧内嵌宿主、`go.ssf.mod`、`-tags ssf`、原仓库 SSF overlay 不再适用。本次保留 Block/ext4 和全部原生功能，未新增 OverlayBD CSI Driver。

默认部署为：原生 Kubernetes CSI 管理卷；公共消费模块嵌入现有项目控制面；公共 Node Agent 直接执行节点挂载；项目 Runtime Adapter 接入沙箱。没有默认独立的 `ssf-controller-xxx` Pod。部署图参考[用户指定架构样式](assets/K8S-CSI架构分析.md)，参考原文保持不变。

2026-10-19 实现对齐：Node Agent 对项目节点服务提供 mTLS gRPC；访问厂商 Node Driver 仍使用本机 UDS。图中的项目分支是后续接入点，本次仅交付公共契约与验证适配器。条件 Gateway 除 mTLS allowlist 外，读取共享账本验证 Binding、真实 node_id、attachment 状态与请求能力，再转发标准 CSI RPC；调用方通过 gRPC metadata 传递 Binding ID，不修改 CSI 消息。部署文件见 [SSF/deploy](../sandbox-storage/deploy/base.yaml)。

## 1 完整部署图

2026-10-21 设计增量：原 Scheduler 内新增公共 VolumePreparation Coordinator，现有 agentenv-storage sidecar 注册卷准备及 RuntimeMount 两部分；Guard 补短时准入默认化和准备保护。原 Deployment/DaemonSet 结构保持，新增职责及最新图见 [21 第 3 节](21-ssf-volume-provisioning-and-runtime-adapter-design.md#3-关键模块与部署归属)。下面图为此前部署基线，尚未展开卷准备模块。

![存储完整部署与内部模块](figures/ssf-full-deployment.svg)

[放大SVG](figures/ssf-full-deployment.svg) · [PNG](figures/ssf-full-deployment.png) · [Mermaid](figures/ssf-full-deployment.mmd) · [固定布局生成源](figures/render_full_deployment.py)

固定图和Mermaid由同一份组件/包含关系清单生成；框嵌套表示工作负载、Pod、容器/进程、内部模块的归属，不表示串行调用。每类Pod画一个实例，副本/节点复制按部署策略配置；集群级Deployment不必位于控制平面宿主。Kubeadm独立静态Pod只是控制面示例。

Cube与AgentENV分支按实际项目选择；普通VM是另一Runtime分支。图包含相关既有辅助Pod及可选替代部署，不表示必须全部启用。后端服务按已有系统边界表示，不展开其内部Ceph/NFS Pod拓扑；与存储无关的UI/网络组件省略。[调用详情](figures/ssf-module-calls.svg)

## 2 来源图例

| 标记 | 来源 | 示例 |
| --- | --- | --- |
| K 灰蓝 | 原生Kubernetes或官方CSI组件 | PV Controller、kubelet、external-provisioner |
| N 青蓝 | 新增公共代码，可嵌入已有进程 | Resolver、消费Coordinator、Node Agent、Guard |
| P 棕色 | 项目现有或定制适配 | Cubelet桥、AgentENV ManagedBlock、Runtime Adapter |
| D 棕色 | 厂商/定制存储服务端Driver | Controller/Node/Identity、legacy CSI facade |
| I 灰色 | 复用基础软件或宿主资源 | PostgreSQL、etcd、containerd、Firecracker |
| B 绿色 | 实际存储后端 | NFS、Ceph、云盘、对象层 |

虚线灰边为条件部署，不改变来源分类。N模块嵌入P进程时仍标N，不能因此画成额外Pod。新增Pod可使用原生Deployment资源，但内部实现不因此变成原生K8S组件。

## 3 原生 Kubernetes 与 CSI Pod

| 实际部署 | 容器/进程 | 内部职责与来源 |
| --- | --- | --- |
| kube-apiserver独立静态Pod或托管服务 | kube-apiserver | K：存储资源REST/watch、认证/RBAC、admission调用 |
| kube-controller-manager独立静态Pod | kube-controller-manager | K：PV绑定/回收、原生Pod Attach/Detach、PVC/PV protection；不是多个控制器Pod |
| kube-scheduler独立静态Pod | kube-scheduler | K：Pod调度、VolumeBinding；不直接调度独立沙箱 |
| etcd独立静态Pod或外部服务 | etcd | I：原生对象持久化，与消费账本不同 |
| CSI Controller Deployment/StatefulSet → Pod | external-provisioner | K：唯一动态Create/DeleteVolume及clone供卷 |
| 同CSI Controller Pod独立容器 | 厂商Driver | D：同一进程的Identity + Controller及后端SDK |
| 同Pod条件容器 | external-attacher | K：原生Pod/单独认证VA模式，不管理同一SSF Direct attachment |
| 同Pod可选独立容器 | external-snapshotter、external-resizer | K：分别执行Snapshot与ControllerExpand管理RPC，不串行互调 |
| 同Pod推荐容器 | livenessprobe | K：Identity Probe |
| 独立snapshot-controller Deployment → Pod | snapshot-controller | K：Snapshot/Content协调，通过API与snapshotter协作，不是上面同名sidecar |
| 每节点CSI Node DaemonSet → Pod | 厂商Node Driver | D：Identity + Node、真实node_id、Stage/Publish/逆操作及按需Expand |
| 同CSI Node Pod独立容器 | node-driver-registrar、livenessprobe | K：注册/发现与Probe；CSI服务socket与注册socket分别存在 |

原生sidecar经共享UDS调用Driver，继续使用原有K8S生命周期。普通Pod及基础设施PVC仍由kubelet消费，与独立沙箱按资源/操作owner隔离。[官方CSI部署](https://kubernetes-csi.github.io/docs/deploying.html)

## 4 默认新增 Pod 与嵌入模块

### 4.1 实际新增部署

| 部署 | 容器/进程 | 内部公共模块 |
| --- | --- | --- |
| SSF Node Agent DaemonSet → 每适用节点Pod | ssf-node-agent | Node API、Registry/SessionManager、Prepare/Release、Stage/Publish、WAL、RuntimeBinding、Recovery/GC、telemetry |
| SSF Guard Deployment → Pod | ssf-guard | 原生资源/消费者准入校验、删除/迁移许可校验、读取共享使用账本、健康/诊断 |
| CSI Controller Pod内条件新增容器 | ssf-csi-gateway | mTLS、operation/volume/node/epoch校验、只允许Attach/Detach、本地UDS调用 |
| 自建账本StatefulSet → 独立Pod〔可选〕 | postgres | 事务/CAS、use operation、writer、Binding/Attachment/pin、排空屏障 |

Guard独立部署，使用Service/TLS、ValidatingWebhookConfiguration和RBAC；这些配置对象不是额外Pod。Guard不在准入中执行排空或写账本。Gateway仅在Controller无合适认证网络入口时需要，允许必要的身份/探测/能力只读查询，有副作用RPC仅Attach/Detach，不接管原生Create/Delete/Snapshot/Expand。

账本优先复用外部事务服务或满足要求的项目数据库独立schema，自建时才新增PostgreSQL Pod。所有项目消费同一物理资源时共享规范身份和使用权威，不能每个项目一套独立writer锁。

### 4.2 CubeMaster：嵌入现有 Pod

CubeMaster Deployment → 原有Pod → cube-master容器/进程：

| 内部模块 | 来源 | 职责 |
| --- | --- | --- |
| Volume API / ControllerPlugin / 项目选点 | P | 原接口适配、租户/legacy映射、计算lease、创建RPC |
| Native facade / PVC-PV Resolver | N | 原生资源操作与消费描述，唯一管理入口 |
| DelayedBinding | N | WFFC动态/静态桥、计算预留、原生结果确认 |
| Use授权/Drain/账本客户端 | N | writer、Binding、node pin及排空许可 |
| Attachment / BuildNodePlan | N | 请求直接Attach，下发当前publish_context和票据 |
| Registry/SessionManager | N | Controller端点、身份/能力、版本化长连接 |
| Step Runner/Recovery | N | 持久步骤、重复请求、未知结果及补偿 |

这些N模块为同一公共实现，不是Cube专属再写一套存储逻辑，也不是新增的Pod/容器。

### 4.3 AgentENV：嵌入现有权威入口

保留gateway与scheduler两个独立Deployment/Pod。gateway负责API路由、卷引用与身份传递；在scheduler/权威创建编排入口增加P层适配，嵌入与上表相同的N层Resolver、DelayedBinding、Use/Drain、Attachment/BuildPlan、SessionManager和step runner。此处是拟改造设计，不声称现有scheduler已经具备卷编排。

2026-10-20 将该边界具体化为 [AgentENV 原生接入方案](16-agentenvadapter-native-integration-design.md)：默认 ControlPlugin 编译进原 Scheduler；原生 Orchestrator 保持沙箱生命周期权威，调用独立 StorageProvider/NodePlugin。沿用原 Kustomize base 加 overlay，保留节点 headless Service、原 API、心跳和 P2P。当前 `agentenvadapter/scripts/render.py` 的独立 agentenv-control、关闭 reporting 和静态 worker 部署属于历史原型，不符合本节。完整目标拓扑及部署验收见 [新部署图](figures/agentenv-native-deployment.mmd) 与 [实施规范](17-agentenvadapter-refactoring-and-validation.md#3-原生部署-overlay-规范)。

Go模块优先在现有Go服务内复用；Rust节点通过生成SDK访问公共Node Agent。若某部署的权威创建入口无法嵌入，则选择下节共享消费服务，不经进程内ABI强行绑定Go/Rust。

### 4.4 替代服务形态

可选共享消费Coordinator Deployment → Pod → coordinator容器/进程，承载同一组公共模块；项目通过gRPC调用。它替代项目内的协调执行权，不同时维护另一份使用权威，不管理第二套卷生命周期。选择此profile需计入额外RPC和故障域；默认嵌入式图与默认启动SLO不以此Pod为前提。

VA/Holder兼容profile按Driver逐项认证，存在API/watch与kubelet相关依赖，不属于默认直接消费。原生Pod仅按native owner使用卷。

## 5 节点 Pod、主机进程与 guest

| 执行位置 | 进程/模块 | 来源与职责 |
| --- | --- | --- |
| Pod外宿主服务 | kubelet，CRI containerd/CRI-O、shim/runc | K/I：平台Pod；kubelet注册Driver并维护原生Pod卷 |
| 公共Node Agent容器 | Agent及上述N模块 | 参考atelet挂载部分，直接CSI Node，hostPath WAL跨Pod重建保留 |
| Cube node DaemonSet → Pod | cubelet容器 | P：VolumePlugin桥、Runtime Adapter、Ready/停止观测、Reference恢复 |
| 同Cube node Pod | wait-node-prep init；可选cube-s3lvol | P：已有启动门槛/引擎，不是公共CSI执行器 |
| Cube runtime执行域 | 专用containerd/CubeShim/CubeRuntime、VMM、virtiofsd | P/I：真实进程；namespace归属按原profile，不因宿主可见PID称为systemd服务 |
| AgentENV node DaemonSet → Pod → agentenv容器 | server内部Runtime Adapter/ManagedBlock、既有orchestrator/checkpoint模块 | P：消费Block endpoint、槽位、readonly、就绪和释放 |
| 同AgentENV容器内子进程 | ublk-daemon/OverlayBD、jailer/Firecracker | P/I：保留旧卷路径，迁移后由Driver唯一持有相应设备 |
| 普通VM宿主profile | 管理进程、Runtime Adapter、libvirtd/QEMU等 | P/I：替代运行时接入，无需guest内容器 |
| 宿主内核/路径 | mount/block/ublk/KVM、SSF staging/targets/WAL | I/N：实际资源，不是Pod |
| 独立VM guest | guest agent、文件系统、业务进程/容器 | P：识别共享目录/磁盘并确认GuestReady |

文件参考Substrate共享树/virtiofs，块路径依实际VMM配置。Driver、Agent与runtime共享受管hostPath、mount propagation和设备权限；现有项目manifest尚不等于已经实现这套契约，需接入验证。CSI Controller不负责配置所有VMM。

## 6 既有配套 Pod

| 条件部署 | 容器/职责 |
| --- | --- |
| cube-node-bootstrap DaemonSet | invalidate-stale-node-prep、wait-pvm-host、cube-node-init等init；write-node-prep-ready运行容器 |
| cube-node-installer DaemonSet | shim/kernel/guest/agent四个独立install容器，同一Pod内 |
| cube-node-pvm DaemonSet | pvm-host-bootstrap init、hold-pvm-ready运行容器，仅PVM profile |
| MySQL/Redis/MinIO各自StatefulSet或外部服务 | 项目数据库、缓存、对象制品；不是SSF新增公共实现 |

部署事实依据：[CubeMaster](../CubeSandbox/deploy/kubernetes/chart/templates/master.yaml)、[Cube node](../CubeSandbox/deploy/kubernetes/chart/templates/node-daemonset.yaml)、[bootstrap](../CubeSandbox/deploy/kubernetes/chart/templates/node-bootstrap-daemonset.yaml)、[installer](../CubeSandbox/deploy/kubernetes/chart/templates/node-installer-daemonset.yaml)、[PVM](../CubeSandbox/deploy/kubernetes/chart/templates/node-pvm-daemonset.yaml)、[MySQL](../CubeSandbox/deploy/kubernetes/chart/templates/mysql.yaml)、[Redis](../CubeSandbox/deploy/kubernetes/chart/templates/redis.yaml)、[MinIO](../CubeSandbox/deploy/kubernetes/chart/templates/minio.yaml)、[AgentENV gateway](../AgentENV/deploy/k8s/base/gateway-deployment.yaml)、[scheduler](../AgentENV/deploy/k8s/base/scheduler-deployment.yaml)、[node](../AgentENV/deploy/k8s/base/agentenv-daemonset.yaml)。标N及新增P适配是本方案拟实现内容。

## 7 控制、管理与实际 I/O

| 路径 | 边界 |
| --- | --- |
| API → 原生sidecar → Driver Controller | 管理卷及资源；Resolver仅观测/通过facade提交原生声明 |
| 创建请求 → 项目内公共协调 | 直接调用，WFFC才等待必要的原生供卷/绑定 |
| Coordinator → Gateway/认证端点 → Controller | 仅Attach/Detach，成功后BuildNodePlan |
| 现有创建RPC → 项目node → Node Agent | 完整受约束计划；Node不临时远端Attach |
| Agent → Node Driver | 本机UDS，Stage/Publish与逆操作 |
| PreparedEndpoint → Runtime Adapter → guest | 资源交付与guest接入，不是CSI Controller调用Node |
| guest → 文件共享/虚拟设备 → 后端 | 实际数据I/O；不经过Coordinator/Gateway或CSI管理RPC转发 |
| 删除卷 → Drain许可 → 原生回收 | 原生provisioner唯一DeleteVolume，Gateway不转发删除 |

连线实线为控制/交付，虚线为API/状态协调，粗线为I/O。固定部署图仅画少量主路径，完整关系由调用图和本表补足。

## 8 启动与故障域

先使原生CSI、Node/CSINode及基础存储就绪；共享事务库就绪；Guard/保护与Node恢复/Driver会话就绪；项目控制面开放受管卷解析/创建。Guard可服务原生基础对象的范围应精确配置，避免引导环或阻断非受管基础设施。

平台数据库、账本和MinIO持久化使用独立原生PVC/外部服务，不依赖SSF消费路径。Coordinator故障不转发也不中断已有数据面；新使用须权威授权。Node重启先核对WAL/mount/device/runtime，不能按Pod重建判定资源空闲。管理API不可用不转直接供卷。回收与迁移遵守[生命周期设计](05-kubernetes-integration.md)。
