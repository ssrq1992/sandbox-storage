# Kubernetes 卷资源与生命周期集成

本方案所有正式卷都通过真实 StorageClass/PVC/PV 和原生 CSI 组件管理。SSF 编排运行时需要的卷内容准备，以及独立 VM 的 attachment、节点发布与 guest 接入。供卷和消费按操作固定权威并建立删除屏障，避免两个协调器对同一资源发出矛盾操作。

2026-10-21：新增设计以 [21：卷管理与两部分 Adapter](21-ssf-volume-provisioning-and-runtime-adapter-design.md) 为准。StorageClass 通过受信 profile 关联运行时定制；用户创建 PVC 后由 SSF 自动准备，沙箱直接引用 PVC，无必需 import。以下为设计要求，当前实现及验收边界见 [22](22-volume-preparation-implementation.md)。

PVC/PV Resolver 是唯一资源解释入口，输出消费视图；Substrate 只作为直接消费执行核心的代码参考。其 Actor 卷 Create/Delete 流程不引入。项目 Volume API 通过原生资源创建、快照、克隆、扩容和回收；消费索引不是第二套生命周期权威。

## 1 三种概念与就绪状态

PV 是描述存储资源的 API 对象，通过 Driver name、volumeHandle 等定位实际后端资源；PVC 是使用需求，绑定的是 PV。CSI publish target 是节点上的目录或设备，并不等同于 PV。PV 元数据中不保存业务数据。[PV 生命周期](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#lifecycle-of-a-volume-and-claim)

| 状态 | 含义 | 能否直接开始用户任务 |
| --- | --- | --- |
| Provisioned | 后端资源存在 | 否 |
| Bound | PVC/PV 绑定完成 | 否 |
| CatalogReady | SSF 已验证身份、权限、能力、管理权和保护策略 | 还需节点准备 |
| VolumeReady〔设计新增〕 | adapter 所需内容已验证，准备占用已释放或接续，持久化 profile/源/FS 身份 | 可以进入运行挂载，仍需 guest 就绪 |
| NodePrepared | 必要 attachment、stage、Binding target publish 完成 | 还需 runtime/guest 接入 |
| GuestReady | VM 内目标路径或设备可用 | 可以，仍遵守声明的数据持久性 |
| Durable | 特定数据版本达到声明的持久化提交点 | 表示数据版本，不替代挂载状态 |

提前创建卷可以确定存储可用区，但不代表选定或 attach 到某台 Worker。已有卷的拓扑通过 PV nodeAffinity、Driver 能力和后端实际约束进入联合调度。

有内容定制的 Immediate 卷可为准备临时 Attach 到合适节点，完成后释放；这不等于固定未来的 Worker。WFFC 保持延迟绑定，第一位沙箱消费者选点后自动准备。`VolumeReady` 属于 SSF 派生状态，不更改 Kubernetes 的 `PVC.status.phase`；完整状态与受保护观察注解见 [21 第 7 节](21-ssf-volume-provisioning-and-runtime-adapter-design.md#7-就绪状态与管理权威)。

## 2 供卷与绑定模式

Immediate 和 WFFC 均纳入首期支持。Immediate 可作为适合提前供卷场景的默认性能优化；需要结合沙箱所在节点、可用区或本地容量建卷/选卷时使用 WFFC。静态/动态供给与绑定模式独立，不能把“支持静态 PV”理解为“只支持预先绑定”。

### 2.1 静态 PV 的提前绑定用法

这种用法由管理员先准备后端资源，再创建引用它的 PV，业务在 VM 创建前申请并绑定 PVC。SSF 将已绑定且通过保护登记的卷开放给节点使用。静态 PV 也支持第 2.3 节的 WFFC 延迟绑定，不要求所有静态卷提前绑定；缺少现存资源的 PV 元数据不视为后端已准备好。

以下是字段关系示意，Driver、handle、容量、访问能力和拓扑须替换为真实值，不是可直接部署的通用模板。

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: sandbox-data-pv
  labels:
    storage.ssf.example/consumer-owner: ssf
spec:
  capacity:
    storage: 100Gi
  accessModes: [ReadWriteOnce]
  volumeMode: Block
  persistentVolumeReclaimPolicy: Retain
  storageClassName: sandbox-static
  csi:
    driver: block.csi.vendor.example
    volumeHandle: existing-backend-volume-id
  # 若后端有 zone/node 限制，必须按真实拓扑补齐 nodeAffinity。
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: sandbox-data
  namespace: sandbox-storage
spec:
  storageClassName: sandbox-static
  volumeName: sandbox-data-pv
  volumeMode: Block
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Gi
```

`volumeName` 显式选择 PV 也不代表该卷没有拓扑限制；SSF 仍校验 PV/后端约束，不能因预绑定绕过节点可访问性检查。

### 2.2 提前创建 Immediate PVC

```text
提交 PVC
 -> external-provisioner 调用 CreateVolume
 -> 后端创建逻辑卷并返回 handle
 -> external-provisioner 创建 PV
 -> PV Controller 完成声明绑定
 -> SSF Catalog 同步并完成保护登记
 -> CatalogReady
```

示意 StorageClass：

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: sandbox-immediate
provisioner: block.csi.vendor.example
volumeBindingMode: Immediate
reclaimPolicy: Retain
allowVolumeExpansion: false
parameters:
  # 只填写所选 Driver 明确支持的参数。
```

provisioner 必须匹配实际 Driver identity，示例域名不是已经实现的驱动。Filesystem 或 Block 在 PVC 中按能力声明；选择 Block 时，后续 guest 文件系统初始化由受控流程负责，不能在恢复时自动格式化已有数据。

静态导入或 Immediate 均须在沙箱创建前完成供卷与绑定，才属于提前供卷。与 VM 请求同时新建 Immediate PVC 时，供卷延迟仍计入启动时间。[供卷 sidecar](https://kubernetes-csi.github.io/docs/external-provisioner.html)

### 2.3 WFFC 延迟绑定与节点相关供卷

标准 WFFC 等待首个消费者的调度结果，再完成供卷或选卷绑定。独立 VM 的需求不会自动进入 kube-scheduler，框架以 **DelayedBinding Coordinator（公共延迟绑定协调器）** 承接这部分消费者职责，正常路径仍不创建 Holder Pod。[K8S 绑定模式](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode)

由 StorageClass 的 `volumeBindingMode: WaitForFirstConsumer` 选择该模式；框架遵守每个 class 的声明，不能因平台默认 Immediate 而覆盖它。

适用场景包括本地 NVMe、节点专属存储池，以及必须与计算节点处于同一可用区的云盘。WFFC 对“节点相关”的约束取决于实际后端：本地盘可绑定到单台宿主，区级云盘通常约束到 zone，不代表固定某台 Worker。仅创建 PVC 时不选点、不 attach；首个沙箱请求消费时才启动联合选点。

```text
创建受管 WFFC PVC -> 登记 WaitingConsumer（动态场景通常尚无对应 PV）
首个沙箱请求
 -> PlanVolumes：计算需求 ∩ 已绑定卷拓扑 ∩ 待供卷/静态候选约束
 -> ReservePlacement：预留计算资源与 PVC/PV 需求，记录 attempt/generation
 -> EnsureVolumesBound：动态供卷桥或静态绑定桥
 -> 等待 PV/PVC Bound，验证实际拓扑，完成 CatalogReady
 -> 校验计算预留仍有效，AcquireBindings、EnsureAttachment、BuildNodePlan
 -> 原有创建 RPC 下发计划，Node Agent、Runtime Adapter 完成 guest ready
```

项目 PVC facade/登记 API 返回稳定 volumeUID、PVC UID 和 WaitingConsumer，不阻塞到 Bound，也不返回可挂载 endpoint。需求可以提前提交；实例启动前必须完成本次 EnsureVolumesBound。既有 WFFC 卷已 Bound 时遵守已有拓扑，不为每次消费重新建卷或重新选绑定。

若申请卷时已具备目标沙箱的有效 placement lease，可同时登记该消费者引用并直接推进 EnsureVolumesBound，复用既有选点；没有消费者时保持 WaitingConsumer。节点关联通过已授权的 placement operation 传递，不能让普通用户直接填写任意 selected-node。卷如何接入正在运行的 VM 仍取决于 runtime 的热插拔能力，不因支持 WFFC 自动获得该能力。

#### 动态供卷桥

1. 校验受管 Pending PVC、StorageClass、唯一 provisioner、节点存活与 Driver 能力，先持久化 placement operation，再通过 UID/resourceVersion 条件更新写入 `volume.kubernetes.io/selected-node`。值为真实 K8S Node 名，与 SSF nodeUID、CSI node_id 分别映射。
2. external-provisioner 继续是唯一 CreateVolume 调用方。它按选点与 StorageClass 生成 CSI accessibility requirements、创建后端卷及 PV，PV Controller 完成声明绑定；SSF 不并发直接 CreateVolume。
3. 观察绑定结果，重新校验容量、访问模式、volumeMode、身份与实际 PV nodeAffinity。节点严格相关的 profile 使用经认证的 strict-topology 配置或等价约束；不能把 CSI Preferred 当作硬性保证。结果不满足预留节点时不启动 VM，先在已有卷约束内重新规划。

桥接依据是上游选点协议：在 external-provisioner `v5.2.0` 使用的 [provisioner 库 `v11.0.1`](https://github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/blob/v11.0.1/controller/controller.go) 中，WFFC 供卷检查 selected-node；[上游拓扑实现](https://github.com/kubernetes-csi/external-provisioner/blob/v5.2.0/pkg/controller/topology.go) 再由 Node/CSINode 生成约束。本设计据此扩展独立沙箱消费者，属于新增集成，不是上游对 VM 调度的现成功能；部署须固定并验证组件版本。

若 Driver 使用拓扑供卷，Node、CSINode driver entry/topologyKeys 与 Node labels 必须预先就绪。K8S 模式优先复用 node-driver-registrar/kubelet 的发现注册；这不让 kubelet 执行沙箱卷的挂载。仅有 SSF Registry 不能替代 native provisioner 的拓扑输入。没有原生注册时必须另行实现并认证注册桥及字段管理权，不能宣称原生 WFFC 已可用。[供卷拓扑与 strict-topology 配置](https://github.com/kubernetes-csi/external-provisioner/blob/v5.2.0/README.md#topology-support)

#### 静态绑定桥

已有 PV 不需要 CreateVolume。协调器按请求容量、class、volumeMode、访问模式、selector 及 PV nodeAffinity，与计算候选节点联合匹配。先在 SSF 账本预留候选，再以 PV UID/resourceVersion 条件更新预绑定至具体 PVC UID，由 PV Controller 完成双向绑定并验证结果；不得手填 Bound status。不同 PVC 抢同一 PV 时由 K8S 条件更新裁决，失败方重新规划，不能只依赖本地缓存。实现参考 [kube-scheduler VolumeBinding 的预留和绑定顺序](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/scheduler/framework/plugins/volumebinding/binder.go)，独立实现沙箱契约，不整体嵌入调度器。

示例：已有 `worker-a` 和 `worker-b` 的本地 PV，WFFC PVC 先不绑定；CPU/GPU 需求使沙箱选中 `worker-b` 后，才匹配并绑定 `worker-b` 的 PV。本地 PV 的 nodeAffinity 保留，后续恢复不能无条件移到 `worker-a`。

#### 预留、失败与重选

计算资源由项目调度器返回可续期的 placement lease，SSF 保存其引用并锁定 PVC UID、候选 PV UID、选点 attempt 和管理权版本；两套服务使用可恢复步骤协调，不宣称跨 K8S、调度器和 SSF 数据库存在一个原子事务。容量观测来自已认证的 CSIStorageCapacity/GetCapacity 或后端索引，只用于过滤与评分，不是容量保证，实际建卷仍可能失败。[存储容量跟踪](https://kubernetes.io/docs/concepts/storage/storage-capacity/)

同一 Pending PVC 同时只允许一个选点/供卷 attempt。后到的沙箱请求等待并复用其结果，再按实际拓扑和访问策略准入；不能各自覆盖 selected-node，或为一个 claim 创建多个后端卷。

| 情况 | 处理 |
| --- | --- |
| 确定性容量不足、provisioner 已清除 selected-node | 确认该次供卷没有未知结果后，更新 attempt，释放/更换计算预留并重新联合选点 |
| CreateVolume 超时或结果未知 | 保留 operation 与 PVC 身份，等待唯一 provisioner 恢复；不盲目改节点或换请求建第二个卷 |
| 已 Bound，但原计算预留失效 | 依据已生成卷的实际拓扑重新选点；不假设删除 annotation 能移动卷 |
| 多卷部分成功 | 成功卷保留受管记录与保护，将其拓扑带入下一轮；无共同可行节点时返回明确冲突并按策略保留或清理新建资源 |
| 客户端取消或 Controller 重启 | operation 继续核对已发生副作用；确认安全后释放预留，节点本地卷不能只按 lease 到期直接删除 |

Guard 除拒绝原生 Pod 抢用外，还验证 SSF 选点/预绑定许可，允许唯一 native provisioner 与 PV Controller 的必要写入及合法重选；这些允许操作按账号、UID、字段和阶段限定，不能被通用“禁止绑定修改”规则阻断。原生控制器的重试、API/watch 等待和真实建卷耗时全部纳入 WFFC 首次创建性能。

## 3 按操作划分唯一权威

| 操作 | 独立沙箱模式 | 原生 Pod 模式 |
| --- | --- | --- |
| 声明与容量请求 | 原生 PVC/PV/StorageClass；项目 API 仅 facade | 原生 PVC/PV/StorageClass |
| 创建后端卷 | 唯一 external-provisioner；静态 PV 对应预先存在资源 | 同左 |
| PVC/PV 绑定 | 原生 PV Controller | 原生 PV Controller |
| 运行时卷内容准备〔SSF 扩展〕 | 公共准备协调 + adapter 第一部分；自动满足运行时 profile | 沿用 Pod/Driver 的原有卷语义 |
| WFFC 消费者选点 | 项目调度器与公共 DelayedBinding 桥，唯一 provisioner 供卷 | kube-scheduler VolumeBinding |
| 使用授权/writer 与 node pin | 公共全局 Use Ledger | 原生机制及后端约束 |
| ControllerPublish/Unpublish | 默认公共消费协调模块直接调用；不同时交 external-attacher | external-attacher |
| NodeStage/Publish 及逆操作 | SSF Node Agent | kubelet |
| guest 接入 | 项目 Runtime Adapter | 容器 runtime；VM runtime 仍需适配 |
| Snapshot/DeleteSnapshot | 原生 Snapshot/Content + 官方组件 | 同左 |
| Clone/Create from snapshot | 原生新 PVC dataSource + provisioner | 同左 |
| ControllerExpand | 原生 PVC 更新 + resizer | 同左 |
| NodeExpand/guest expand | SSF Node Agent + Runtime Adapter | kubelet + 容器环境 |
| 回收后端卷 | SSF 排空授权后，原生 reclaim/provisioner 执行 | 原生 reclaim/provisioner |

固定 provisioningOwner=Kubernetes、attachmentOwner=SSF Direct、nodePublishOwner=SSF Node、reclaimOwner=Kubernetes。消费端和 Gateway 没有 Create/DeleteVolume、Snapshot 或 ControllerExpand 权限。VA 兼容模式若启用则 attachmentOwner 切换 external-attacher，SSF 不再直接调用相同操作。

## 4 PVC/PV Resolver 与消费账本

### 4.1 解析输入

| 原生字段/来源 | 消费侧处理 |
| --- | --- |
| PVC namespace/name/UID、spec.volumeName | 首次解析固定 UID；验证真实双向绑定，拒绝 Terminating/漂移对象 |
| PV UID、claimRef UID、spec.csi.driver/volumeHandle | 固定 Driver 实例和真实后端身份，不把 PVC/PV 名称或 UID 当 handle |
| PV.spec.csi.volumeAttributes | 原样按用途传递 volume_context；不是重新使用 StorageClass.parameters 代替 |
| PV.spec.csi.fsType、PV.mountOptions | 构造 Mount capability；按 Driver 认证配置和权限策略校验 |
| PVC/PV volumeMode、accessModes、消费 readonly | 映射 CSI capability与访问策略，区分 FileTree/Block；不能固定 SingleNodeWriter/可写 |
| PV CSI controllerPublishSecretRef/nodeStageSecretRef/nodePublishSecretRef 等 | 按 RPC 解析有权限的凭证；只下发引用，不在业务创建请求/日志中传播明文 |
| PV nodeAffinity、SC 拓扑、Node/CSINode、Driver 实际能力 | 联合计算选点，保留 bound 后实际约束 |
| resourceVersion、消费 generation、保护策略版本 | 构造不可混用的新旧计划；资源变化后重新校验，不把全局 RV 当恒定序号 |

CSI access mode 的映射还需后端与沙箱语义确认；RWO 不等于单沙箱写者保护。Driver 依赖 Pod UID/Token/FSGroup 等额外行为时需认证该部署 profile，无法提供的输入不能伪造。

### 4.2 账本与 CatalogReady

Use Ledger 保存规范身份映射、观测资源版本、operation、计算预留、writer、Attachment/Binding/pin 和排空许可；新增准备记录保存 profile/source revision、FS 身份及临时准备占用。Kubernetes 是卷资源管理权威，账本是准备与独立消费者的使用权威；禁止账本自行决定另建卷、更改容量或直接回收后端。volumeUID 与 PVC UID 绑定，PVC 同名重建得到新 UID；Released Retain PV 的重新接入必须显式审批，不自动转给新 claim。

先启用受管 namespace/PV 范围的 Guard 和 RBAC、登记 Importing 屏障，关闭新原生消费者；再核验真实绑定/后端身份、排空已有 Pod/VA/target、完成保护，最后提交 CatalogReady。保护先于存量检查生效，避免检查后新增 Pod。Pending WFFC 先登记需求与保护，Bound 前没有最终 handle，不准入 Node。

上述 Importing 是框架内部的接管屏障，不是要求用户调用 `/volumes import`。新受管 PVC 在准入时按 SC profile 固定保护，公共 watcher 自动登记；直接创建沙箱和后台准备汇入同一 PVC UID 记录。存量原生消费者仍需真实排空后交接，不能自动夺取设备。

Resolver 使用 informer/watch 缓存跟踪管理状态，常用已绑定卷不在每次 Node Prepare Get PVC。缓存不独立批准 writer：每次使用仍由共享事务账本批准，且须满足资源观测新鲜度/保护有效性；API/保护状态不可确认时关闭相应新准入，已有 I/O 继续。

## 5 删除与回收：SSF 排空，Kubernetes 删除后端

2026-10-21 新受管 PVC 的目标交互补充：CREATE 时安装受保护 finalizer，DELETE 先记录 DeletionTimestamp，由公共 watcher 持久排空，引用清零且许可生效后移除 finalizer。原 Guard 的许可校验继续保护实际释放及 PV 回收；不能既拒绝 DELETE 又假设后台能观察到这次请求。详见 [21 第 11 节](21-ssf-volume-provisioning-and-runtime-adapter-design.md#11-生命周期与部署补充)。下文现有许可流程是实现基线，新增自动排空实现和未完成恢复边界见 [22](22-volume-preparation-implementation.md)。

原生 PVC protection 判断 Pod 引用，无法感知独立 VM 使用。[原生保护](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storage-object-in-use-protection) 因此直接消费要补保护，但不把后端删除权搬到 SSF。

### 5.1 保护组合

生产准入要求：专用受管 namespace/PV 范围与 RBAC；新建/导入默认 Retain；持久 Use Ledger 和 UID 映射；独立 Admission Guard。必要时可增加 SSF finalizer 辅助流程，但不能只加 finalizer 或只看引用数。2026-10-19 实现使用 Guard、Retain、权限边界及单向 Draining 屏障，不增加自有 finalizer，不修改原生 finalizer。

Guard 验证 PVC/PV 删除、绑定/handle/reclaimPolicy/保护字段等破坏性变更及回收许可，拒绝原生 Pod 引用 SSF-owned/Importing PVC。合法原生首次供卷/绑定、selected-node 重选和生命周期写入按账号、UID、字段、阶段许可，不能被保护规则阻断。保护基于受控资源范围及不可复用 UID，不只依赖可移除标签；PV 别名冲突也需登记检查。

### 5.2 删除具体步骤

1. 项目 DeleteVolume facade 在账本事务中设置 Draining，拒绝新 Acquire；记录本次原生资源 UID/版本和 Retain/Delete 意图。
2. 停止或排空消费者，依次确认 runtime、Node target、stage、attachment、热 pin 和未知结果；活跃/不可确认状态不发回收许可。独立快照 pin 按其真实依赖处理，不能删掉仍依赖源数据的版本。
3. 对 Retain，许可删除 PVC/解绑时保留 PV/后端及稳定身份记录；状态为 Retained，不虚报后端 Deleted。重新使用通过显式接入/绑定流程。
4. 对 Delete，排空后授权将该 PV 的 reclaimPolicy 从 Retain 改为 Delete，并确认更新成功，再授权删除 PVC；唯一原生 provisioner/reclaim 流程执行 CSI DeleteVolume及 PV 清理。已有 Released Retain PV 的显式销毁也进入受控原生回收操作，固定版本验证其回收行为，不能直接 RPC 兜底。
5. SSF 自有 finalizer在已完成消费排空、许可持久且可恢复时移除，不等待它所阻塞的原生删除完成，避免循环等待；原生 finalizer由其所属组件维护。
6. 观察真实 Kubernetes 对象及原生回收结果后更新操作状态并保留 tombstone。API 响应丢失、provisioner故障或 DeleteVolume 未知结果保持 Draining/ReclaimPending，由同一原生身份恢复，不另行直接 Delete。

Retain 是默认保护，显式授权的原生 Delete 才回收数据；仅删除一个 Retain PVC不等于后端删除。原生回收方虽不逐次读取消费账本，但只有通过受控的策略切换/对象删除才能进入回收；因此 Guard、RBAC、默认 Retain 和排空事务缺一不可。[原生回收](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#reclaiming)

### 5.3 准入边界与失效处理

Guard 默认独立 Deployment，读取同一使用权威并验证许可；不在 admission 中执行 Drain、Attach、删除或写入账本。受管变更使用 failurePolicy=Fail、sideEffects=None，dry-run 无副作用。许可绑定操作、资源 UID、版本/关键字段、有效期和排空 generation；新 Acquire 被持久屏障禁止，K8S 写入失败不提前解除。

通过资源范围/RBAC约束普通租户和自动化，避免移除标签、重建同名对象绕过。控制面管理员绕过准入或存储管理员外部删除超出框架控制；检测漂移后阻止新消费并恢复核验。原生回收和快照控制器的后台行为需在指定版本/Driver组合下验收。[Webhook 约束](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/)

## 6 attachment 和 Node 注册

### 6.1 默认 Direct 模式

嵌入式消费协调模块使用从对应节点 Driver `NodeGetInfo` 获得的 node_id，调用 ControllerPublish；记录结果与 publish_context 后再允许 Node publish。Controller 仅提供 UDS 时，可在同 Pod 增加受控 CSI Controller Gateway，将 SSF mTLS 请求转为本地 CSI RPC。

Gateway 不是任意 CSI 代理：验证 operation、volume、node、epoch 和允许 RPC；不得让租户指定任意 backend handle。公共消费协调模块与 Gateway 的重试共享同一 operation，不因网关断连而重复创建相反操作。

Gateway允许身份/探测/能力及适用的ValidateVolumeCapabilities只读查询，有副作用RPC固定只允许已授权的ControllerPublish/Unpublish；默认拒绝 Create/DeleteVolume、Snapshot/Clone和 ControllerExpand。原生 sidecar通过其原有 UDS 执行管理操作，不受消费 Gateway转发。BuildNodePlan 在 Attach成功后下发当前 publish_context，Node 不自行 Attach或复用旧节点的上下文。

SSF 管理的 attachment 默认不创建 VolumeAttachment，同一对象也不交给 external-attacher 协调。后台按 Driver/backend 能力对账，异常仍由 SSF 唯一权威恢复。SkipAttach Driver 省略远端 attach，但保留 node pin。

### 6.2 可选 VA Bridge

Bridge 需要明确创建与管理 VolumeAttachment，补齐匹配的 CSINode/Driver node ID、RBAC、status/finalizer 协议及重启恢复，并验证 K8S 原生控制器不会删除它。CSI Controller 调用只由 external-attacher 执行，SSF 不能再直接执行同一操作。

这是兼容模式，不是无 Pod 自动拥有的能力。API 写入、watch、sidecar 队列与等待都计入首次 attachment 时间。[external-attacher](https://kubernetes-csi.github.io/docs/external-attacher.html)

### 6.3 Driver 进程复用

优先验证专用 Driver 部署 profile。不重复安装监听相同 provisioner identity 的控制器；使用已有 external-provisioner 时继续由它唯一供卷。新增实例时先确认 Driver 是否支持独立 identity、backend scope 和 leader election 范围，不能只改 StorageClass 名字就宣称隔离成功。

复用现有 CSI Node socket 不等于获得该卷管理权。Direct 模式必须确认没有 Pod、遗留 VA、kubelet target 和其他别名指向同一资源。CSI Controller/Node 进程还可能有 K8S API、云身份或固定目录依赖，按版本认证后才能接入。

## 7 原生快照、克隆与扩容

Snapshot/Content 与 snapshot-controller、snapshotter继续管理快照；SSF 只协调冻结/内存/多卷一致性并保存原生 UID/handle/pin。创建快照不保证与内存处于同一时刻，readyToUse也不能替代应用一致性协议。快照默认受控 Retain，删除已被 checkpoint引用的 Content/Snapshot需排空许可。

Clone/restore 创建带 dataSource 的新 PVC，经唯一 provisioner供卷；等待 Bound与保护后按同一直接消费路径接入。新卷是新 UID，失败子卷经许可删除原生资源，不直接 CSI Delete。

首期关闭任意用户扩容。正式启用时更新 PVC，由唯一 native resizer执行 ControllerExpand，Node Agent补 NodeExpand，Runtime Adapter完成设备容量刷新/guest扩容。实现必须适配原生状态和已发布路径需求，不伪造容量完成状态，不假定 sidecar自动扩大 VM内文件系统。[快照/克隆管理](https://kubernetes-csi.github.io/docs/volume-snapshot-restore-feature.html)、[扩容](https://kubernetes-csi.github.io/docs/volume-expansion.html)

原生生命周期不支持的 AgentENV legacy版本协同仍明确标为迁移兼容能力；不能把该兼容路径宣传为已完成的通用原生快照接口。

## 8 与普通 Pod 共存和迁移

同一集群可同时运行原生 Pod 卷和 SSF 卷，按资源 identity 与操作权威隔离。共享同一后端文件系统不等于共享同一个未协调的卷生命周期；别名目录与共享数据集需要显式登记。

从 Pod 转移：关闭新消费者、排空 Pod、确认 kubelet publish/stage 和原 VA 已安全释放、检查实际后端状态，再由 SSF 安装保护并导入。反向转移先停止 VM、发布待提交数据、清空 SSF Binding 与 node pin，再交给 native 路径。

未来若沙箱真的变成 Pod 消费者，可以将 nodePublishOwner 切换为 kubelet，但必须通过上述交接，不能在运行中用两个调用方同时管理同一个 handle。Holder Pod Bridge 仅为必须使用 Pod 身份的 Driver 提供兼容，正常创建不依赖它。

## 9 部署范围

本版要求 Kubernetes API作为卷管理面，不设计无 Kubernetes的 Direct供卷入口。沙箱进程可以不由 Kubernetes逐实例管理，也可以作为认证宿主服务运行；仍引用原生 PVC/PV并使用公共消费核心。

需要拓扑供卷的宿主节点必须具备原生 Node/CSINode/标签映射；无此注册的外部节点不自动获得 WFFC能力，需先完成认证注册桥。平台 Pod的基础卷继续由 kubelet管理，沙箱使用独立 target和调用权威；两者是消费隔离，不是两套卷生命周期。
