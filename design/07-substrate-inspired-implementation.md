# Substrate 消费层代码复用与实现设计

本版只使用 Kubernetes 管理卷；Substrate 提供直接消费的代码与设计参考。参考提交 `6edd1fccfc9d8a0c0c21db31cb1125336edce442`，原始实现证据见[源码分析](06-substrate-storage-analysis.md)。本文件描述拟实现的公共模块，不表示已经复制源码或通过实机验证。

## 1 可复制、需适配与不引入

### 1.1 基本可复制的调用骨架

| 源码 | 复用内容 | 调整 |
| --- | --- | --- |
| [csi/client.go](../substrate/internal/volume/csi/client.go) | endpoint解析、gRPC连接、生成客户端与Close | 包路径/依赖解耦；连接配置与生命周期注入 |
| [identity.go](../substrate/internal/volume/csi/identity.go) | Info、Capabilities、Probe薄包装 | 基本保留 |
| [node.go](../substrate/internal/volume/csi/node.go) | Stage/Publish/逆操作和能力薄包装 | 增补NodeGetInfo；扩容启用时增补NodeExpand |
| [controller.go](../substrate/internal/volume/csi/controller.go) | Publish/Unpublish、Controller能力薄包装 | 补齐适用的ValidateVolumeCapabilities只读查询；有副作用消费接口仅Publish/Unpublish，Create/Delete不引入 |
| [client测试](../substrate/internal/volume/csi/client_test.go)、[Plugin测试](../substrate/internal/volume/csi/plugin_test.go) | mock Driver及请求/错误组织方式 | 保留辅助骨架，新增实际能力和引用用例 |

这些代码都是调用方，不是厂商Driver。标准CSI生成代码直接依赖固定版本的官方spec库，不另外复制或修改csi.v1协议。

### 1.2 需要实质修改的模块

| 源码/模块 | 保留的设计 | 必须修改 |
| --- | --- | --- |
| [volume/plugin.go](../substrate/internal/volume/plugin.go) | 控制面/节点面接口分离 | 拆成ControllerConsumer和NodeConsumer，不含供卷/回收；补能力/只读/凭证与身份 |
| [csi/plugin.go](../substrate/internal/volume/csi/plugin.go) | 请求映射、Stage→Publish顺序、context传递 | 动态构造Mount/Block/访问模式、readonly、fsType/flags/Secret；持久holder和安全末次释放 |
| [atelet/volumes.go](../substrate/cmd/atelet/volumes.go) | runtime前准备卷、停止后卸载、按Driver缓存 | 去Actor/Atelet依赖；提取Node Executor，新增多卷补偿、WAL和观测 |
| [Resume workflow](../substrate/cmd/ateapi/internal/controlapi/workflow_resume.go) | ensure-step、Attach后将publish_context下发 | 将EnsureCreated替换为Resolve/EnsureBound；嵌入项目创建入口，不迁入Actor数据库 |
| [控制面卷逻辑](../substrate/cmd/ateapi/internal/controlapi/volumes.go) | 真实handle/context记录、部分结果保存 | 只保留消费相关结构；绑定现有PVC/PV，不使用Actor命名生成后端身份 |
| [Registry](../substrate/cmd/ateapi/internal/controlapi/service.go)、[DriverConfig](../substrate/pkg/api/v1alpha1/csidriverconfig_types.go) | 受控端点发现、名称校验、连接缓存 | 自有配置、role/revision、singleflight、漂移/失效与连接释放；原生CSIDriver对象仍单独保留 |
| [TLS实现](../substrate/internal/volume/csi/plugin.go)、[credbundle](../substrate/internal/credbundle) | 证书/CA动态加载与服务器身份验证 | 使用部署方证书来源，去掉Substrate专属路径与PodIdentity硬依赖 |
| [microVM/csi.go](../substrate/cmd/ateworker-microvm/csi.go) | CSI目录bind到guest共享树 | 项目目录/实例和资源生命周期适配；只用于支持文件transport的runtime |
| [共享树](../substrate/cmd/ateworker-microvm/internal/kata/overlay_linux.go)、[OCI路径转换](../substrate/internal/ocispec/microvm.go) | host资源与guest路径分层、递归bind、share合并 | 去Cloud Hypervisor/Kata/Actor强耦合，按实际VMM/guest适配，增加块卷/纯VM契约 |

### 1.3 不引入的组件

| 不引入 | 替代 |
| --- | --- |
| createActorVolumes/deleteActorVolumes | 原生PVC/PV + provisioner/reclaim |
| externalVolumeTemplate按Actor独立供卷 | 用户PVC引用或稳定volumeUID，卷独立于沙箱 |
| 整个ateapi/atelet二进制与Actor API/Store | 现有项目入口 + 公共协调模块 + Node Agent |
| WorkerPool、Actor调度器及完整控制器 | 项目计算调度 + PlacementLease |
| Substrate完整安装器与证书控制器 | 独立部署清单、现有平台认证 |
| durable-dir/rootfs/内存快照引擎 | 项目现有能力，明确的checkpoint兼容契约 |
| DirectCSI供卷Provider | 不提供；所有正式卷管理都走Kubernetes |
| 默认独立SSF Controller Pod | 公共协调默认嵌入项目控制面，服务式仅替代部署 |

### 1.4 不可照搬的具体假设

当前Plugin固定Mount/SINGLE_NODE_WRITER、readonly=false；Stage路径直接拼VolumeID；Unmount立即Unstage；能力失败/空集合假定全支持；部分Unimplemented会跳过步骤。当前Attach工作流使用Worker Node名称，通用实现应改为Driver真实node_id。这些是源码中的现有假设，不把它们复制为框架契约。

源码位于Go internal目录，不能作为外部稳定SDK直接import。源码迁移需调整包路径、保留版权/许可证，并按项目工具链整理依赖；当前参考仓库声明Go 1.27，不能假定下游现有工具链无需调整。测试复用不等于运行了测试。

## 2 Kubernetes 管理入口与描述解析

项目Create/Delete/Snapshot/Clone/Expand API只做原生资源facade。PVC/PV Resolver是唯一解析入口，Bound前输出VolumeRequirement，Bound后输出ResolvedVolume；不直接Create/DeleteVolume。

| 对象 | 内容 |
| --- | --- |
| VolumeRequirement | cluster/PVC UID、class/需求、Immediate/WFFC、候选拓扑、消费profile；允许无PV/handle |
| ResolvedVolume | volumeUID/generation、PVC/PV UID、Driver实例/backendAuthority/真实handle、Mount/Block、访问模式、fsType/flags、context、SecretRef、拓扑/保护revision |
| UseOperation | request摘要、计算预留/选点attempt、步骤/未决RPC、补偿责任 |
| NodeVolumePlan | 上述消费子集、Binding/epoch、nodeUID/boot、attachment结果及当前publish_context、用途受限凭证引用、签名授权 |
| PreparedEndpoint | 真实目录/设备与身份、Binding/epoch、卷代际、node boot、guest目标与readonly |

PV.csi.volumeAttributes是volume_context来源，不把StorageClass.parameters重新当作已创建卷context。volumeHandle与PVC名/PV名/volumeUID分开。WFFC经原生绑定后再产生最终Node计划，不能伪造handle。

Use Ledger只是消费权威，不是PV/PVC替代品。账本记录不能独立批准建卷、扩容或后端删除。默认全局事务库可由多个项目共用；每个物理资源只有一个规范身份和使用权威。

## 3 请求驱动的可重入工作流

### 3.1 已绑定卷

```text
CreateSandbox(PVCRef/volumeUID, guest intents)
 → ResolveAndValidate / joint placement
 → AcquireUse（writer、node pin、计算lease）
 → EnsureAttachment（必要的ControllerPublish，持久结果）
 → BuildNodePlan
 → 现有Create/Restore RPC
 → Node Prepare：按能力Stage → 每Binding Publish → 观测Prepared
 → Runtime AttachPrepared → GuestReady → CommitReady
```

默认控制面在创建RPC前完成Attach，Node不回调发起Attach；无需Attach的Driver仅建立node pin。已验证的兼容attachment可复用，但publish_context必须属于当前卷/节点/权限与有效attachment。

### 3.2 WFFC前置阶段

在AcquireUse之前执行JointPlan → ReservePlacement → 原生动态selected-node/静态PV桥 → AwaitBoundAndProtect → RevalidateCompute。动态建卷只由external-provisioner完成。多卷部分成功保留真实结果，再在这些约束内重选。完整规则见[K8S集成](05-kubernetes-integration.md)。

### 3.3 持久步骤与未知结果

每步持久intent→执行→outcome，状态包括NotStarted/Running/Succeeded/Unknown/Compensating/ReleasePending。相同request_id、相同摘要复用operation；不同参数拒绝。长RPC不占用数据库事务锁，执行权由条件更新控制。

超时/取消不证明失败或无引用。已发出的RPC结果未知时保留pin和操作身份，禁止相反操作；原生供卷未知结果等待原PVC/provisioner恢复。执行租约过期不撤销旧RPC或旧writer，不提供端到端exactly-once承诺。

正常请求直接执行，后台恢复使用同step handler重新获权推进。多卷失败不Ready；VM已接入但回执丢失先Inspect，不能超时即卸载。删除沙箱只释放使用，不删除PVC。

## 4 Driver 会话与参数

### 4.1 会话

按driverInstance、role、endpoint/config revision缓存连接；预先校验GetPluginInfo、Probe、能力、NodeGetInfo，并合并并发初始化。失败不缓存半成品，新revision验证后才接新操作，旧stage/未决RPC保留原版本记录。

node_id、K8S Node、nodeUID/boot、Pod名分别存储。Driver名相同不能单独证明相同存储集群；backendAuthority/真实node_id漂移关闭相关新准入并恢复核验。socket消失不表示卷无人使用。

### 4.2 参数和能力

| 情况 | 处理 |
| --- | --- |
| 身份/能力查询失败 | 相关新操作失败关闭，不假定全支持 |
| 合法空optional集合 | 只跳过没有声明的可选步骤 |
| 已声明必需操作却Unimplemented | 视为能力/部署不一致，不默默跳过必要隔离步骤 |
| Mount/Block/访问组合不支持 | 拒绝，不转换为另一种数据语义 |
| 新旧stage的fsType/flags/凭证域不兼容 | 拒绝或排空后重建，不换key重复Stage掩盖冲突 |
| 原生依赖Pod UID/token | 认证适配或显式兼容profile；不伪造Pod身份 |

CSI的节点能力集合不能通用推断某Driver是否支持Block；根据认证组合及适用的ValidateVolumeCapabilities校验。readonly在Controller权限、Publish、runtime设备/share和guest逐层落实。[CSI规范](https://github.com/container-storage-interface/spec/blob/v1.12.0/spec.md)

volume_context属于卷，publish_context属于具体attachment，Secret按对应RPC用途获取。敏感信息不进入context、业务SDK、日志或明文账本。Controller连接认证由Gateway/现有端点提供，Node CSI经本机UDS；两者不是一个服务实例。

### 4.3 Gateway白名单

允许身份、Probe、能力及适用的ValidateVolumeCapabilities只读查询；有副作用RPC仅允许已有UseOperation授权的ControllerPublish/Unpublish，校验实际volume/node/epoch和参数摘要。Create/DeleteVolume、Snapshot、ControllerExpand全部拒绝。原生sidecar继续通过本地CSI UDS执行管理；不能经网络代理绕过生命周期保护。

## 5 节点共享、并发与释放

同卷attach/stage/publish/release按规范身份串行，不同卷按Driver/节点限额并行；多个沙箱可共享兼容stage，每个Binding仍独立Publish。Stage路径使用受控的Driver/规范卷身份编码，不直接拼任意后端handle。

| 复用项 | 必须核验 |
| --- | --- |
| Driver连接 | role/配置/认证revision、身份与readiness |
| Attachment | 卷/节点/node_id、参数/权限、有效pin、当前结果 |
| Stage | 同节点boot、卷代际、fs/flags/凭证域、实际mount/device、holder |
| Binding重试结果 | 同实例/mount/request摘要、epoch、target及runtime观测 |

释放顺序：runtime确认停止 → 移除本实例share/drive引用 → NodeUnpublish → 释放holder → 最后holder/热pin后NodeUnstage → 全局无使用后ControllerUnpublish → 释放node pin。引用集合是权威，refcount只是缓存；不能相信调用者传入零引用。

热stage/attachment为显式策略，仍持有WAL/node pin、容量与drain责任。dirty/Uploading资源不进通用空闲池。busy/未知结果保持ReleasePending，不lazy-unmount后宣称已flush或停写。租约到期不等于fencing，没有后端隔离/节点停机等证据不批准新exclusive writer。

## 6 Runtime Adapter

接口为ValidateCapabilities、AttachPrepared、WaitGuestReady、DetachAndConfirm、InspectBindings；checkpoint增加Quiesce/Flush/Freeze/Thaw。PreparedEndpoint是短期节点结果，恢复manifest保存卷/版本身份及槽位，换节点重新Prepare，不保存临时/dev路径作为身份。

文件路径参考Substrate共享树，按runtime支持采用virtiofs；Cube可沿用已有文件接入。AgentENV Firecracker块路径增加ManagedBlock，保留jailer/drive ID/slot/readonly与恢复契约；不能直接copyvirtiofs。纯VM补guest挂载与就绪，不依赖guest内容器。Block在host用受管发布目标；按 [2026-10-21 设计](21-ssf-volume-provisioning-and-runtime-adapter-design.md)，空盘内容初始化属于 adapter 的 VolumePreparation，由公共框架在供卷链自动驱动，RuntimeMount 消费其结果，恢复不格式化。

运行时token必须关联认证实例与实际观测，不接受任意字符串当停止使用证据。创建失败/重复销毁/恢复所有路径都完成相同引用纪律。

## 7 项目接入与部署

2026-10-20 AgentENV 的具体接入见 [16](16-agentenvadapter-native-integration-design.md)：采用 Substrate“原有工作流中插入存储 ensure-step”的边界，控制库嵌入原 Scheduler，原生节点驱动 StorageProvider；不引入独立沙箱 control API/调度器。公共 runtime 契约的能力协商、暂停持有及 checkpoint 引用扩展见该文第 4、8、9 节，仍由本章公共包实现。

| 公共职责 | 项目提供 |
| --- | --- |
| PVC/PV解析、WFFC桥、消费授权、Attach、Node CSI | 公共实现，不在两项目重复编写 |
| API与身份 | project/tenant/runtime/sandbox instance UID、PVC/legacy映射 |
| 计算预留 | Reserve/Renew/Commit/ReleasePlacement，最终启动前确认 |
| 创建RPC | 携带完整受认证NodeVolumePlan，保持request/operation身份 |
| Runtime接入 | 消费FileTree/BlockDevice，VMM/guest ready与停止使用观测 |
| checkpoint | 明确Reference/Capture、Share/Clone、版本pin和恢复槽位 |

协调模块默认嵌入CubeMaster/AgentENV权威创建入口；Go/Rust项目可通过生成SDK及嵌入库/经认证本机模块接入。若跨语言成本要求服务式部署，共享Coordinator为替代，不另设独立账本。默认Node Agent、Guard与条件Gateway具体见[部署清单](08-full-deployment-architecture.md)。

## 8 代码包与依赖

```text
api/                      Create消费契约、NodeVolumePlan、PreparedEndpoint；Go/Rust SDK
core/                     规范身份、能力、使用operation、Binding/holder/pin
kubernetes/facade/        原生PVC/Snapshot/Clone/Expand/回收资源操作
kubernetes/resolver/      PVC/PV解析、版本/保护、informer与原生结果观测
kubernetes/delayedbind/   WFFC动态/静态桥，计算lease与attempt
consume/                  请求驱动步骤、Acquire/Attach/BuildPlan/Drain/恢复
drivers/csi/client/       从Substrate提取的薄客户端，不对租户暴露管理RPC
drivers/csi/session/      身份/能力、revision、长连接
guard/                    独立准入服务，读取共享使用权威
gateway/                  条件部署，仅Attach/Detach→Driver UDS
ledger/                   共享事务、owner/CAS、operation与删除屏障
node/                     UDS、Stage/Publish、WAL、观测、末次释放
runtime/                  Adapter/PlacementLease契约
adapters/cube/            CubeMaster/Cubelet与既有runtime
adapters/agentenv/        控制入口、Rust SDK、ManagedBlock及checkpoint
```

没有sources/direct，也没有自主供卷/回收controller。CSI客户端/执行器不依赖Actor/项目模型；原生对象解释集中在kubernetes包。后端Driver独立部署，legacy facade也必须由原生组件供卷。

## 9 实施与验证

2026-10-19 实施落点为 [SSF](../sandbox-storage/README.md)。公共库与进程按本章分层独立实现，没有引入 Substrate 的 Actor 数据模型；本次不创建 `adapters/cube`、`adapters/agentenv`，改为公共 Runtime 接口、验证适配器与可编译接入示例。实际协议以 SSF 的 protobuf 为准，源码来源说明见 [NOTICE](../sandbox-storage/NOTICE)。详细实施记录与验收边界见 [软件实施记录](09-implementation-log.md)。

先固定PVC身份、消费/回收权威、创建RPC和runtime契约；提取薄客户端并增强Plugin；实现Resolver/Guard/账本、Immediate/WFFC；再接Cube文件路径与AgentENV块路径。支持普通VM时必须到真实guest验证。

新增验收：原生API是唯一管理入口；消费Gateway拒绝管理RPC；删除沙箱不删PVC；Retain/Delete原生回收时序；当前attachment上下文；Stage多消费者；参数/Secret传递；配置重载/Driver重启；取消/迟到结果；同UID与同名新对象；WFFC动态/静态及未知供卷恢复。性能和项目完整语义见[验证计划](04-integration-and-validation.md)，不能以mock通过或host mount成功替代真实guest和故障测试。
