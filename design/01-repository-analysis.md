# AgentENV 与 CubeSandbox 存储现状分析

统一框架具备落地基础，但两个项目的抽象边界不同。CubeSandbox 已有面向文件目录的可插拔卷框架；AgentENV 的卷与分层块设备、快照发布及沙箱 fork 共同实现。可以复用两者已有的底层能力，同时把通用协调逻辑逐步移到独立框架。

## 1 分析基线

| 项目 | tag | 本地 HEAD |
| --- | --- | --- |
| CubeSandbox | `v0.7.3` | `4411c96ba8658a959de34693f54f4918db2a432f` |
| AgentENV | `v0.2.3` | `6cccaa7842bd5be2051111d4f74d9e37aa721244` |

下文为这两个版本的源码事实及其设计推论。E2B 只作为用户提出的同类架构背景，未对 E2B 源码做独立审计。

## 2 部署在 K8S 与由 K8S 创建每个沙箱是不同选择

AgentENV 的部署文档包含 gateway、scheduler 和 privileged runtime DaemonSet。CubeSandbox 的 Helm 模板也包含节点 DaemonSet。这两类部署都允许常驻节点进程继续承担沙箱创建，无须每个沙箱映射为一个 Kubernetes Pod。

证据：[AgentENV K8S 部署说明](../AgentENV/docs/src/deployment/kubernetes.md)、[AgentENV DaemonSet](../AgentENV/deploy/k8s/base/agentenv-daemonset.yaml)、[CubeSandbox 节点 DaemonSet](../CubeSandbox/deploy/kubernetes/chart/templates/node-daemonset.yaml)。

因此可以让 K8S 负责安装、升级、节点发现及基础设施治理，让沙箱调度器继续直接调用节点。CSI 卷管理也可以采用相同分层。创建性能是否改善，需要比较实际控制链路和预热状态，不能仅凭是否使用 K8S 得出数值结论。

## 3 CubeSandbox 的卷路径

### 3.1 已有插件模型

CubeMaster 的 `ControllerPlugin` 提供 `Create` 和 `Destroy`。Cubelet 的 `VolumePlugin` 提供 `Init`、`Attach`、`Detach`、`Close`，支持 builtin、binary 和常驻 RPC 插件。RPC 协议分为 `VolumeControllerService` 和 `VolumePluginService`。

这里的 `Attach` 负责宿主机挂载和返回 `hostPath`，职责更接近 CSI 的 Node 阶段；不能按名字直接映射为云盘挂到节点的 `ControllerPublishVolume`。

证据：[ControllerPlugin](../CubeSandbox/CubeMaster/pkg/volume/plugin/plugin.go) 第 171 行、[VolumePlugin](../CubeSandbox/Cubelet/plugins/volume/interface.go) 第 30 行、[RPC proto](../CubeSandbox/pkgs/proto/services/volumeplugin/v1/volumeplugin.proto)。

### 3.2 创建与使用

```text
创建卷
SDK / CubeAPI
  -> CubeMaster volume API
  -> 预留唯一数据库记录
  -> ControllerPlugin.Create
  -> 保存 driver、token、private_data

创建沙箱
CubeMaster 解析卷并下发 driver 与 private_data
  -> Cubelet storage.attachPluginVolume
  -> volume.Manager.Attach
  -> 存储插件返回宿主目录
  -> bind 到沙箱专属共享目录，按需 remount readonly
  -> 原有 virtiofs 路径接入 VM
```

`private_data` 最大 1024 字节，创建后保存到卷记录，下发给 Node Attach；Attach 返回的 metadata 保存到 `StorageInfo`，Detach 原样使用。该机制已经减少了节点再次向控制面查询插件上下文的需求。

证据：[volume API](../CubeSandbox/CubeMaster/pkg/service/httpservice/cube/volume.go) 第 98、128 行、[pluginvolume](../CubeSandbox/Cubelet/storage/pluginvolume.go) 第 116、204、241 行、[插件指南](../CubeSandbox/docs/zh/guide/volume-plugin.md)。

### 3.3 引用与清理

`volume.Manager` 对同一 volume 加锁串行 Attach/Detach。节点引用存储以 namespace、volumeID 和 sandbox holder 记录持有关系，并持久化到 bbolt。Attach 传入挂载前的引用数，Detach 传入卸载后的引用数，失败时回滚相应引用变更。

节点的首次使用和最后释放通过响应事件上报 CubeMaster；Master 的 `t_cube_volume.refcount` 表示使用此卷的节点数。删除 API 检查该值是否为零。

证据：[Manager](../CubeSandbox/Cubelet/plugins/volume/manager.go) 第 121、168 行、[节点 refcount](../CubeSandbox/Cubelet/plugins/volume/refcount/store.go)、[Master refcount](../CubeSandbox/CubeMaster/pkg/volume/refcount/refcount.go)、[删除 API](../CubeSandbox/CubeMaster/pkg/service/httpservice/cube/volume.go) 第 266 行。

**设计推论：** 新框架可以复用首次挂载、末次卸载和 holder 去重思路，但应以持久化 attachment 集合及删除屏障决定安全性。响应事件汇总值适合展示与加速判断，不足以独立处理节点失联、响应丢失和删除并发。此处是统一框架的增强要求，不代表已对现有实现完成故障复现。

### 3.4 文件与快照语义

Plugin Volume 的 readonly 是一次挂载的属性，同一卷可以同时有读者和写者。读者可能看到其他写者的修改，因此它不是不可变数据版本。

外部卷的 sandbox snapshot 保存卷 ID、路径和只读属性，不复制卷内容；恢复时查询当前卷记录再 Attach。原地 rollback 保留外部挂载。Plugin Volume 能否跨节点使用取决于目标节点 driver 与后端；raw host mount 则绑定源节点。

该版本的指南还明确说明，调度器当前不检查 Volume portability、topology、multi-attach 或目标节点 driver。因此统一框架的 PlanVolumes 需要与 CubeMaster 的实际 placement 衔接，提前过滤不可行节点，不能只在节点挂载失败后报错。

证据：[插件指南](../CubeSandbox/docs/zh/guide/volume-plugin.md) 第 527、549 行、[Host Mount 说明](../CubeSandbox/docs/zh/guide/persistent-storage.md)。

另外，CubeSandbox 包含 [cubecow](../CubeSandbox/cubecow/README.md) 和 [CubeS3lvol](../CubeSandbox/CubeS3lvol/README.md) 等块存储能力。它们承担的根盘、分层盘或快照职责不能与用户 Plugin Volume 混为一谈，首期继续由原运行时管理。

### 3.5 与 CSI 的关系

项目已借鉴 CSI 的管理面与节点面分工，但 `volumeplugin.proto` 不是标准 `csi.v1`。`CubeShim/protoc/protos/csi.proto` 是源自 Kata 的卷统计消息，不包含完整的 CSI Identity、Controller、Node 服务，不能据此判断已经支持标准 CSI driver。

证据：[统计消息定义](../CubeSandbox/CubeShim/protoc/protos/csi.proto)、[来源说明](../CubeSandbox/CubeShim/README.md)。

## 4 AgentENV 的卷路径

### 4.1 卷目录与访问约束

`VolumeManager` 管理卷记录及 repository。卷包含 `backing_layers`、本地派生的 `backing_image_config`、容量、状态和 holder。`exclusive` 允许一个沙箱独占写入；`ro` 允许多个沙箱只读使用。`ready`、`uploading`、`failed` 决定是否可以重新挂载。

证据：[VolumeMode 与 VolumeRecord](../AgentENV/src/volume.rs) 第 37、54 行、[创建及挂载说明](../AgentENV/docs/src/concepts/volumes/create-and-mount.md)。

### 4.2 创建沙箱

```text
volumeMounts
  -> resolve_volume_mounts
  -> 校验路径与重复卷，reserve holder
  -> repository materialize_backing
  -> ExtraDrive::Overlaybd
  -> prepare_extra_drives
  -> UblkDeviceManager 创建 OverlayBD runtime device
  -> Firecracker 配置 virtio-blk drive
  -> guest 内挂载文件系统
```

当前 `ExtraDrive` 只有 Overlaybd 变体。持久卷经由 block drive 接入，与 CubeSandbox 插件返回宿主文件目录的接口不等价。直接把任意 CSI `Mount` 结果交给当前 AgentENV，无法自动进入 Firecracker guest。

证据：[挂载解析](../AgentENV/src/api/impls/volumes.rs) 第 20 行、[ExtraDrive](../AgentENV/src/sandbox/extra_drive.rs) 第 18、432 行、[Firecracker add_drive](../AgentENV/src/sandbox/firecracker/instance.rs) 第 318 行。

### 4.3 快照、fork 与持久化

fork 时每个可写 exclusive 卷生成独立 COW 子卷，ro 卷共享。snapshot 单独记录卷的 layers、容量、路径和 mode；从 snapshot 启动时创建新卷。删除源卷不应使已提交快照失效。

runtime 会协同 guest flush 或 freeze、VM pause、OverlayBD seal/restack，再由 repository 发布层引用。发布前进入 `uploading`，完成后恢复 `ready`，失败后进入 `failed`；不能把“本地块设备已创建”当作“跨节点持久化完成”。

证据：[fork 处理](../AgentENV/src/api/impls/sandbox.rs) 第 191 行、[snapshot/restore 处理](../AgentENV/src/api/impls/sandbox.rs) 第 271、292 行、[运行时快照](../AgentENV/src/sandbox/firecracker/sandbox.rs) 第 421、514、2600 行、[发布状态](../AgentENV/src/volume.rs) 第 714 行、[卷快照说明](../AgentENV/docs/src/concepts/volumes/sandbox-forks-and-snapshots.md)。

### 4.4 repository 与节点标识

repository 提供卷记录、独占预留、只读预留和 backing 发布等接口。OSS 实现包含带版本条件的卷记录更新，POSIX 实现也有其目录存储逻辑；新框架需要明确自己的强一致元数据权威，不能假设所有对象存储都提供相同的 CAS 与租约保证。

K8S DaemonSet 的 `AENV_NODE_ID` 来自 Pod 名。统一框架应另有宿主节点稳定 ID 和节点进程 incarnation，且将它们映射到每个 CSI driver 的 `NodeGetInfo.node_id`；三者不能直接混用。

证据：[repository 接口](../AgentENV/src/snapshot/repository/interfaces.rs) 第 140、285、310、319 行、[OSS reserve](../AgentENV/src/snapshot/repository/backends/oss/repository.rs) 第 952 行、[DaemonSet](../AgentENV/deploy/k8s/base/agentenv-daemonset.yaml) 第 45 行。

## 5 统一模型需要保留的差异

| 维度 | AgentENV v0.2.3 | CubeSandbox v0.7.3 | 统一设计 |
| --- | --- | --- | --- |
| 用户卷产物 | OverlayBD 分层块盘 | 插件产出的宿主目录 | FileTree 与 BlockDevice 两类 endpoint |
| VM 接入 | ublk 后的 virtio-blk | bind 后的 virtiofs | 项目 Runtime Adapter |
| 访问策略 | 卷级 exclusive 或 ro | 挂载级 readonly | 卷的共享策略与挂载只读分开 |
| sandbox fork | 可写卷独立 COW 子卷 | 外部卷通常保持引用 | Clone、Share 或 Reject 显式选择 |
| snapshot | 独立 layers 引用 | 外部卷保存引用 | Capture 与 Reference 显式选择 |
| 数据发布 | 层发布状态是核心约束 | 由外部文件后端决定 | 独立 durability 和版本状态 |
| 卷插件化 | 与内部存储类型结合紧密 | 已有 Controller/Node 私有接口 | CSI 核心与遗留兼容桥 |
| 本地数据 | 派生配置、设备及缓存 | 共享挂载、bind 目录及引用 | Node 状态与 durable catalog 分离 |

**可行性判断：** 文件目录型卷较容易先接入 CubeSandbox；AgentENV 需要额外拆分块设备的提供与消费。后者工作量更大，但能复用现有 ublk 和 OverlayBD，无需首期重写存储引擎。对于其他新项目，能否接入取决于其 runtime adapter 与 driver 产物是否匹配。

## 6 基于 Kubernetes CSI 增强的接入边界

两项目的运行节点都可以部署在 K8S，但独立 VM 不是 kubelet 的 Pod 卷消费者。接入统一框架时，Immediate 或静态预绑定卷可提前准备；节点相关卷支持 WFFC，由沙箱调度和公共延迟绑定协调完成选点、供卷/绑定。两条路径都在卷就绪后由 Node Agent 同步调用 CSI Node，必要的 ControllerPublish 由 SSF Attachment Coordinator 执行。

项目定制部分集中在 Integration 与 Runtime Adapter：Cubelet 继续处理文件 share/guest 就绪，AgentENV 扩展受管块设备消费并保持槽位、COW 和版本发布语义。存储 Driver 不为每种 VM 重复实现 guest 接入。

K8S 供卷方与 SSF 消费方按操作分权，需增加 PVC/PV UID 映射、CatalogReady、删除准入和回收屏障。不能将现有 Pod 引用保护或 legacy refcount 直接视作独立 VM 的完整保护。具体部署和协议见[整体设计](03-framework-design.md)与[K8S 集成](05-kubernetes-integration.md)。
