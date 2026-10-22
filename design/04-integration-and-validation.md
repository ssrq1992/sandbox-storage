# 项目接入与验证计划

以 Kubernetes 唯一卷管理入口、Substrate 模式直接消费实施。公共消费协调默认嵌入项目控制面，不强制新增 SSF Controller Pod；首期同时支持 Immediate 提前供卷与 WFFC 消费时供卷/绑定。先打通资源保护、联合选点和延迟绑定，再实现共用 Node Agent 同步调用与 Runtime Adapter。正常路径不经 kubelet/Holder Pod 触发沙箱挂载；兼容桥保留现有能力，逐步迁移设备和版本所有权。

## 1 实施原则

当前软件实施范围更新（2026-10-19）：本轮只实现 SSF 公共框架、SDK、验证适配器及测试环境；本章 CubeSandbox/AgentENV 改造清单作为后续项目接入要求，不在此次修改它们的源码。真实 VM/guest 验收在接入对应 Runtime Adapter 后执行。

按卷 UID 或 StorageClass 灰度，每个卷按 provisioning、attachment、nodePublish、reclaim 四类操作固定唯一权威，并遵守共同删除屏障。未迁移卷继续走原路径；已迁移卷的原有 API 可以保留，但不再直接调用原存储后端。

初期两个项目的 SDK 协议不必统一，框架内部统一使用 Volume、Binding、Endpoint 和 profile。外部兼容入口只做转换，不能根据调用者是哪个 SDK 而隐式改变数据行为。

### K8S 供卷与消费者切换前置条件

选择专用受管资源范围，提前安装 Retain、UID 映射、Guard 与 SSF 权威账本。Immediate/静态预绑定卷可在 VM 请求前 CatalogReady；WFFC 先登记 WaitingConsumer，在消费时通过计算 lease 与延迟绑定协调完成 Bound/CatalogReady，二者均在此后调用 Node Prepare。既有 Pod/VA/target 未排空时禁止转为 SSF Direct。

两个项目都需向 SSF 提供可续期的计算 placement lease，并在最终发起 VM 创建前确认其有效性。由公共协调器完成 selected-node/静态 PV 桥接，不在各项目重复实现一套 WFFC；同一沙箱混用已绑定卷和待绑定卷时联合计算全部约束。

第一轮 PoC 至少验证一个无需 attach 的文件 Driver，以及一个需要真实或可控模拟 ControllerPublish 的 Block Driver。普通 VM 的 VMM 设备配置与 guest readiness 单独测试，不能只验证 host mount。

## 2 CubeSandbox 接入

### 2.1 最小接入范围

| 当前入口 | 迁移方式 | 需要保持的行为 |
| --- | --- | --- |
| CubeMaster ControllerPlugin.Create | Kubernetes facade创建PVC并保存UID映射；Immediate可等待就绪，WFFC返回WaitingConsumer | 不能在 WFFC Create 中等待尚未发生的 VM 选点；不重复建卷 |
| ControllerPlugin.Destroy | 按UID发起Drain；授权后走PVC/PV原生Retain/Delete流程 | 在用卷拒绝删除，删除结果可重试 |
| Master 选点、卷解析和 annotation 注入 | 嵌入公共协调；Plan/Reserve/EnsureBound后Acquire、Attach，创建RPC下发NodeVolumePlan | WFFC 先预留与绑定，最终节点仍匹配；用户 annotation 不绕过授权 |
| Cubelet VolumePlugin.Attach | 同步调用本机 Node Agent.PrepareVolumes，返回受管 FileTree | 原 hostPath 根目录、路径与失败回滚 |
| Cubelet 文件共享路径 | 消费 FileTree 并沿用 bind/virtiofs | guest readonly 和原有 UID/GID 行为 |
| runtime Ready 与销毁 | 增加 Ready 回执、停止使用确认和 Release 调用 | 失败创建回滚、重复销毁幂等 |
| snapshot/restore/clone | 记录 SSF UID、Reference profile 及兼容映射 | 外部卷不随 VM 快照复制或回滚 |

接入点：[ControllerPlugin](../CubeSandbox/CubeMaster/pkg/volume/plugin/plugin.go)、[volume API](../CubeSandbox/CubeMaster/pkg/service/httpservice/cube/volume.go)、[Master 卷上下文](../CubeSandbox/CubeMaster/pkg/service/sandbox/util.go)、[VolumePlugin](../CubeSandbox/Cubelet/plugins/volume/interface.go)、[pluginvolume](../CubeSandbox/Cubelet/storage/pluginvolume.go)。

仅实现一个现有 Attach/Detach 插件还不能完成严格接入：现有协议没有 generation、writer epoch、完整租户身份和 runtime Ready 回执，需要在项目集成层补齐。`namespace` 是 containerd namespace，不能默认当作租户权限证明。

当前 Cube 卷 API 若把 Create 成功理解为后端已存在，项目集成层需新增可查询的 WaitingConsumer/Provisioning 状态或版本化返回契约；不能只改插件内部实现便将 Pending 卷报告为 Ready。创建沙箱时由 Master 完成延迟绑定阶段，Cubelet 仍只消费就绪 endpoint。

### 2.2 两种桥的方向

```text
项目桥
CubeMaster/Cubelet 私有接口 -> SSF 公共接口 -> 任意已验证 CSI driver

遗留后端桥
SSF -> CSI facade -> 现有 Cube Controller/Node 存储插件
```

项目桥使 CubeSandbox 使用统一框架；遗留后端桥使现有 COS/S3/其他插件成为框架可用后端。两者职责不同，可以分开实施。遗留后端 facade 也由 Kubernetes external-provisioner 调用 Create/Delete，不是另一个绕过 PVC/PV 的供卷入口。

遗留后端桥需要实现真实的 CSI facade：Controller 保存 legacy volumeID 与 CSI handle 的映射及 private_data；Node 按每个 CSI target 维护持久 holder，可用 target 的稳定内部 ID 作为 legacy sandboxID，不能要求标准 CSI 请求额外携带某个项目的真实 sandboxID。首期可不声明 Stage 能力，由每次 NodePublish 内部复用 legacy host mount，再建立独立 target bind；NodeUnpublish 只撤销对应 holder，最后一个 holder 才调用后端的末次清理。它需自己保证 refCount 与 metadata 的幂等和崩溃恢复。

这样 legacy 插件仍使用原协议，SSF 面对的是标准 CSI；facade 的实现复杂度必须计入迁移成本，不能简单把四个 hook 改名。

配置可为每个允许的原生 StorageClass 配置 Cube 消费 driver 别名，例如 `ssf-workspace`。Cube 的 driver 名仍按当前规则精确匹配；桥根据受管卷映射路由，不能把用户提交的 socket 当作 driver endpoint。

### 2.3 ID 和私有上下文

创建请求以 `(projectInstance, tenantUID, legacyVolumeID, legacyCreationGeneration)` 映射到不可复用 SSF volumeUID；生成代际并持久保存删除 tombstone，使同名重建与原创建重试可区分。`private_data` 只需保存版本和短的 SSF 引用，不放完整 CSI context、凭证或全部状态，遵守当前 1024 字节上限。

Destroy 只传 volumeID，不携带 private_data，因此桥必须有可持久查询的映射，不能仅把 UID 塞进 Attach 的参数。显式请求上下文或集成入口应传递租户身份，单靠现有 Create(volumeID, name) 签名无法得到可靠租户边界。

### 2.4 引用和清理权威

Cubelet 现有 refcount 可以暂留作兼容统计，但 SSF 的 Binding/Attachment 集合决定实际资源回收。对于迁移卷，原插件不得在 SSF 之外自行按 `refCount=0` 卸载相同后端。

在 Cubelet 的 bind/virtiofs 接入和释放路径增加明确顺序：先确认 VM 与 virtiofs 不再使用，解除本沙箱 bind，再通知 SSF Release。审查创建失败的所有阶段，包括 Attach 已成功但 bind 或 readonly remount 失败；保留 operation 与补偿材料直到清理完成。

### 2.5 原有快照兼容

Cube external profile 保持外部 Reference，FromSnap 按原卷 UID 和固定路径重新接入。既有 snapshot 若仅有 legacy ID，通过版本化 manifest 和持久映射解析；禁止把后来创建的同名新卷误当旧卷。

如果旧 snapshot 和历史记录不足以确定该 ID 的创建代际，迁移时不能自动补出可靠身份。需要明确登记历史映射，或将该 snapshot 标为无法安全解析并拒绝自动恢复；不靠当前同名卷推断。

raw host mount 保留节点亲和，不因为框架支持远端 CSI 就自动获得跨节点能力。Plugin Volume 跨节点恢复仍要求 VM snapshot 自身可恢复且目标节点能够访问该后端。

## 3 AgentENV 接入

2026-10-20 修订：本节的接入边界由 [16：原生插件设计](16-agentenvadapter-native-integration-design.md) 和 [17：重构与验收](17-agentenvadapter-refactoring-and-validation.md) 细化。必须保留原 Gateway/Scheduler/节点部署及原生 API；现有独立 adapter 的 202 API、静态调度、生命周期禁用方案退出目标架构。完整验收包括 autoPause/autoResume、联合快照、Fork 与模板继承，不能以 cold-only 阶段替代。

### 3.1 新卷先接入，旧卷显式交接

第一阶段只为新卷类启用原生PVC/PV与公共直接消费。现有VolumeManager/repository卷继续旧路径，不计为正式接入卷，不给同一旧卷增加一套可独立批准写者的SSF锁。PoC可用标准CSI Block Driver，先验证ManagedBlock和真实guest。

第二阶段将OverlayBD/ublk封装为标准CSI Driver：Controller由原生provisioner管理卷元数据/创建删除，Node持有设备，repository仍保存真实层数据。新API的Create/Delete/Clone/Snapshot逐项转原生资源；未实现标准管理的legacy checkpoint明确标为兼容能力，不宣传为完整原生CSI管理。

上述 legacy 存储 Driver 化属于显式选择的后续迁移，不是新 CSI 卷接入的前置条件。本次保留 legacy VolumeManager，通过 VolumeProviderMux 共存；SSF 外部设备采用并列 PreparedExternalDrives，不要求改写所有 OverlayBD ExtraDrive/Drop。计算 reservation 归原 Scheduler，存储保护归公共账本；snapshot 的内存/磁盘屏障归原 Orchestrator。

迁移旧卷先停止新使用、排空、确认durable版本与设备释放，记录旧权威交接，再建立静态PV/PVC或经认证的原生导入；共享消费账本接管使用授权。旧VolumeManager转为新PVC facade，不能同时保留独立writer批准路径。

### 3.2 接入点和改动

| 当前入口 | 迁移后的职责 | 关键约束 |
| --- | --- | --- |
| `VolumeManager` | 已迁移卷的PVC facade；旧卷保持原路径 | exclusive、ro、Uploading/Failed 准入不变 |
| `resolve_volume_mounts` 与 scheduler | 公共消费协调解析PVC；WFFC先绑定，再Acquire/Attach/BuildNodePlan | 路径校验、数量限制、计算预留与失败恢复 |
| `ExtraDrive::Overlaybd` 与 launch plan | 保留原类型，增加并列 PreparedExternalDrives | 不把块设备路径传给要求 image config 的构造函数 |
| `prepare_extra_drives` | 消费 SSF BlockDevice 或兼容委托结果 | 设备身份、readonly、virtual size、释放责任明确 |
| Firecracker drive 配置 | 按既有槽位消费设备 | jailer 可见性、slot 数量与 snapshot manifest 对齐 |
| `UblkDeviceManager` | 兼容阶段原样保留；后续由存储 driver 调用 | 保留设备引用计数和现有 upper 生命周期 |
| snapshot/fork API | 选择 Capture/Clone 或 ro Share | 子卷补偿与独立 snapshot pin |
| repository backing 发布 | legacy 保持原路径；显式迁移后才交给 driver | 上传未完成时不可跨节点恢复或重新写入 |

接入点：[VolumeManager](../AgentENV/src/volume.rs)、[卷解析](../AgentENV/src/api/impls/volumes.rs)、[ExtraDrive](../AgentENV/src/sandbox/extra_drive.rs)、[sandbox API](../AgentENV/src/api/impls/sandbox.rs)、[Firecracker runtime](../AgentENV/src/sandbox/firecracker/sandbox.rs)、[repository](../AgentENV/src/snapshot/repository/interfaces.rs)。

### 3.3 设备所有权

完整 native driver 模式下，Node driver 创建并 pin ublk 设备，SSF Node 管理 Binding 和发布目标，Runtime Adapter 只消费设备。原 runtime 不得再为同一个卷额外创建一套 ublk 或在自己的 Drop 中提前销毁 driver 所拥有的设备。

兼容阶段如设备仍由原 runtime 创建，driver 只是委托桥，必须明确它没有完整接管节点资源，不能宣称已实现标准 CSI Block driver 的通用设备发布。迁移完成后才对外开放其 CSI Node 服务并执行 conformance。

### 3.4 checkpoint 保持

先保持原 runtime 的 guest flush/freeze、VM pause、seal/restack 顺序，将稳定卷版本与 snapshot pin 接入共同 manifest。之后再将存储侧动作迁到 CheckpointExtension。

fork exclusive 卷必须保持每个孩子独立可写：legacy 生成独立 writable upper，CSI 卷从同一次联合 Capture 的 VolumeSnapshot 恢复独立 PVC；ro 可以共享。必须在原生冻结/暂停屏障内联合捕获内存与磁盘，不能在源 VM 恢复写入后补做 CSI snapshot。不得把 Cube external 的 Share 策略用于 AgentENV 可写卷以获得较低的测量延迟。

### 3.5 文件卷支持

当前 AgentENV block 路径不能消费 CSI Mount 目录。若后续要求 AgentENV 直接使用 NFS/共享文件 driver，需要新增文件 transport：选择支持该 transport 的 VMM，或经安全验证的 guest 网络文件系统路径。它是明确的 runtime 功能建设，不计为“几行 CSI 适配”。

反过来，若希望两项目复用同一个 OverlayBD Block driver，应给 CubeSandbox 增加并验证用户块卷 Runtime Adapter。底层 VMM 有块设备能力，不代表 Plugin Volume 入口已经提供了完整用户块卷语义。

### 3.6 K8S 对象与 reservation 权威

旧卷阶段仍由 AgentENV 原权威批准，未完成交接不进入SSF正式消费。完整迁移后，K8S唯一负责底层供卷/回收，SSF负责使用授权、attachment、publish与排空屏障；repository继续保存层数据。映射 source PVC/PV UID 与 legacy generation，并在旧路径排空后交接。

新接入的 WFFC 卷在 SSF 使用 WaitingConsumer 状态，不将它伪装为已有可读写 backing 的 AgentENV ready 卷。scheduler 选点预留后完成 EnsureVolumesBound，再进入 runtime 的受管块消费路径；既有 Uploading/Failed 等数据发布约束仍然有效。

## 4 新沙箱项目接入清单

2026-10-21 契约补充：每个 adapter 包实现供卷定制准备和 RuntimeMount 两部分。第一部分由公共 PVC/profile 准备协调器自动驱动；第二部分消费已满足运行时规格的 Endpoint。项目 Volume API 是可选 facade，不能要求 Kubernetes 创建的 PVC 先手工初始化或登记。新 SPI 与验收以 [21](21-ssf-volume-provisioning-and-runtime-adapter-design.md) 为准，以下生命周期集成职责继续保留。

新项目提供稳定的 runtime/sandbox instance 身份；已就绪卷调用 AcquireBindings、EnsureAttachment、BuildNodePlan，再随原有创建 RPC 下发完整计划并调用 PrepareVolumes。WFFC 首次消费先通过公共 ReservePlacement/EnsureVolumesBound 完成供卷/绑定。项目提供计算 lease，消费类型化 endpoint，回报 guest ready，并在停止使用后释放。若有 checkpoint，需明确 Freeze、Capture、Thaw 和 manifest 约束。

只有新增存储后端才需要实现 Backend Driver。一个项目可能同时贡献 runtime adapter 和其原有存储的 driver，但不要要求每个项目为 NFS、S3、云盘再各写一次后端。

采用 Substrate 的调用模式时，公共框架承担 CSI client、Driver 会话、工作流、WFFC、attachment、节点引用和恢复；项目按下表接入。节点调用可嵌入现有节点进程或调用独立 Agent，不要求每个项目再开发一套 CSI 消费者。

| 项目必须提供的契约 | 最小完成条件 |
| --- | --- |
| 身份与 API 映射 | 稳定 tenant/project/runtime/sandbox instance UID；请求重试与同名重建可区分；Volume API 保存持久映射 |
| PlacementLease | Reserve/Renew/Commit/Release，携带 nodeUID、boot/选点版本；Commit 转为实际计算占用，过期预留不继续启动 VM |
| 挂载声明 | volumeUID/generation、mountUID、guestPath、readonly、profile；不由项目组装 CSI secrets/context |
| PreparedEndpoint 消费 | 校验 Binding/epoch、节点与卷身份，支持自己声明的 FileTree/BlockDevice；VMM 和 guest 两侧落实访问属性 |
| 生命周期回调 | AttachPrepared、WaitGuestReady、DetachAndConfirm、InspectBindings；失败回调与幂等重试覆盖各启动阶段 |
| checkpoint 适配 | 保存稳定卷/版本和槽位，不保存临时设备路径；明确 Reference/Capture、Share/Clone 及冻结义务 |

Cube 文件卷适配与 AgentENV ManagedBlock 是两条首期执行路径，不强求使用相同 transport。项目需要把原后端开放给其他消费者时，再独立实现 CSI facade/Driver；公共客户端和后端服务端是两个扩展方向。源码与公共化边界见[参考 Substrate 的实现细化](07-substrate-inspired-implementation.md)。

## 5 当前能力覆盖和分期

| 能力 | 统一框架落点 | 首期方式 |
| --- | --- | --- |
| 建卷、删除、导入 | 原生PVC/PV + provisioner/reclaim | 遗留后端需标准Driver/facade，仍经原生管理 |
| WFFC 与节点相关供卷 | 公共 DelayedBinding Coordinator、项目计算 lease | 动态 selected-node 供卷桥与静态 PV 绑定桥均纳入首期 |
| 节点 mount 与共享 | Node 状态机、stage pin | Cube 文件卷及 CSI 文件卷 |
| guest 块盘 | Block endpoint 与 Runtime Adapter | AgentENV OverlayBD/ublk |
| readonly 与独占 | mount 属性、全局 reservation、后端约束 | 验证项目现有语义 |
| 共享文件读写 | 能力准入与文件后端 | 保留 Cube 已支持的后端行为 |
| pause/resume | runtime 生命周期与 Binding | 保持原 profile |
| 快照与 fork | 原生Snapshot/PVC dataSource + runtime一致性 | legacy版本协同显式兼容，固定manifest/pin |
| 层上传和 lazy 读取 | driver/repository | 保留现有引擎，框架不转发数据 |
| 本地 Host Mount | LocalDirectory driver 与拓扑 | 保持节点亲和、默认 Retain |
| rootfs 和内存存储 | 后续专门的 Artifact/Checkpoint 能力域 | 不在首期重写现有引擎 |

统一 rootfs 和内存存储的后续方向是共享后端实例、凭证、durable artifact 引用和 GC pin，而不是把内存恢复伪装成普通文件卷。内存 lazy paging、VMM 设备状态和根盘启动约束需要各自接口，CSI 单独不足以覆盖它们。

## 6 PoC 与验收

### 6.1 建议实施阶段

以下为依赖顺序，非工期承诺。完成前一阶段的正确性门槛后再扩展。

| 阶段 | 交付 | 完成门槛 |
| --- | --- | --- |
| P0 契约 | ResolvedVolume、原生PVC/PV UID、固定owner、profile、PlacementLease、NodeVolumePlan | 唯一K8S管理入口、嵌入协调与两项目边界清楚 |
| P1 公共执行核心 | 参考 Substrate 抽取 CSI client、SessionManager、step runner、Direct attach、Node/WAL/UDS | fake 与实际 Driver 的能力、参数、并发、未知结果、取消恢复及引用测试通过 |
| P2 K8S 管理与双模式绑定 | 原生facade/Resolver、Immediate/WFFC、静态/动态、计算lease、Guard/Retain/原生Delete回收 | 不创建消费者 Pod；原生节点相关供卷、重选、保护及公共执行链通过；WFFC 不延期 |
| P3 Cube 路径 | 项目桥、FileTree/virtiofs、遗留后端 facade | 原卷语义、guest readonly、创建失败清理无回归 |
| P4 AgentENV 路径 | ManagedBlock、OverlayBD/ublk 所有权迁移 | exclusive/ro、fork、snapshot、上传失败与设备生命周期无回归 |
| P5 通用性与性能 | 固定版本原生 CSI Driver、普通 VM adapter、分项基线 | 原生 Driver 不依赖项目字段，端到端性能和故障门槛达标 |
| P6 企业共存 | Pod/VM 管理权交接、drain、升级、可选 VA Bridge | 原生与 SSF 卷互不干扰，失联不产生双写 |

当前 NFS 文件 Driver 不能直接被只支持块路径的 AgentENV 消费。同一个真实 Driver 跨两项目演示需额外完成 Cube 用户块 adapter 或 AgentENV 文件 transport，不将两种不同后端的演示冒充同一 Driver 复用。

### 6.2 性能目标

所有数字是初始工程目标，需在 Linux 实机上验证并根据存储类型调整。

| 指标 | 初始目标 | 测量口径 |
| --- | --- | --- |
| 单个预热 Binding 的节点本地框架增量 | p50 ≤ 2 ms，p99 ≤ 10 ms | 相同后端下新增的 UDS、WAL、锁、调度和校验；driver 与全局 reservation 分项计时，不计入本地预算 |
| 多卷预热创建 | 端到端 p99 增量不超过基线的 10%，或经确认的绝对预算 | 相同卷数、相同语义、同一宿主和数据状态 |
| 冷卷接入 | 给出分项延迟，不设统一毫秒保证 | 后端 create/attach/mount、format、拉取和 guest readiness 全部计时 |
| WFFC 首次消费 | 动态供卷与静态延迟绑定分别报告 SLO | 选点、计算 lease、API/watch、供卷/绑定、Catalog、attach 与 guest ready 全部计时 |
| 框架数据面 | 无中心服务逐 I/O 转发 | 同时报告 IOPS、带宽、延迟、CPU、内存和新增 transport 成本 |
| 全局 reservation | 观测新增 RTT 与冲突率 | 不从基线扣除跨节点正确性所需步骤 |

主基线采用“已供卷且 CatalogReady、尚未固定 Worker”的静态 PV/Immediate PVC，测第一次节点接入。另测“已在该节点使用、attachment/stage 可复用”的节点热状态。只有后者称为预热 Binding，本次 target publish 与 guest 接入仍需完成。不能将缺失挂载的零卷沙箱或降级 Share 语义作为对照。

本地预算达标不代表启动 SLO 达标。最终端到端测量从客户端发起创建到 guest 挂载可用，完整包含全局 reservation、driver、VMM 和 guest 的延迟；分别报告原路径与新路径分布，不把各项 p99 简单相加。

强制分别报告提前供卷后的首次节点接入、同节点重复使用、跨节点重接入、即时 Immediate 供卷、WFFC 首次动态供卷、WFFC 静态延迟绑定六类场景；WFFC 已绑定后的消费归入对应的已有卷场景。不将节点热缓存结果代表所有启动。标准 kubelet/Pod 路径作为可比较参考时保持相同后端、预热和实际就绪口径。

测试矩阵包括并发 1/32/128/512、1/4/运行时允许上限的卷数、同卷与不同卷、FileTree/BlockDevice、只读/独占、首次/重复挂载、本地/跨节点恢复、空/热缓存。若运行时上限小于某矩阵值，以真实上限为准。

每组稳定采样建议至少 10000 次并记录失败样本与超时，不只统计成功请求。比较 p50/p95/p99、吞吐、队列等待、CPU、内存、文件描述符和 mount 数。固定内核、VMM、driver 版本、存储区域、凭证模式、设备容量、文件数和缓存状态，报告差异。

### 6.3 功能和语义用例

| 用例 | 必须验证的结果 |
| --- | --- |
| 同卷并发首次挂载 | 首次 host mount/stage 不重复，Binding 各自独立 |
| Driver 并发初始化 | 一个 role/revision 只建立一个初始化会话，失败不缓存半成品 |
| 能力与参数传递 | Mount/Block、readonly、访问模式、topology、flags 和 secret 用途正确，不将能力查询失败当全支持 |
| 唯一管理入口 | 所有正式卷有真实PVC/PV；Create/Delete/Snapshot/Clone/Expand走原生资源，消费Gateway拒绝这些管理RPC |
| 创建RPC计划 | Attach在创建RPC前完成；Node不回调发起Attach，当前publish_context和真实node_id正确 |
| 沙箱与卷删除分离 | 删除沙箱只释放引用；Retain不删数据，显式Delete经排空、许可、原生回收，SSF不直接DeleteVolume |
| 末次引用释放 | 多个 Binding 逐一释放，只有末次 holder/pin 清除后才 Unstage/detach；热保留仍受 drain/删除保护 |
| endpoint 与恢复 | 重启或换节点后重新 Prepare，拒绝旧 boot/epoch/设备身份，guest 槽位对应原卷 |
| exclusive 竞争 | 同节点和跨节点都只有一个写者获准 |
| readonly | 实际 guest 写入失败，读者可见性符合所声明后端语义 |
| 创建失败 | 每个步骤失败都保留正确 pin 并完成可恢复补偿 |
| 多卷部分失败 | 沙箱不对用户 Ready，不卸载其他沙箱正在用的卷 |
| 删除与新挂载竞争 | 删除屏障与 reservation 互斥，没有 check-then-delete 窗口 |
| AgentENV fork | 孩子写入不污染父卷或兄弟卷，失败孩子无孤儿 writable upper |
| AgentENV 快照 | 源卷删除后快照可恢复，层上传失败时不能读到过期 head |
| Cube Reference 快照 | 外部卷变更不被 rollback，引用原 UID，路径和 readonly 保持 |
| 原生 CSI 接入 | Node Agent 同步执行，无项目私有字段也能完成标准生命周期；按能力跳过 attach/stage |
| 静态 PV 与 Immediate | VM 请求前 CatalogReady，运行时不重复 CreateVolume，不提前固定 Worker |
| WFFC 动态 Pending | 创建卷返回 WaitingConsumer；首个沙箱预留节点后触发唯一 provisioner，Bound/CatalogReady 后再准备节点 |
| WFFC 静态 PV | 已有多节点 PV 按计算与 PV 约束选卷，条件预绑定；不重复建卷、不手写 Bound |
| WFFC 绑定后再次消费 | 复用已有卷与真实拓扑，不按新的计算节点重建或移动原卷 |
| 同一 WFFC PVC 并发首用 | 只存在一个选点/供卷 attempt，后到请求等待结果，不互相覆盖 selected-node |
| 混合卷联合选点 | 一个沙箱同时使用 Bound 卷和多个 WFFC 卷时选择共同可行节点；无交集不启动 VM |
| 普通 VM 接入 | 文件/块 endpoint 到真实 guest 可用，Driver 无 VMM 专属依赖 |
| K8S 管理权 | 已有 Pod/VA 禁止直接导入；不同对象别名不能绕过独占与回收保护 |
| 原生消费者竞争 | 导入检查期间和 SSF 使用期间新增 Pod 引用均被拒绝，交接完成后才放开 |
| 拓扑与 slot | 错误节点/区域/设备上限在准入失败，恢复槽位不会错盘 |

### 6.4 故障注入

| 注入点 | 预期恢复 |
| --- | --- |
| intent 后、RPC 前崩溃 | 从持久 intent 安全执行 |
| Driver 身份/能力查询失败或空集合 | 失败关闭相关新准入；合法空 optional 集合仅跳过对应可选操作，不全开能力 |
| 配置/证书轮换与 Driver 重启 | 验证新 revision，已有挂载保持 pin，未决 RPC 不被并发相反操作覆盖 |
| 同 request_id 改参数 | 返回冲突，不能复用另一组卷的成功结果 |
| 客户端取消 / step lease 丢失 | 持久 operation 接管已发出的副作用；不误报清理完成，不凭 lease 到期重建或删除 |
| 原生解析/供卷超时 | 保留同一PVC身份与唯一provisioner，不自动直接建卷 |
| RPC 成功后、outcome 落盘前崩溃 | 相同参数重试，不重复创建资源或提前补偿 |
| Prepared 返回前响应丢失 | 重试返回同一 Binding 和 target |
| VM 接入后 Ready 回执丢失 | Inspect runtime 后恢复，不能按超时卸载 |
| Unpublish/Unstage 超时或 busy | ReleasePending 保持 pin，退避重试 |
| SSF Node、driver 或 runtime 重启 | 重新核对 mount/设备/实例，不能仅信任旧路径 |
| 独占写者网络分区 | 无可靠 fencing 时阻止新 writer，验证不会双写 |
| 对象层上传失败 | 未完成版本不发布，旧 durable head 和 snapshot pin 不受损 |
| 多卷 checkpoint 中途失败 | 不发布半成品 manifest，执行解冻或隔离并保留恢复记录 |
| K8S apiserver 不可达 | 已运行数据面继续；新 writer 仍需权威批准，Catalog/Guard 不就绪时关闭相关准入 |
| Retain→Delete/PVC删除/原生回收中崩溃 | 许可与UID持久；不提前移除屏障，不让SSF finalizer与原生回收互相等待；原生Delete未知结果只由原生组件恢复 |
| 活跃 VM 使用中删除 PVC/PV | 平台删除流程先建立屏障，Guard 验证许可并拒绝在用删除；Retain 和回收权限避免原生误删 |
| Guard 不可用 | 受管破坏性 API 失败关闭，不影响已运行正常 I/O |
| 准入 dry-run / K8S 写入失败 | dry-run 无副作用；正式写入失败不提前解除排空屏障，按 operation 核对恢复 |
| 保护标签移除 / 受控范围 PV 别名 | 不绕过 UID/权威校验；未批准的管理权变更被拒绝 |
| PVC 同名重建 / PV handle 漂移 | 旧 UID 不被新对象替代，阻止新准入并告警 |
| CSI Controller Gateway 丢失响应 | 同operation核对Attach/Detach，不能并发相反操作；建卷和回收不经过Gateway |
| WFFC 容量不足 / selected-node 清除 | 确定该次供卷无未知结果后受控重选，不与 provisioner 并发建卷 |
| WFFC CreateVolume 超时 | 保留 PVC/operation 身份并等待唯一供卷方恢复，不能立即改节点或创建第二个卷 |
| 计算 lease 到期 / 节点故障 | 按已绑定卷实际拓扑重新规划；本地卷没有跨节点能力时返回明确不可行结果 |
| WFFC 多卷部分成功 / Controller 重启 | 保留已绑定资源与进度，重建预留并重新求交集，不删除仍受 pin 保护的卷 |
| CSINode/拓扑标签缺失 | 供卷前关闭相关准入，补齐注册后重试，不以不受约束建卷掩盖错误 |
| kubelet/native attacher 共存 | 只操作自己的卷；同一 handle 管理权交接之前禁止并用 |
| 可选 Holder Pod 被驱逐 | 下层沙箱先停止使用；不能只依赖 finalizer，异常进入隔离 |
| 节点 drain 与 driver 升级 | 拒绝新绑定、完成持久化与释放，或明确报告不可迁移资源 |

driver sanity 使用 [csi-test](https://github.com/kubernetes-csi/csi-test) 的适用测试；它不能替代以上沙箱、租户和联合 checkpoint 用例。

## 7 迁移与回退

新卷先使用新 class，旧卷默认不动。迁移旧卷时先设置维护屏障，停止新挂载，排空现有沙箱，确认 durable head 和 snapshot 引用，记录源 owner 的移交结果，再导入 SSF 并切换映射。导入已有数据默认 Retain。

回退也是所有权交接：停止新绑定、确认设备停止使用与版本已发布、交还唯一 owner，最后切回原 API 路径。转回原生 Pod 模式前确认 SSF Direct attachment 与 node targets 已清空；转入 SSF 前确认 kubelet targets 与 VA 已安全释放。不能在新旧框架同时活动时只切配置开关。已有 SSF 格式的 checkpoint、UID 映射和依赖版本需要保留读取兼容，不能在回退时删库。

## 8 落地前应确定的配置

当前方案默认 Linux、用户数据卷优先、适用场景优先 Immediate 提前供卷，同时正式支持 WFFC 节点相关供卷；共用 Direct attachment、独立 Node Agent、Runtime Adapter、后台恢复和按操作单一权威。后续评审需要确定优先后端、两项目消费类型、两种绑定模式的启动 SLO、计算 lease 接口、节点规模、共享读写需求与 checkpoint 一致性等级。

这些选择影响 PoC 优先级和资源预算，不影响“CSI 可直调且需补齐沙箱协调与 Runtime Adapter”的主要结论。

## 9 默认部署验收

确认CubeMaster或AgentENV权威入口嵌入公共消费协调，不要求额外ssf-controller；服务式替代部署不增加另一份使用权威。默认新增Node Agent与独立Guard，Gateway按Driver端点条件部署，所有模块/容器/进程归属与[部署图](08-full-deployment-architecture.md)一致。验证项目数据库、账本与Guard启动不依赖尚未就绪的沙箱卷路径。

本次交付是文档和架构图更新，以上PoC、性能目标与故障测试均为后续实现的验收要求，并未在当前仓库运行或证明达标。
