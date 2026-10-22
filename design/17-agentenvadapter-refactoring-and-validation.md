# AgentENV SSF 原生接入：重构、迁移与验收

> 2026-10-21 独立化更新：本页保留此前设计/阶段记录。当前 AgentENV 的消费与准备协调、账本事务在 `sandbox-storage` 的独立 `ssf-coordinator` 中执行；Scheduler 为轻量客户端，standalone 同样独立。以 [03](03-framework-design.md)、[23](23-current-implementation-architecture.md)、[26](26-independent-coordinator-implementation.md) 和 [13 部署](13-agentenvadapter-installation-and-deployment.md) 为准。旧内嵌宿主、`go.ssf.mod`、`-tags ssf`、原仓库 SSF overlay 不再适用。本次保留 Block/ext4 和全部原生功能，未新增 OverlayBD CSI Driver。

日期：2026-10-21。本文配套 [16：目标设计](16-agentenvadapter-native-integration-design.md)，描述改造与验收要求，不是已执行测试报告。此前原生接入已实施；本轮新增的“供卷定制准备 + microVM 挂载”和 PVC 直接引用仍待实现，见本文第 8 节及 [21](21-ssf-volume-provisioning-and-runtime-adapter-design.md)。[12](12-agentenvadapter-design-and-implementation.md)～[15](15-agentenvadapter-quick-test.md) 与 [18](18-native-integration-implementation-status.md) 保留当前代码和既有验证状态。

## 1. 先确认当前代码与目标之间的距离

| 能力 | 重构前基线（历史状态） | 本次目标/实施结果要求 |
| --- | --- | --- |
| Gateway/Scheduler | 原源码存在；adapter 部署绕开，关闭节点报告 | 原入口和两个原生工作负载继续工作 |
| 创建 API | adapter `/v1/sandboxes`，202，外层 launch/mounts | 原生路径、body、201、native ID 和 SDK |
| placement | adapter 配置 workers、私有 SQL 预留和拓扑检查 | 原生节点发现/策略/ReservationStore + 公共 topology |
| 模板 | adapter 静态 catalog，限制别名 | 原 SnapshotManager 投影、不可变 pin、继承卷 |
| 节点生命周期 | adapter 请求原生创建/删除；原生 hook 核验设备 | 原 Orchestrator 驱动插件；所有内外启动路径共用 hook |
| 暂停/恢复/快照/Fork | managed 卷被拒绝；自动策略被改写 | 原生能力保留、UseHold、联合 Capture/Clone |
| 事件和设备证据 | 有持久日志、真实 PID/FD、块设备/RO 检查 | 复用并搬到独立 StorageProvider，不再占用 custom extension |
| 共享账本与 Node WAL | 公共路径已经存在 | 原样复用主线，增加 owner/run、hold、checkpoint pin |
| Guard | Pod/PVC/PV 保护 | 补 Snapshot/Content 引用及暂停持有保护 |
| 文档和测试 | 原型构建/单测/实测脚本 | 原生兼容性测试和新部署实测，不能复用旧报告宣称通过 |

实施需记录 AgentENV 基线 commit、现有未提交补丁、SSF 版本和 schema 版本。先为当前树建立快照或补丁备份，再逐项重构；不要直接 reset 工作区或重复 apply 旧补丁。现有补丁的安全逻辑与架构偏差应分别处理。

## 2. 源码修改清单与侵入边界

### 2.1 AgentENV Go 控制服务

| 文件/模块 | 允许的改造 | 不应引入的职责 |
| --- | --- | --- |
| [gateway/internal/schedule_hint.go](../AgentENV/services/gateway/internal/schedule_hint.go) | 保留 advisory hint；增加必需的 launch intent 解析、模板引用和卷省略/空数组区分；完整 body 不变 | 直接选 Worker、创建 PVC、调用 CSI、维护模板 catalog |
| [gateway/internal/server.go](../AgentENV/services/gateway/internal/server.go) | 内部 requestUID、剥离外来私有 header、透传签名计划、同尝试重放；保留路由和响应处理 | 新公开 sandbox API、202 operation、替代 guest proxy |
| [api/proto/scheduler.proto](../AgentENV/services/api/proto/scheduler.proto) 及生成 SDK | additive 字段、内部存储协调 RPC、运行/预留确认和能力版本；不复用旧字段编号 | 将 PVC/CSI 暴露为所有原生客户端的必填项 |
| [scheduler/internal/service.go](../AgentENV/services/scheduler/internal/service.go) | 挂载 ControlPlugin；共享原生选点/预留 helper；原生固定节点启动准入 | 调用外层 Schedule 形成递归、adapter 再次选点 |
| [strategy.go](../AgentENV/services/scheduler/internal/strategy.go)、[filter.go](../AgentENV/services/scheduler/internal/filter.go) | 原策略消费公共可行性结果；计入未上报预留 | 全盘改成静态 workers 遍历 |
| [store.go](../AgentENV/services/scheduler/internal/store.go)、新 ReservationStore | 原 route binding 保留；独立持久预留、CAS、序号及过期处理 | 用路由 TTL 作为停止证明，混用 route 与 volume owner |
| [scheduler/cmd/main.go](../AgentENV/services/scheduler/cmd/main.go) | 插件依赖注入、配置/RBAC、内部 TLS、单写恢复器 | 默认另起 agentenv-control Deployment；复制整个 Scheduler |

新增存储 RPC 属于内部 `AgentENVStorageControl` 服务，可注册在原 Scheduler 进程。与原 Schedule 的增加字段共同定义，例如 `PreparePinnedRun`、`CommitRun`、`ReleaseRun`、`ResolveVolume`、`CreateCheckpointSet`；它们不接受任意 native 创建 body 后代用户创建 VM。Prepare/Commit/Release 都以持久 operation 和 request digest 幂等，权限绑定可信节点或 Gateway 角色。

### 2.2 AgentENV Rust 节点

| 文件/模块 | 允许的改造 | 保持的所有权 |
| --- | --- | --- |
| [api/impls/auth.rs](../AgentENV/src/api/impls/auth.rs)、API middleware | 可信内部 storage context 进入请求作用域；外部用户不能伪造 | 原 x-api-key/现有鉴权与原错误模型 |
| [api/impls/volumes.rs](../AgentENV/src/api/impls/volumes.rs) | VolumeProviderMux、SSF 元数据/facade；统一校验和列表分页 | legacy materialize/reserve 仍归 VolumeManager |
| [api/impls/sandbox.rs](../AgentENV/src/api/impls/sandbox.rs) | 将有效卷/模板 pin 交给共同 launch 路径；所有入口覆盖；只读元数据投影 | 原字段、默认值、native response、Fork 部分结果 |
| [orchestrator/service.rs](../AgentENV/src/orchestrator/service.rs) | 通用 hook、launchUID/native ID 关联、outbox、联合 Capture、暂停 hold、恢复 run；移除旧 blanket reject 前先补完语义 | native ID、生命周期状态、超时/驱逐/autoPause/autoResume、VMM 管理 |
| [sandbox/backend.rs](../AgentENV/src/sandbox/backend.rs) | 统一 checkpoint 屏障与通用外部设备 hook；保证其他 backend 的能力协商 | 原 SandboxBackend 抽象和生命周期控制 |
| [sandbox/extra_drive.rs](../AgentENV/src/sandbox/extra_drive.rs)、launch plan | 增加并列 PreparedExternalDrives；统一槽位和路径验证 | OverlayBD extra drive 的原类型/Drop/分层语义 |
| [sandbox/firecracker/sandbox.rs](../AgentENV/src/sandbox/firecracker/sandbox.rs)、[config.rs](../AgentENV/src/sandbox/firecracker/config.rs) | 已准备 block endpoint 接入、RO、真实进程/guest 证据、联合 freeze/capture/thaw | 原 Firecracker 创建/恢复/停止和 rootfs/tools/attachedDrives |
| [sandbox/manifest.rs](../AgentENV/src/sandbox/manifest.rs)、[snapshot/types/snapshot.rs](../AgentENV/src/snapshot/types/snapshot.rs) | 有版本的外部 checkpoint 引用和 slot/容量/冻结状态；旧格式兼容 | 原 snapshot ID、repository、cache、P2P、对象层发布 |
| [cfg.rs](../AgentENV/src/cfg.rs)、`../AgentENV/src/sandbox/custom_extension/storage.rs`（已移除的历史原型） | 把通用存储 hook/配置迁到独立 storage_provider；保留安全事件逻辑 | custom extension URL/参数仍属于用户扩展 |

由 OpenAPI/protobuf 源生成的文件通过生成器更新，避免手改生成路由。生命周期 public schema 不增加必填字段；只有 Volume API 扩展需要调整 public OpenAPI。测试应对比生成前后的 schema 差异并确认 SDK 兼容。

### 2.3 SSF adapter 的去留

| 当前文件/内容 | 重构处理 |
| --- | --- |
| `../sandbox-storage/adapter/agentenvadapter/control.go`（已移除的历史原型） | 移除公开 `/v1/sandboxes` 和独立沙箱状态机；抽出公共消费调用、幂等存储步骤、卷映射至嵌入库 |
| `../sandbox-storage/adapter/agentenvadapter/placement.go`（已移除的历史原型） | 原生 placement bridge 替代私有调度；通用 PV/SC 匹配移到公共模块；计算 reservation 数据经显式迁移/排空，不双写 |
| [types.go](../sandbox-storage/adapter/agentenvadapter/types.go) | 移除外层 CreateRequest/launchBody 的强制改写；保留通用 Device/Event，并加 owner/run/context 版本 |
| [node.go](../sandbox-storage/adapter/agentenvadapter/node.go)、`../sandbox-storage/adapter/agentenvadapter/runtime.go`（已移除的历史原型） | 去掉 normal create/delete/guest proxy 职责；保留 Prepare、evidence、Release、Inspect/Recovery，实现 Freeze/Thaw 协作 |
| [platform_linux.go](../sandbox-storage/adapter/agentenvadapter/platform_linux.go)、[journal.go](../sandbox-storage/adapter/agentenvadapter/journal.go) | 复用设备、进程、pidfd、初始化与 bbolt 日志；fencing 角色和作用范围明确授权 |
| `../sandbox-storage/adapter/agentenvadapter/volumes.go`（已移除的历史原型）、`../sandbox-storage/adapter/agentenvadapter/store.go`（已移除的历史原型）、`../sandbox-storage/adapter/agentenvadapter/schema.sql`（已移除的历史原型） | 保留卷 UID/租户映射及初始化策略；拆分原生 reservation 与公共消费状态；停止 adapter public sandbox identity |
| [server.go](../sandbox-storage/adapter/agentenvadapter/server.go) | node 服务和证据接口保留；control 改为 Scheduler 注入的库；管理诊断不成为用户生命周期入口 |
| scripts/render.py、deploy/installation、config workers/templates | 用原 Kustomize base 的增量 overlay 替换；取消独立 control、静态 catalog 和关闭 reporting 的配置 |
| api/openapi.json、examples/client.py、旧 realenv 工具 | 标记原型工具；新验收使用原 AgentENV SDK/API，避免继续生产第二套客户端 |

### 2.4 公共代码改造

按 [16 第 9 节](16-agentenvadapter-native-integration-design.md#9-ssf-公共框架的有限优化) 增量修改 `runtime`、`core`、`consume`、`ledger`、`guard`、`kubernetes` 与必要 API/SDK。公共结构用 owner/run/checkpoint/volume 身份表达，不添加 `AgentENV` 字段。

数据库升级至少包括 owner/run 关联、UseHold、CheckpointSet/组件 pin、Ready receipt。旧 binding 映射为一个 owner 的单次 run；未知归属保守保留，不凭 TTL 自动清除。迁移脚本可重入，旧版本节点发现新协议/profile 时明确拒绝，而不是忽略字段继续启动。

## 3. 原生部署 overlay 规范

增量 overlay 已在当前工作区实现并离线渲染验证；真实部署未执行。下列为实际目录，配置和剩余验收见 13、18：

```text
AgentENV/deploy/k8s/base/                    原有 base
AgentENV/deploy/k8s/overlays/ssf/             新增 overlay
  kustomization.yaml                        resources 引用原 base
  gateway.yaml        入口、角色、协议限额
  scheduler.yaml       插件、计算 store、消费配置
  node.yaml          sidecar、PID namespace、hostPath、drain
  rbac.yaml              Scheduler/节点最小授权
SSF/deploy/                                 继续公共 Node、Guard、条件 CSI Gateway
```

渲染检查必须证明：

1. 原 Gateway/Scheduler/agentenv-node/headless Service 均存在，selector 匹配，外部地址/端口和 native API 路径不变；没有 agentenv-control 替代服务。
2. 原 Scheduler 心跳、事件、EndpointSlice/static discovery 方式及 P2P 配置保留；Node Pod 名与 K8S Node/CSI node_id 有可验证关联。
3. API 对 Gateway 可达，只有本地 storage hook/evidence 隐藏在内部；正常用户仍使用原 x-api-key，无需原型专用用户证书。
4. 原节点挂载、探针、优雅终止预算和环境配置保留；新增 SSF 挂载可见性和 durable WAL，不覆盖 custom extension URL。
5. 节点 drain 拒绝新分配，等待原生活动实例停止、暂停状态安全交接和 cleanup 可恢复；不能仅检查 active sandbox list。
6. CSI Controller 仅 UDS 时追加受限 Gateway；已有认证网络入口则复用。公共 Node Agent 每物理节点一个，证据按 runtime/node 分派，不能为每个静态 worker 再造重复 Node Agent owner。
7. Gateway/Scheduler 的存储内部 RPC 具有角色认证；原先 insecure gRPC 不能直接开放新的状态修改能力。共享账本、RBAC、Guard Snapshot 规则与协议版本共同上线。

原生 standalone 若原本没有 Gateway/Scheduler，不强制为了存储增加它们：本地 NodePlugin 的 Go 宿主可承载同一公共消费库，使用固定节点 PlacementProvider，计算准入由原生节点负责；它不提供 sandbox API、不创建 VM。与集群模式二选一配置同一 run 的消费权威，共享 SSF 使用账本，不能同时由本地与 Scheduler 两处推进。集群部署默认仍嵌入原 Scheduler。

## 4. 实施阶段与门禁

| 阶段 | 可独立审查的交付 | 进入下一阶段的条件 |
| --- | --- | --- |
| P0：锁定兼容基线 | 原生 API/schema、SDK、路由/部署、默认值、legacy 行为基线；当前补丁备份 | 能区分现有行为、原型行为、目标行为；无新增 public sandbox API |
| P1：公共 SPI 和原生薄桥 | 版本、身份、内部上下文、VolumeProviderMux、只读模板投影、插件禁用路径 | 原插件关闭时 legacy/无卷回归通过；伪造/超限上下文拒绝 |
| P2：原生调度与正常消费 | ReservationStore、共同 topology、四种绑定组合、计划下发、Ready/Commit、停止 outbox、原部署 overlay | 原 native API 可完整创建/删除 CSI 卷沙箱，故障恢复通过；仍不能宣称完整接入 |
| P3：完整生命周期 | UseHold、pause/resume、统一 Capture、Snapshot pins、Fork/模板恢复、custom extension 共存 | 完整 profile 的真机数据与进程证据验收；取消旧 blanket reject |
| P4：兼容升级与迁移 | schema/协议升级、原型排空、PVC 精确身份导入、部署回归、运维手册 | 所有强制测试通过，更新实测报告后才能替代旧部署手册 |

不能先删除拒绝分支再补一致性；不能把 P2 演示当成本次需求完成。实施 PR 可按阶段拆分，但必须按 P4 完成状态交付原生接入方案。

## 5. 验收矩阵

每项记录 AgentENV/SSF 镜像与版本、Driver/SC/profile、K8S Node UID/CSI node_id、public sandbox ID、request/owner/run/operation、原生资源 UID、guest 校验和与节点证据。通过 API 返回值不足以证明物理释放或数据一致性。

### 5.1 原生兼容性

| ID | 场景 | 必须观察到的结果 |
| --- | --- | --- |
| C01 | 原 SDK 冷/模板/v2 创建；不安装 SSF 或插件禁用 | 原路径、默认值、201/响应结构/native ID 保持；legacy 可运行 |
| C02 | 同样 SDK，卷名对应 SSF；通过原 Gateway 访问 | 无 wrapper body/adapter operation；Scheduler 实际参与选点，节点心跳仍可见 |
| C03 | 列表、查询、删除、timeout、metadata、自定义扩展 | 原接口回归；custom extension 的 URL/参数/回调与 SSF 同时生效 |
| C04 | envd、HTTP、WebSocket、流式响应、端口/host/header 路由 | 继续原代理完整能力；不是 `/guest/...` 子集 |
| C05 | 模板别名在请求中途变更；CPU/槽位不同 | 执行已 pin 版本，资源与 plan 一致；不查静态 adapter catalog |
| C06 | 模板继承卷：字段省略、空数组、显式替换 | 三种结果符合原生语义；隐式 CSI 卷也走 SSF |
| C07 | legacy 卷 + SSF 卷 + attachedDrives 混用 | 数据、顺序、路径/重复卷/数量校验一致；部分失败正确补偿 |
| C08 | 默认 autoPause、autoResume、资源驱逐、connect 恢复 | 不覆盖成 delete 或关闭自动恢复；原 native ID/路由保持 |
| C09 | 存储 plan 过大、畸形 hint、伪造内部 header、越权 namespace | 明确原 Error 结构；不退化为无卷启动，无侧门权限 |
| C10 | 原生无卷模板构建及 P2P 分发、只读槽位不足 | 模板功能不被强制 storage_required 阻断；不足时清晰拒绝并支持原流程重建 |

### 5.2 调度、CSI 与数据语义

| ID | 场景 | 必须观察到的结果 |
| --- | --- | --- |
| S01 | 动态 Immediate、静态 Immediate、动态 WFFC、静态 WFFC | 原生调度 lease + 原生绑定；真实 Bound 后 Attach；无逐 VM holder Pod |
| S02 | 拓扑交集为空、CSI API 读取失败、容量不足 | 三类错误可区分；不把 API 故障当成可任意选点 |
| S03 | 并发普通创建与 SSF 创建、Fork/恢复/模板构建争抢容量 | 同一原生准入计算，未上报 reservation 不被忽略，无重复计费 |
| S04 | 两个 owner 写同一 exclusive 卷；RO 共享 | writer 冲突拒绝；合法只读共享，每个 run 有引用，最后一人才能逆操作 |
| S05 | Kubernetes 新卷、已有 PVC、snapshot restore PVC | 按 profile/来源/真实设备/持久历史授权；新空卷自动准备，不按创建入口区分；已有数据校验和不变 |
| S06 | 删除沙箱、重新挂载卷 | native stop 先于 Unpublish，最终引用才 Detach；PVC 保留，数据持续 |
| S07 | 暂停后另一 sandbox 请求原 exclusive 卷；恢复后旧 stopped 事件到达 | hold 阻止抢写；恢复使用新 run；旧事件不释放新 writer |
| S08 | 多卷 Snapshot 同时持续写入递增事务标记 | 同一冻结屏障内捕获内存与卷；恢复验证跨卷标记和内存一致，不只检查文件存在 |
| S09 | Fork 多 child，一部分 PVC/启动失败 | RW 相互独立，RO 符合共享策略；native 部分结果保留，父/成功 child 不受失败补偿影响 |
| S10 | 删除源 sandbox/源 PVC，再从已提交 snapshot 创建 | checkpoint 依赖受到 pin/Guard 保护，恢复数据正确；不能靠残留路径偶然成功 |
| S11 | 旧 manifest、缺 RO 槽位、容量变化、冻结内存恢复 | 兼容旧 legacy；不兼容设备明确拒绝；所有成功恢复完成 thaw |
| S12 | API 删除被引用 Snapshot/Content，变更关联或回收策略 | Guard 拒绝；正常删除原生 snapshot 释放 pin 后走原生回收 |
| S13 | Driver 不支持 snapshot、freeze 或 restore | capability/profile 提前说明；不标为完整兼容，不影响 legacy 原功能 |
| S14 | 离线扩容与旧 checkpoint 恢复；在线能力协商 | 原生 PVC 流程，容量一致性可验证；未认证在线扩容不会冒险执行 |

### 5.3 失效窗口与恢复

| ID | 故障注入点 | 验收断言 |
| --- | --- | --- |
| F01 | 预留后、WFFC 超时、绑定结果未知时重启 Scheduler | 同 attempt 可恢复；未确认旧结果不能换节点重复供给/启动 |
| F02 | Attach 后、未派发；派发前后分别杀 Gateway | 未派发有证据才 Abort；已派发查询原生日志，不把断连当未启动 |
| F03 | VMM spawn 前后分别杀节点/sidecar | durable launching 先于 spawn；PID/start/boot 核验，未知保留锁 |
| F04 | Node Ready 后丢 Commit，原生 201 丢失 | 同一 launchUID 不产生第二 VM；可以恢复提交或由原生停止补偿 |
| F05 | pause 保存过程中、UseHold 转换过程中崩溃 | 不发生独占权空窗；失败 pause/resume 留下可判定原状态 |
| F06 | 冻结后任一 Snapshot 失败、联合发布前崩溃 | 原 snapshot 不可见 Ready；源 VM thaw 或隔离，孤立组件有 pin/GC 记录 |
| F07 | 删除后 DB/CSI 不可用、NodeUnpublish/Detach 返回不确定 | outbox 持久；不误报物理释放、不删除用户 PVC；恢复后幂等清理 |
| F08 | Redis binding TTL 到期、心跳丢失、原节点网络分区 | route 丢失不释放 writer；无可信 fencing 不允许新节点写 |
| F09 | PID 重用、Pod 重建、节点 reboot、同名 PVC 重建 | starttime/boot/UID 能区分；旧 plan/事件拒绝，数据身份不串用 |
| F10 | 滚动升级、native preStop、旧/新协议混合 | 原服务存在，drain 覆盖存储引用；不删除 WAL，不由两个控制器并管 |

### 5.4 性能和完成证明

对比同一 Driver/节点的原生无卷、legacy 卷、SSF 已绑定卷、SSF WFFC 首次消费；分别记录端到端 p50/p95/p99 及 Resolve、Reserve、Bind、Attach、Node、VM、guest、Commit 分段。保留同样 API 就绪定义，不用 202 返回耗时对比原生 201。

记录暂停、快照/多卷冻结窗口、Fork 子数增长的开销和故障恢复时间；冷后端真实延迟不能从统计中删去。阈值以可复现基线及项目目标设定，不预填未经测量的毫秒承诺。

实施后的证据包应包含：原生 API/schema 差异、Kustomize 渲染差异、控制/节点日志关联、K8S UID/状态、VMM/guest 设备证据、数据校验、失败恢复记录、未通过项。只有本机单测/交叉编译时，报告必须标明未做 Linux/KVM/真实 CSI 实测。

## 6. 从当前原型迁移与回退

1. 新增协议/schema 先完成向后兼容验证，以原生部署的隔离试点验证整套 profile。旧原型不可与新链路同时消费同一 writer；试点隔离只是升级手段，不是最终强制另建平台。
2. 停止原型新建，按其原操作驱动正常停止和释放；核对真实 VMM 退出、Node Unpublish/Unstage、attachment 和计算 reservation 全部可判定。未知操作逐个恢复/fence，不删除日志“解锁”。
3. 保留 PVC/PV/快照及原始 UID、数据和 Retain 策略。导入原生 VolumeProvider registry 时校验原 volumeUID、租户、容量、FS UUID、权限及初始化标记；既有数据永不初始化。
4. 按原 Kustomize base 上线 Gateway/Scheduler/节点新镜像与 overlay，确认心跳/路由/P2P/原生 API，再以原接口创建新沙箱挂载旧数据卷。原型操作 ID 不假冒 native sandbox ID；不承诺无停机搬迁正在运行的旧 VM。
5. 如果需要保留原型中的暂停/运行身份，必须另做原生状态恢复迁移且验收；当前原型本就不支持外部卷 pause，不能凭改 ID 宣称迁移完成。
6. 回退前排空新版本 SSF 运行与暂停持有，确认 checkpoint 依赖和 schema 可读范围。不能让旧二进制读取未知 manifest 或忽略新 hold；先回退入口/功能开关，持续保留可恢复的 storage worker 和 Guard。
7. 通过全部强制验收后重写部署/快速操作手册，退役原型 public API、静态 workers/templates、旧渲染器及其用户客户端。此前旧文档继续标记历史，避免把目标协议当成现成命令运行。

## 7. 首轮原生接入设计修订的历史验证范围

本节保留首轮设计阶段记录；其后软件实施结果见 [18](18-native-integration-implementation-status.md)。当时只交付设计和关联文档，明确原生架构、标准 plugin 边界、接口/身份、完整生命周期、有限公共框架优化，以及源码改造和验收矩阵。当时新 SPI、overlay、UseHold、Capture 实现尚未交付。

文档检查包括本地文件链接、Mermaid 图源与文内图一致性、原参考文档不变，以及 AgentENV/SSF 实现文件未被本次文档编辑改写。实现阶段需另行执行上面的测试；旧原型测试结果不能覆盖这些新增要求。

本次检查结果（2026-10-20）：330 个本地链接及相关章节锚点通过；3 张 Mermaid 图完成解析、SVG/PNG 导出与视觉检查，图源与正文一致；编辑前记录的 759 个 AgentENV/SSF 实现及配置文件哈希均不变；`assets/K8S-CSI架构分析.md` 哈希不变。本次未执行运行时、部署或真实 CSI/VM 测试。

## 8. 2026-10-21 卷准备与 PVC 直接引用增量

本轮只完成设计更新，尚未修改实现。目标详见 [21](21-ssf-volume-provisioning-and-runtime-adapter-design.md)，先完成公共安全机制，再开放直接引用，不以取消 `Import` 禁止分支作为完整修复。

| 阶段 | 待实现内容 | 门禁 |
| --- | --- | --- |
| A | profile/来源固定，VolumePreparation operation、证据、独占权和节点 WAL | 与 writer/hold/维护/回收互斥；未知格式化和旧 epoch 不被覆盖 |
| B | 公共 PVC watcher、Guard 默认化及保护、Immediate 自动准备 | kubectl 建卷后无需沙箱即可 VolumeReady；没有初始化 Pod/import |
| C | WFFC 选点后的同一准备链及原子接续 | 动态/静态四组合；准备 FD 关闭才交给运行，无双 writer |
| D | volumeMounts 的 PVC 来源、UID 归一、原 Volume API 共用规则 | 旧请求兼容；同名重建拒绝；模板/恢复固定源；不同别名不绕过互斥 |
| E | 部署配置/RBAC/协议/SDK、两步验证和补丁重放 | 真机读写和故障证据；本机检查不得代替 CSI/KVM |

公共新增限于现有 kubernetes/consume/runtime/ledger/node/guard/API 模块及配置。AgentENV 原生侧仅增加通用引用、能力和 hook 数据，Scheduler 启动公共准备协调器；不新增厂商 CSI 客户端到 Rust，不替换 Gateway/Scheduler。

具体验收采用 [21 的 P01～P12](21-ssf-volume-provisioning-and-runtime-adapter-design.md#12-实施顺序与验收门槛)，并继续执行本文件 C/S/F 生命周期矩阵。当前 [20](20-kubernetes-pvc-agentenv-mount-validation.md) 的初始化 Pod 流程只反映旧实现限制，不是本轮验收方案。
