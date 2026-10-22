# 28 — OverlayBD CSI 实施状态与验证记录

更新时间：2026-10-22。目标设计见 [27](27-agentenv-overlaybd-csi-reuse-design.md)。

手动验证入口：[29 — OverlayBD PVC 手动验证](29-overlaybd-pvc-manual-validation.md)。

## 当前结论

**完整实施计划尚未完成。** 已接入原生共享引擎、POSIX/OSS 宿主、受控 daemon、
CSI Controller/Node、活动快照、离线扩容、公共回收及停止后最终发布主链路；
未知设备创建、restack 中断和封存前快照恢复仍有实现缺项。
目前不能声称 PVC → OverlayBD → microVM 主场景已经可用，也不能投入生产部署。

既有 generic CSI 的测试结果不计入 OverlayBD 验收。下面区分已交付代码、实现缺项和
尚未执行的真机验证；不能把实现缺项归类成“只差真机测试”。

## 基线和归属

基线在 `../refactor-baseline/overlaybd-20261022-102450/`，分别保存两个仓库的 HEAD、
暂存/未暂存 patch、未跟踪文件副本和 SHA-256。没有重置部署修复，没有提交、推送或部署。

| 位置 | 当前实现 | 边界 |
| --- | --- | --- |
| `AgentENV/storage/volume-core` | 原卷引擎、模型、POSIX/OSS catalog、backing 创建/clone、层压缩/发布/物化；新日志与不可变 head | 不依赖 AgentENV 主程序、K8S、SSF 账本 |
| `AgentENV/src/volume.rs` | 原 VolumeManager 委托共享引擎，保留原配置/清理和快照上下文 | 原入口与公开模型保持原语义 |
| `AgentENV/src/snapshot/repository` | POSIX/OSS 卷及层算法委托共享实现 | memory/rootfs、模板、P2P 发布和原生工件仍留原项目 |
| `AgentENV/storage/ublk-daemon` | 受管会话协议、独立状态目录/进程身份、真实 block 设备检查、capture、release、drain | 复用原设备引擎；legacy 默认行为保留 |
| `AgentENV/storage/overlaybd` | 原 runtime config 视图、ext4 几何核验及带持久化回调的 restack | 用于持久捕获配置，不替换数据 I/O |
| `sandbox-storage/drivers/overlaybd/native` | 版本化 UDS gRPC 服务及可执行入口；原生卷/设备宿主 | 入口组装 POSIX/OSS 原生共享后端；不控制 VMM |
| `sandbox-storage/drivers/overlaybd` | PG 操作/卷/attachment 记录；CSI 创建删除/Attach/Detach/能力校验；Node 会话与 capture/release 协调 | Linux controller/node 可执行入口、标准 Node RPC 和受管清理扩展已增加；完整部署及生命周期仍未完成 |
| `sandbox-storage/ledger` | backend 回收 tombstone、与 Register 互斥、已有使用/hold/维护/准备/快照保护；迁移重入优化 | 公共模块，不导入 AgentENV 类型 |
| `sandbox-storage/gateway` | 鉴权后仅转发绑定 ID 到内部 CSI Driver | 不转发客户凭据和无关 metadata |

原生共享库保留原算法，不在 Driver 中重新实现 ext4→OverlayBD、clone、层压缩或 OSS
条件写。已移除抽离后不再使用的私有包装函数；仍被原生测试使用的目录辅助逻辑保留在
测试构建中。本轮没有裁剪 VolumeManager、SnapshotManager、OverlayBD、P2P、builder
或 recorder 功能，但相关真机兼容性尚未全部验证。

## 已实现的持久边界

1. 创建名称固定 operation/native volume ID/input digest。创建请求的来源与拓扑也固定；
   重试不能随源卷 head 的变化选择另一个版本。CSI secrets 不写入创建登记。
2. 原生创建前保存 intent/checkpoint，按固定 ID 创建独占目录。空卷 FS UUID 绑定卷及操作
   身份；同容量的不同空卷不会得到相同 UUID。clone 保留来源内容，不失败回退为空盘。
3. Started/未知结果不能重置成新的格式化操作。明确 Pending 且身份匹配时，只继续原操作。
   原始回执、部分目录和格式意图保留供恢复使用。
4. POSIX head 使用锁、不可变 manifest、原子指针替换和 fsync；对象存储 head 使用 ETag
   条件更新。发布校验父 revision、FS UUID 和 capacity generation，旧 head 保留。
5. daemon 会话固定 session/run/epoch/node/boot/daemon/device 身份。原设备编号被复用时，
   不能当作原设备继续释放或接纳。专属 daemon 不因宿主客户端退出而被析构杀掉。
6. capture 固定 barrier、冻结证据和封存结果；publication 固定目标 head。可写运行卷释放
   要求真实停止后的最终 capture 已发布，FD 引用已经归还。准备使用独立释放证据。
7. 释放意图在调用 daemon 前持久化。释放结果未知时保留排他屏障，禁止重新借用、捕获和
   发布。Node WAL 的释放重试同时固定停止证据、最终 capture/head 和 FD 解除证据。
8. CSI Attach 使用账本授权，固定已发布 head、FS UUID、容量及 generation；回执丢失后
   重放原 attachment，不重新读取一个可能已经变化的 head。Controller 不调用 CSI Node。
9. 回收检查已有运行、暂停 hold、准备、attachment、维护和快照引用。未被 PVC observer
   登记的后端也能写入回收 tombstone，阻止延迟登记和别名复活，不创建虚构 sandbox。
10. 回归发现重复迁移 DDL 与 Acquire 事务发生死锁；迁移现在在完成当前 schema 后直接
    返回，并拒绝高于自身支持版本的 schema。新增测试验证重入不请求表结构独占锁。

FS UUID 回执与已发布内容元数据关联，并不等于完成实际设备/guest 的 ext4 核验。
OverlayBD Preparation 的 verify-only 接线已实现；实际设备与 guest 核验仍须在真实环境验收。

## 验证记录

| 项目 | 结果 | 证据与范围 |
| --- | --- | --- |
| 共享库 metadata 测试 | 通过 | 17 项 metadata 测试；目录、日志、POSIX/object head 和扩容 CAS；没有执行完整 content feature |
| 宿主协议/身份测试 | 通过 | 同目录 backend-protocol：3 项；Go/Rust 摘要一致、整数精度、独立卷 UUID、协议与日志 |
| Go 定向 race/vet | 通过 | OverlayBD、ledger、gateway；数据库项目由下项另行执行 |
| 临时 PostgreSQL 集成 | 通过 | `validation/overlaybd/postgres/report.json`；无跳过，含创建/删除/Attach 丢失回执、回收屏障、迁移重入及既有 adapter 回归 |
| 全量 Go 单测 | 通过 | `go test ./...`；首次受本机回环监听权限限制，允许本机监听后复验通过；DB/真机条件项不由此项覆盖 |
| 协议生成一致性 | 通过 | `scripts/check_generated.py` 覆盖 Node、control 和 OverlayBD |
| Linux 目标 Rust 类型检查 | 通过 | `validation/overlaybd/native-cross` 与 `host-cross`，必须同时满足 exit_code=0、source_stable=true |
| Rust Clippy | 未执行 | 本机安装的独立 Rust 工具链不含 cargo-clippy；Linux 交付检查应执行原项目 clippy 入口 |
| Linux content 测试/宿主链接运行 | 未执行 | 交付 `scripts/check_agentenv_linux.sh` 入口；交叉类型检查不等于执行 |
| ublk/KVM/真实 CSI 主场景 | 未执行 | macOS 未运行真实设备/VM/K8S |
| 本地 POSIX 完整供卷/发布 | 未执行 | 目录与协议 fixture 不能证明 ext4→OverlayBD→ublk 数据链 |
| 真实共享 POSIX/OSS | 未执行 | object CAS fixture 不能代替真实对象存储验证 |

PostgreSQL 使用本机隔离临时实例，不连接外部数据库。真实环境验收仍须按 Immediate/WFFC
分别执行“PVC → A 写入 → A 删除并最终发布 → B 读回”，并观察 OverlayBD 层和 ublk 会话。

## 尚未完成的实现和验收

- 初始 ublk 创建结果未知时的完整恢复，以及 daemon 重启后尚未完成文件系统工具的维护设备恢复。
  当前保留原操作、upper 和排他占用，不换新 operation，也不自动删除未知设备。
- restack 在封层、重命名、生成新 upper 的内部步骤中断且尚无持久 runtime config 时的恢复。
  扩容维护的停止后恢复已覆盖封层未完成、完整封层未改名、已改名未落盘配置，以及 config 落盘后回执丢失；活动运行 capture 的内部中断恢复仍需补齐。
- 直接 K8S 活动 Snapshot 在原生封存前中断的完整恢复。已封存和已完成 checkpoint 的
  丢失响应可以恢复，不能把此前未完成的任务改成读取较新的空闲 head。
- 原生暂停/恢复、自动策略、联合快照、Fork、模板替换、混合卷、builder/recorder 的
  全部实际 OverlayBD 数据和故障回归。已有接线及本机测试不代表完整生命周期验收通过。
- Controller/节点 overlay、静态登记工具、standalone 和增量离线扩容配置已经交付；
  Linux systemd、K8s sidecar、联合 drain、真实本地/共享 POSIX 与 OSS 部署验收未执行。
- 全量设备故障矩阵和真实端到端验收。主场景工具已有 native-overlaybd 模式，要求真实
  ublk 和同卷层 head 证据；当前没有真实环境，不能记为通过。

代码中已开放的能力不代表这些恢复缺项已经消除。当前不应作为生产部署的完整 Driver。
补丁重放和侵入量报告随代码更新；增量哈希清单不等于相对上游的侵入行数。

## 分阶段实施记录

以下保留各阶段的变更与当时验证范围。涉及“尚未实现”的阶段描述，以前面的当前状态
及文末后续增量为准；不能把某阶段单测通过解读为设计 27 全部完成。

## 本轮 Node 链路增量（2026-10-22）

- `Plan.PublishContexts` 固定 `ssf.dev/stage-scope=binding-v1`；公共 Node 的共享判断、释放、
  扩容路径均读取 WAL 中的计划。普通 CSI 仍使用按卷共享 stage 的行为。
- Stage/Publish 携带完整签名计划及唯一 binding metadata；Driver 独立检查签名、node UID、
  boot、CSI node ID、owner/run/epoch、能力、volume context、head 和规范路径。
- Node 路径在设备/挂载副作用前持久占用，旧 target 不能改绑其他会话。Linux 使用 bind mount，
  检查真实块设备 major/minor/容量、拒绝符号链接；不用 lazy unmount 隐藏繁忙资源。
- 公共 Node WAL 保存签名 token 和已验证的停止证据。协商 `ManagedStorageDriver v1` 的
  Driver 必须先完成 Finalize，再执行标准 Unpublish/Unstage；普通 CSI 不调用这个扩展。
- 最终 capture/publication 固定停止证明、FD 归还证明、capture ID 和父 head；中断重放不能
  换新 capture。最终发布尚未确认时不会解除运行时 borrow，终结中的会话不能重启。
- 准备使用独立释放回执和私有节点信任链，不伪造 VMM 停止。准备与运行释放由不同 RPC 处理。
- AgentENV OverlayBD profile 为 verify-only；没有已发布 UUID 或请求 generic 初始化均拒绝。
- 新 Linux 入口 `cmd/overlaybd-csi` 组装 Controller/Node、账本、原生 UDS worker、私有 CSI
  socket、真实设备 publisher 和 mTLS runtime verifier。当前 Controller 已有固定快照来源 resolver，
  但创建快照的原生屏障尚未完整接入，
  不得将该入口描述为已经完成全部 CSI 能力。

新增本机测试覆盖 stage 隔离与重放、签名篡改/续签、设备创建响应丢失、未停止拒绝清理、
最终发布响应丢失、cleanup 前置 Finalize 失败重试、OverlayBD 禁止格式化。设备/VM/OSS
实际运行仍未执行；这些单测和 Linux 交叉编译不能替代真机验收。

OSS 增量：原 `OssClient` 已迁入 `volume-core/oss_client.rs`，SnapshotRepository 通过薄包装
继续复用。`MetricGuard` 同步迁入共享库并由原 observability 模块 re-export，保留原指标
名称、标签及取消语义。宿主的 `repository_config.rs` 组装原对象卷仓库和 head CAS，
不复制对象读写/上传算法。真实 OSS/共享文件系统测试未执行。

Controller 本地供卷 worker 路由增量：`BackendWorker` 仅暴露 repository 管理方法，
隐藏其他 repository 和设备能力。远程连接要求客户端证书/CA，服务端要求明确 URI SAN
白名单；它不调用 CSI Node，不代理业务 I/O。单测覆盖外部 repository 拒绝和设备 RPC
不可用。Controller/worker 的实际跨节点部署与真机验证仍未执行。


## 快照持久引用与卷查询增量（2026-10-22）

- Driver schema 升至 4，增加 `snapshot_references`。快照固定源 handle、不可变 head、
  文件系统 UUID、容量、K8S Snapshot/Content UID 及 capture digest；这些字段必须由可信
  捕获协调链提供，不能由用户 annotation 直接授予授权。此阶段尚未开放 CreateSnapshot；后续接线见下节。
- 注册快照与源卷删除共用源卷行锁。存在未删除快照时拒绝进入 native 删除；源卷开始
  删除后也不能再注册快照。逻辑删除留下 tombstone，不回收原生层文件。
- 克隆在派发前持久 pin 快照，在独立目标 head 和卷登记均成功后解除引用。未知结果保留
  引用，沿用同一 native operation 恢复；已释放引用不能由延迟请求重新获取。
- `CreateVolume` 默认解析已登记的快照来源，限定同一 repository、相同容量；来源缺失、
  已删除、跨仓库和容量不匹配均明确失败。普通 live-volume clone 仍需要协调器屏障，
  不转换成空卷或读取旧 head。尚未广告完整 CSI 快照/克隆能力。
- 实现 ControllerGetVolume 和带稳定游标的 ListVolumes，包括 published node IDs，
  返回原始 content source，排除正在删除及已删除卷；对应查询能力已广告。
- PostgreSQL 集成测试覆盖快照注册/删除竞争、clone pin/tombstone、成功克隆后的来源
  删除及重试、卷查询分页和节点信息；测试 fixture 不证明真实 OverlayBD clone 数据链。


## 捕获分阶段协议与恢复增量（2026-10-22）

用户已明确当前没有真实测试环境。仅隔离 namespace 的部署/测试资源清理已获授权，但
尚无可执行目标；本轮没有外部部署或真实设备测试。

- 公共 `core.CaptureIntent` 承载中立捕获身份，原 `consume.CaptureIntent` 保留类型别名；
  不引入 AgentENV 私有类型或 Driver 依赖。
- 公共 Node 与私有 ManagedStorageDriver 增加 PrepareCapture、InspectCapture、PublishCapture；
  Driver 显式协商 `ssf.dev/managed-capture=v1`。原 CSI 方法和公开 sandbox API 不变。
- PrepareCapture 独立验证签名计划、run/epoch、节点、绑定集合和原生捕获证据；由配置的
  mTLS `/v1/capture/evidence` 端点核验实时 native outbox/冻结设备。首次捕获不接受过期计划。
- Node WAL 固定 capture ID、barrier、意图摘要、父 head 和捕获回执。未知捕获阻止新屏障
  越过；已有稳定回执可以在 thaw 后查询、发布，不重新执行 restack。
- 捕获回执和发布回执分别落盘，发布回执与 session head 在同一 bbolt 事务更新。
  重放历史捕获不会将当前 head 回退到旧版本。未取得稳定捕获回执不能发布。
- 只读会话复用 native 的配置捕获路径，跳过 restack；后续查询固定 immutable head，
  不更新源卷 head。设备实际行为尚需 Linux 测试。
- Node 后台每 5 秒重试已持久捕获的发布以及已持久授权的释放；退出时先取消并 join
  恢复循环，再关闭 WAL/后端连接。不会自动重做未知 capture、创建 writer 或推断停止。
- daemon 对已持久封存的捕获结果先验证原始 barrier/冻结摘要，再返回原回执；此时不要求
  旧设备仍在线，也不操作同编号的新设备。这覆盖封存成功后回执丢失并重启的恢复窗口。
- 这个恢复循环及已封存回执重放不等于完成 daemon 重启后的旧 upper/设备恢复；后者仍是实现缺项。
- Node 配置新增必填 `captureEvidenceURL`，示例使用 `/v1/capture/evidence`；
  `evidenceURL` 示例修正为原 adapter 实际提供的 `/v1/evidence`。

本轮验证：Go 全量单测、相关包 race/vet、临时 PostgreSQL 集成（无跳过）、协议生成
一致性、Linux Go 构建和宿主 Linux 目标 Rust 类型检查通过。新增测试覆盖捕获后重启、
发布响应丢失、thaw 后发布、伪造/更换屏障、新捕获证据失效、未协商能力、异作用域回执、
只读 head 不可变化，以及恢复循环不重做未知捕获。上述均不替代真实设备数据验证。

协调器和标准 CSI Snapshot 的后续接线见下一节。公共 Checkpoints 提前 thaw 的后续实现见文末；完整设计 27 仍未完成。


## 原生 checkpoint 到 CSI Snapshot 接线增量（2026-10-22）

- 公共 ledger schema 7 新增 `ssf_snapshot_captures`，在原生/PVC pin 已建立后保存不可变
  capture/head/FS UUID/容量及 PVC UID；记录校验真实 binding、run/epoch 与 checkpoint。
- AgentENV adapter 调用公共 Node API，全部 OverlayBD 参与卷先 PrepareCapture，再
  PublishCapture；失败保留同一身份重试，不调用 CSI Node 或控制原生 VMM。
- `KubernetesCaptureAuthority` 读取真实 Snapshot、Content、PVC，对照公共捕获记录
  验证 driver、handle、UID、Block 模式和来源。snapshotter metadata 仅用于定位对象，
  不授予捕获权限；固定 Snapshot/Content pin 后才允许返回 CSI handle。
- Driver 新增 CreateSnapshot/DeleteSnapshot/ListSnapshots，仅配置 authority 时协商能力。
  schema 5 固定 Kubernetes UID 唯一性与删除 tombstone；未知 ID 的删除也阻止迟到登记。
  注册和删除共用事务 advisory lock，clone 继续使用原固定 head 来源和引用保护。
- 新增真实临时 PostgreSQL + Kubernetes/backend fixture 验证捕获到 CSI 快照/clone/删除、
  捕获重放、UID/handle/driver/模式替换拒绝、PVC 同名重建拒绝、删除前置及并发竞争。
  fixture 不执行 ublk、原生 freeze 或 OverlayBD 数据读写。
- Controller 增加只读 `-probe`；清单渲染入口补齐 Controller 的账号、RBAC、mTLS worker
  配置、provisioner/snapshotter metadata 和 readiness。原节点示例 mTLS 证据端口修正为
  9444；9445 仍为原生 HTTP hook。清单未部署，完整节点 overlay/联合 drain 仍未完成。

剩余快照边界：空闲卷直接 K8S Snapshot 的后续实现见文末；活动卷经原生屏障的入口
仍需实现，当前保持 Pending。原生 checkpoint 分阶段时序的后续实现见文末。


后续保护与部署增量：

- CSI 接受快照前必须等待协调器持久固定首次 Snapshot UID；验证 Snapshot 与 Content
  同时被同名替换时也拒绝。sidecar 与协调器共用 Content UID 的持久 step，哪条路径
  先观察到对象都不能让后续路径改绑另一个 UID。
- `drivers/overlaybd/deploy/standalone` 提供 daemon/host/Node/worker/Controller 五个独立
  systemd 单元；保留 AgentENV 原服务，配置反向停止顺序，不以超时 SIGKILL 代替设备
  排空。仅交付单元和配置说明，systemd 校验、Linux 执行及完整联合 drain 未执行。

## 空闲卷捕获保护增量（2026-10-22）

- 公共账本 schema 8 增加独立 `ssf_idle_captures`。按规范 backend key、PVC UID 和请求
  摘要固定操作，Capturing 阻止新 writer/准备/维护，Ready 固定不可变回执并继续保护
  来源 PVC/后端，Released 保留删除 tombstone。未伪造 sandbox/run 或 VMM 停止事实。
- 初次进入与 Acquire 共用卷行锁，检查运行、暂停 hold、未释放 attachment、维护和
  Preparing/ContentReady/NeedsRecovery 状态；未知结果没有超时自动解锁。离线维护也
  补齐未释放准备状态检查。源卷回收授权与 PVC Permit 均检查存活的空闲捕获引用。
- OverlayBD 的 `IdleSnapshots.Capture` 在公共保护下调用原生 ResolveVolume，核对
  repository/native ID、head/content digest、FS UUID、容量和拓扑后持久保存 SnapshotPin。
  Ready 重试不联系后端、不读取新运行的 head；失败保留保护，不初始化空内容。
- 新增 PostgreSQL 测试覆盖 writer 并发、暂停 hold、规范身份/UID、未知准备、扩容、
  删除先于创建、迟到回执，以及捕获完成后已有新 writer 时的固定回执重放。
- consumer SQL 角色增加新捕获表、已有 snapshot capture 和 backend tombstone 权限。
  只读不变量工具增加捕获/使用冲突及来源回收检查，并用故意损坏的 DB fixture 验证告警。

该前置模块已在后续增量中接入直接空闲快照 fallback，见下一节。活动卷原生屏障
入口和真实设备验收仍未完成，不能表述为完整快照端到端完成。真实设备、Linux/KVM/ublk 与 Kubernetes 验收仍未执行。


## 直接空闲快照与 Guard 接线（2026-10-22）

- Controller 已接入直接 K8S VolumeSnapshot 的空闲卷路径，复用原生 sidecar 的标准
  metadata；校验实际 SnapshotClass、Snapshot、Content、PVC/PV、driver、handle、
  Block 模式和原生 finalizer。原生 checkpoint 缺少可信捕获时不能绕入空闲 fallback。
- 公共 ledger schema 9 增加 `ssf_direct_snapshots`，固定两类 K8S 对象 UID 和来源身份。
  Guard 识别后再读取原对象，关闭“读取 finalizer 后、保护登记前对象被替换”的窗口。
- `SnapshotControllerUsers` 独立配置允许移除 finalizer 的原生控制器身份；普通用户和
  仅有 CoordinatorUsers 身份不能移除。直接快照 DELETE 在原生 finalizer 下进入原生
  删除流程；公共引用/clone pin 未清除前，CSI 不得完成删除并解除来源保护。
- Controller 检查快照 webhook 的准确资源组、版本、操作和集群/命名空间范围，拒绝
  Fail-open 或 selector 缩窄规则；渲染 RBAC 已补齐 webhook 的只读 get。
- 后台恢复按持久对象 UID 处理未登记的取消请求。取消登记与 RegisterSnapshot 共用
  事务锁；若 Retain 快照成功登记先完成，取消不能删除该快照。已登记资源继续由原生
  snapshot-controller/CSI DeleteSnapshot 处理，来源引用在 Driver 删除后释放。
- 新增 PostgreSQL 集成覆盖直接空闲快照创建/重放/删除、来源回收保护、错误回执取消、
  原生对象删除后的后台排空及取消/登记并发。新增 Guard 测试覆盖 native finalizer 和
  专用身份边界；admission 测试覆盖错误 API group/version/scope、缺失操作和 selector。

此处通过的是 Go/数据库/协议 fixture 检查。没有执行真实 Kubernetes finalizer 时序、
真实 OverlayBD/ublk 数据读写或 microVM。活动/暂停/未知引用的卷仍不走空闲快照；
活动卷直接 K8S Snapshot 的原生屏障、完整恢复和扩容尚未完成；提前 thaw 的后续实现见下节。


## 封存后解冻与原生节点部署增量（2026-10-22）

公共 `consume.Checkpoints.Seal` 在所有原生工件、来源卷及参与卷稳定后持久记录封存许可。
OverlayBD 通过公共 Node PrepareCapture 封存；普通 CSI 卷必须等待原生 Snapshot/Content
Ready 并固定 UID。最终写封存记录前再次核验真实冻结证据，部分失败不发放 thaw 许可。
后续 Advance 使用同一 intent/binding 摘要，仅发布已封存内容，不重新捕获。

原生 snapshot 路径在 Seal 成功后由原 FirecrackerSandbox 执行 resume/thaw，随后等待上传、
Kubernetes 快照就绪和联合提交。暂停、Fork 的 capture_to_dir 等要求保持暂停的调用不被
提前恢复。解冻后上传失败保留工件/引用，并报告可恢复捕获失败；不再次 resume 已运行 VM。
节点 journal 持久保存协调器确认的 seal，重启后可接受同一 receipt 的发布重试；更换
barrier/digest、未确认 seal 或已停止的运行不能借此发起捕获。

新增数据库回归覆盖混合卷与全部托管卷、封存末次证据失效、部分捕获失败、thaw 后重试；
节点测试覆盖 seal 确认丢失与重启回放。相关数据库测试已通过；Rust Linux target 类型检查
及 daemon 目录保护改动的类型检查已通过。没有运行真实 freeze/ublk/VM。

节点部署增量与使用说明在 `sandbox-storage/drivers/overlaybd/deploy/README.md`。复用原
Kustomize overlay，加独立 daemon/宿主/worker/CSI/registrar，原节点 workload 保留。
配置 init 按真实 Node 名选择管理员后端 inventory，CSI 查询真实 Node UID。静态清单只
描述存储仓库，不替代原计算调度。Controller 可消费同一 inventory。

CSI Node 收到 SIGTERM 后进入 drain，保留清理 RPC，等待借用、会话和未发布捕获解除；
信号不成为真实停止证据。已有测试验证 drain 期间拒绝新接入、最终发布仍可完成。
部署结构离线检查通过；真实 Pod 退出顺序、强制终止与异常节点恢复尚未验证。

仍未完成：活动卷直接 K8S Snapshot 原生入口、旧 writable upper/设备重建、原生离线
扩容、完整故障及生命周期验收。因此完整实施状态仍为未完成。


## 静态受管卷登记与宿主校验（2026-10-22）

新增 `overlaybd-register` 管理员工具，读取真实 PVC/StorageClass，要求 Guard 固定 UID、profile
和 finalizer；仅通过已协商的 `static-registration-v1` 检查当前空闲原生卷，固定发布 head、
访问模式、容量和拓扑，再登记 Driver 身份并输出预绑定 Retain PV。无 Ensure/Create、
格式化、挂载和自动 apply。现有 legacy 卷不满足隔离受管 head 要求，不被自动接管。

重复登记保留原始 PVC UID 和 head；同名重建、第二个 PV 别名、旧宿主、未知 head、活动
原生引用及已删除身份拒绝。Go 单测、临时 PostgreSQL 集成和 Rust metadata 可执行测试
已通过；Linux 宿主全 target 类型检查及协议生成一致性已通过。实际静态 PV 绑定、
ublk 读写及 VM 生命周期仍未执行。Dockerfile 已包含登记工具和宿主构建所需 protobuf，
本机没有执行 Docker 镜像构建。

## 活动 Kubernetes 快照控制链路增量（2026-10-22）

新增公共 `ssf.control.v1/CaptureVolume` 方法及独立 storage peer 权限。OverlayBD Driver
只携带已被公共账本固定的 DirectSnapshot ID，AgentENV adapter 持久固定 request/run、
native ID、binding 和 barrier；原生 hook 经 StorageProvider 验证任务，调用原 Orchestrator
的 snapshot，未在 Driver/NodePlugin 中实现 VMM 冻结或新增沙箱控制面。

原生启动标记落盘后才执行；Snapshotting 状态内再次检查运行身份，避免与 resume 竞争。
重复请求优先恢复原 checkpoint，不能改用后续 idle head。封存后的发布可使用节点 journal
中的精确 receipt 继续；完整 checkpoint 的丢失响应可在原 run 结束后恢复。直接快照持有
独立 checkpoint 引用；派发前已有来源 PVC pin，取消/删除与晚到引用转移互斥。
来源 pin 是仅管理引用的记录，不创建 VM 工件或 Kubernetes Snapshot；退休时不进入
原生工件回收流程。

暂停卷增加受限 published-head 路径：保留 paused UseHold，要求其所有 run 已 StopRun，
所有 binding/attachment 已释放；公共卷行锁阻止捕获期间 resume。不放宽普通 IdleCapture
对未知 hold 的拒绝。新测试覆盖该互斥，验证结果以最新 PostgreSQL 报告为准。

本轮已有公共角色隔离、固定运行与完成结果重放、禁止 idle 回退的测试；原生 Linux target
类型检查和 SDK 启动标记/barrier scope 可执行测试通过。未执行真实 VM/ublk/K8s。
尚未封存就进程中断的直接快照仍保持 Pending，没有自动重建 barrier；完整未知结果恢复、
旧 writable upper 恢复和离线扩容仍有缺项，不能标记设计 27 全部完成。

## 停止后 upper 恢复与离线扩容增量（实施中）

停止后恢复现在支持：节点已核验真实停止及 FD 归还、原内核设备确已消失时，
以原 runtime image config 打开持久 upper，使用原 OverlayBD 引擎封存并发布；
不创建替代设备供旧 VM 使用。设备号复用、旧设备仍存在、旧 restack 结果未知时保持保护。
`NeedsRecovery` 会话只允许匹配原 finalization 的停止后捕获/发布/释放，不能重新借给 VM。
对应 Go 恢复测试已通过；实际 ublk/VM 重启测试未执行。

离线扩容新增以下基础实现，**尚未开放完整 CSI 扩容能力**：

- 公共账本把维护占用与规范后端、PVC UID、操作、源 head、容量固定，禁止同一维护任务并发派发第二个操作。
- 原生共享库新增维护专用容量/层引用 CAS，POSIX 使用原文件锁，OSS 使用原 ETag 条件写。普通 put_volume 继续禁止改容量。
- Driver 持久扩容输入并校验返回容量、代数、FS UUID 和拓扑；响应丢失后保持同一 operation。worker 要求宿主显式声明 offline-expansion-v1。
- 原生 daemon 新增独立维护请求，复用 runtime、ublk、ext4 工具和 restack；不制造 sandbox/run/RuntimeReady 或 VMM 停止证据。该路径正在编译和测试，尚未连通宿主最终 head 发布与 CSI Controller。

原生共享库 metadata 可执行测试共 17 项通过，包括新增 POSIX/OSS CAS 测试；
OSS 测试是内存条件写 fixture，不能视为真实 OSS 结果。PostgreSQL 集成报告通过，
包含维护授权固定和 Driver 扩容丢失响应测试。ext4 几何检查的首次离线运行因缓存缺少
alloca 失败，随后按现有 Cargo.lock 下载依赖并重跑；以实际完成的测试报告为准。

仍需完成：维护设备中断恢复、宿主容量 generation/head 发布、Controller/原生 PVC 扩容入口、
能力协商与 resizer 部署、相关回归，以及真实 Linux/KVM/ublk/Kubernetes/后端验收。

### 离线扩容主链路接入后的状态

上节列出的宿主 head/容量 generation、CSI Controller、PVC 原生入口和 resizer 渲染
现已接上。Controller 在解析源 head 前取得公共维护占用；Guard 对管理员明确声明
`controllerOfflineExpansion` 的 profile 仅放行增长意图，保持只读，不操作设备或写账本。
Driver 发布回执和目录成功后才完成维护，旧完成请求不能解除后续更大容量的维护任务。
容量按 GiB 向上取整，并保持 facade 原始请求与公共占用的一致性。

原生维护复用 runtime/ublk/restack 和 e2fsck/resize2fs。文件系统子进程通过启动 gate
在 PID/start/boot 落盘后执行；旧进程仍活着时不启动第二个工具，已退出的中断 resize
先重新检查文件系统。初始设备创建未知、daemon 重启后的维护设备、restack 结果未知
仍有恢复缺项；完整扩容故障验收没有完成。

已通过：扩容公共账本与 Driver 联合 PostgreSQL 回归、全量 Go 单测、原生 metadata
17 项可执行测试、ext4 几何 4 项可执行测试、宿主和 daemon 的 Linux target 类型检查、
6 项部署渲染单测、实际 Kustomize 渲染和原工作负载结构检查、108 路径补丁精确重放。
检查器首次使用默认 namespace 导致部署校验失败，改用实际测试 namespace 后通过。
真实 Linux/KVM/ublk/Kubernetes/共享文件系统/OSS 运行仍未执行。

### 原生证据工具与 restack 持久化边界补充

`native_pvc_e2e.py` 新增 native-overlaybd 模式；不再用 generic CSI profile 验证本方案。
A 写入、删除 A、B 读回过程中校验 PVC/PV UID、OverlayBD Driver/handle，并在 A、B
运行时采集专用 daemon 的原生会话、真实块设备和层 head。新增只读
`cmd/overlaybd-evidence`，限制输出字段，不导出 WAL 请求凭据；不向生产授权路径提供
替代证据。Dockerfile 包含该工具，preflight 和日志采集覆盖 OverlayBD 组件。
Go 证据单测、Linux 二进制交叉构建、11 项 Python 工具测试通过；真机执行未进行。

原生 OverlayBD 引擎新增通用 checkpointed restack 接口，复用原封层/重叠逻辑。
在镜像写锁释放前同步封存文件、新 upper 和目录，再由宿主原子持久化 runtime config；
落盘失败按原 restack terminal failure 处理，不能恢复 guest 写入。managed capture 与
扩容使用此接口，legacy 入口保持原语义。新增真实本地文件层测试已执行通过：持久
config 重新打开后保留封层前的数据和新 upper 写入；注入 checkpoint 失败后不能继续写。
这不是 ublk/KVM 验证，也没有消除封层/重命名内部各中断点的全部恢复缺项。

### 扩容封存恢复和失败现场保留

维护流程新增两个停止后恢复入口：e2fsck/resize2fs 已记录成功且原设备消失时，打开
原持久 upper 核验 UUID/目标容量并封层；已持久化 restack config 但未记录 sealed 回执时，
比对原层栈和唯一追加层，并只读打开封存层核验内容后恢复回执。两者都不创建替代 ublk
设备、不借给 VM、不重做文件系统初始化。工具尚未完成或内部 restack 结果未知仍受保护。

managed runtime 的创建/物化失败改为保留原目录，防止沿用 legacy 回滚删除恢复现场；
legacy 请求继续使用原有回滚语义。新增目录保留/旧目录拒绝重用及封存层来源校验测试。
daemon 全目标 Linux 类型检查覆盖这些测试，但本机未执行 Linux daemon 测试。

新的持久化边界增加 `durable-restack-v1` 显式协商：daemon→宿主→Node/扩容 worker
逐层验证。旧宿主的 capture/expansion 声明不足以通过新检查，不能静默降级。

### 本次续作：维护封层恢复与创建回执窗口

维护恢复在 restack 开始前持久固定原 upper 的设备号/inode 和原层栈。原端点消失、
文件系统工具已完成时：部分封层沿用原 RW index 重开；完整封层尚未改名时，以硬链接
保留原 inode；已改名则校验 sealed 文件与原 inode 一致，再用原引擎只读核验 ext4 UUID
和容量。不会覆盖已有输出，也不会删除中断时遗留的新 upper。macOS 原生文件层回归
包含完整/部分 trailer 的实际读写；Linux daemon 恢复测试当前只做目标类型检查。

原生设备插入管理表时即绑定 session/维护 operation。若返回或 WAL 写入失败，同一
存活 daemon 可从原句柄重新构造并持久化回执，不再依赖之后才设置的 managed_view。
重复 owner、容量或固定 runtime 路径不一致均拒绝；受管请求禁止借用 legacy warm pool。
这不解决 ADD_DEV 结果未知或 daemon 已重启的设备恢复，不能据此解除其排他保护。

续作中 Linux 类型检查发现遗漏 `File` 导入，以及 DashMap 引用解引用错误，均已修复并
重跑通过。最新检查以 `validation/overlaybd/daemon-cross/report.json` 为准；验证脚本
记录源文件哈希，历史通过结果若源文件改变，不等于最新版本已验证。

本轮补丁精确重放通过：109 个 AgentENV 路径。侵入统计相对记录的重构前补丁为
81 → 109 个路径（包含共享库迁移、生成代码和测试，不等同于原生核心生产文件数量）；
分类明细见 `validation/overlaybd/invasiveness.json`，本轮保存基线后的变更哈希见
`validation/overlaybd/changed-files.json`。新增故障工具范围回归后，Python 工具测试
14 项、部署渲染测试 6 项通过；原生 restack 文件层测试共 5 项通过。故障工具 dry-run
不运行真实场景，不能计为恢复通过。
