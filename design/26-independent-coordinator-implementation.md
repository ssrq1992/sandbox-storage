# AgentENV 最小侵入与独立协调服务实现

更新：2026-10-21。基于当前两个仓库增量实现；未提交、推送或部署。本轮使用全新测试环境，不迁移旧在线 run，不删除已有 DB/WAL/PVC。

## 1. 实际变化

- `sandbox-storage/cmd/ssf-coordinator` 是独立服务入口，组合公共 `coordinator.Runtime` 注册、Consumption、Preparation、Resolver、Controller 会话、Ledger 和 AgentENV adapter。公共 `coordinator` 包不导入 AgentENV 类型。
- PostgreSQL 保存唯一共享账本；Ledger 保留事务库形态。AgentENV 专用 run/reservation/template/volume 表和映射仍属于 adapter。Guard 保持独立进程及受限数据库角色。
- 原 Scheduler 的发现、心跳、资源过滤和策略不变。`services/scheduler/cmd/storage.go` 仅投影节点并调用标准库 Go 客户端 `ssf.dev/control`。不再导入 SSF 核心/PG/CSI，也不再使用 `go.ssf.mod` 或 `ssf` 构建标签。
- NodePlugin 的命令直连协调服务；原 Scheduler 的 SSF `StorageControl` RPC 已移除。standalone 同样运行独立本机协调服务，不再在 NodePlugin 内打开数据库。
- Rust 客户端及存储适配实现位于 `sandbox-storage/adapter/agentenvadapter/runtime`。原 `src/sandbox/storage/mod.rs` 为宿主 bridge；模块重导出保留原调用点及持久化格式。外部 crate 不引用 AgentENV 私有类型。
- 原 Gateway `/proxy` 路径修复、Pausing/resume 等待、guest 最后匹配 mount 核验、节点镜像拉取策略和 `/livez` 启动解环保留。SSF 的 300s Gateway 超时移到框架 overlay，原 base 回到 90s。

## 2. 内部协议与信任边界

`ssf.control.v1` 是 HTTPS JSON 协议；使用 TLS 1.3 双向证书和 URI SAN 角色白名单。它不新增公开沙箱 API。线类型定义在独立模块 `sdk/go/control/control.go`，请求/响应都有 `version`；未知版本或方法拒绝。协议端点：`POST /ssf.control.v1/<Method>`。

| Method | 调用方 | 行为 |
| --- | --- | --- |
| ResolveLaunch | 原 Scheduler | 验证原 Gateway 签名和原 body digest；持久 request/owner/run；通过公共 Step 固定模板、卷、clone 引用；返回资源和存储可行候选，不取得计算预留 |
| PrepareLaunch | 原 Scheduler | 只接受选择的节点；重新核对原健康投影及实际 Pod/Node/boot；在唯一原生容量表上持久预留；WFFC/准备/Acquire/Attach/签名 dispatch |
| InspectOperation | 原 Scheduler | 同一已认证 launch 的状态、已固定节点或完整原 dispatch；用于恢复未知结果 |
| InvokeLifecycle | NodePlugin | 保留原签名 node envelope、owner/run/证据核验；恢复、Fork、compute、发布、删除及卷 facade |
| LocalLaunch | standalone NodePlugin | 固定本机 placement，使用相同 consumer 和账本；只有配置 standalone 的协调服务开放 |
| ObserveNodes | 协调服务 → 原 Scheduler | 独立只读 9447 mTLS listener；从原 registry 返回当前发现且心跳 READY 的节点，绝不选择节点或写账本 |

Gateway、node、dispatch 签名密钥保持分离。Scheduler 只持有自己的 TLS 身份；协调服务持有计划/dispatch 签名权限。Node 收到的仍是完整原签名计划，公开请求私有 header 继续清理。

创建服务不会自行改选计算节点。`capacity` 错误仅在数据库明确尚无预留时返回，Scheduler 才能用原策略重选。连接断开、超时或 Attach 结果未知只检查原 launch，保留已有预留。已持久化 launch 的原签名过期时，只在 Scheduler mTLS 身份下核对原签名及完整不可变 intent 后恢复；过期证明不能创建新 launch。多副本共享原有 operation/capacity/使用权事务锁。准备后台调谐仍由 `RecoverNative` 承载，变更的是宿主进程。

## 3. Rust 边界和不变的安全时序

| 外移到框架 crate | AgentENV 保留的宿主责任 |
| --- | --- |
| loopback HTTP、token、序列化、请求上下文 | 原 API 鉴权后插入通用 request scope |
| outbox 实现、事件交付和恢复扫描 | 在 spawn、Ready、freeze/thaw、stop、删除等原生副作用边界调用并等待持久化 |
| PreparedExternalDrives 会话、capture 等待 | Firecracker/jailer 实际设备接入；设备路径、major/minor、容量及挂载路径校验 |
| guest mount/retain 命令策略 | 通过原受限 guest executor 传递 argv，保留冻结/恢复先后顺序 |
| publication/delete 参与接口、GC 协商 | 原 SnapshotManager/VolumeManager 元数据及工件操作；仅删除原生 managed-snapshots 下合法实例目录 |
| 模板替换的 layout/retained 策略 | 原盘位、只读槽位、隐藏保留克隆和旧 FD 语义 |

外移 crate 仍链接进原 Node 进程，未将关键落盘改成远程异步通知。停止证据仍来自原进程身份及 Node 独立核验；超时不视为停止。native memory/rootfs、legacy 与 CSI 卷继续走同一冻结屏障。

startup recorder 不再因为存在 storage context 被整项过滤。它通过既有 compute admission 使用新的 owner/run；有受管卷时必须有联合 checkpoint，并从 checkpoint 获得独立卷。原 recorder 的可选、best-effort 语义及清理逻辑保留。缺少 checkpoint 或复用源 writer 身份时拒绝该次 recording。

## 4. 构建和部署

两个仓库应保持兄弟目录，路径为 `AgentENV`、`sandbox-storage`。Go 普通构建直接使用轻量模块；Rust 锁文件包括外部 adapter crate。联合 Dockerfile 和 overlay 均在框架仓库。详见 [13](13-agentenvadapter-installation-and-deployment.md)。

协调服务是新 Deployment/Service，默认 2 副本，探针和 45s drain；与原 Scheduler 相互调用的只读 observer 不参与 readiness 启动依赖。退出停止新创建准入，保留已持久化未知工作；完成在途请求后停止恢复循环和数据库连接。原 Node sidecar/PID/mount/WAL/drain 不变。

本轮不支持与旧内嵌组件混用。NodePlugin 遇到 `scheduler_endpoint` 或 `standalone_config` 明确拒绝；使用新的 `coordinator` mTLS 配置。计划使用全新环境，旧环境资源由操作者保留。

## 5. 验证和交付证据

本轮结果目录：[validation/control-service](../sandbox-storage/validation/control-service)。最终状态汇总及侵入量报告见该目录 `summary.json`、`invasiveness.json`；本机证据不表示用户集群已完成验收。

最终本机验证结果如下，完整日志和源码哈希在各报告目录。没有最终失败项。

| 检查 | 最终状态 | 证据与范围 |
| --- | --- | --- |
| SSF Go 单测 / race | 通过，含跳过项 | 每轮 68 个顶层测试通过、37 个跳过；其中 36 个已由独立 PG 运行补测，剩余 1 个需真实 K8S |
| AgentENV Go 单测 / race | 通过，含跳过项 | 每轮 232 个顶层测试通过；3 个原生 Redis 测试因无 `redis-server` 未执行 |
| 独立 PostgreSQL / race | 通过 | 72 个顶层测试，无跳过；包括多协调器重放、固定选点、未知结果、容量和 writer/hold/引用保护；节点/CSI 为 fixture |
| Go vet / Linux build / 依赖边界 | 通过 | 双仓普通 Go 构建；Scheduler 无 SSF 核心/PG/CSI 依赖，控制 SDK 仅标准库 |
| mTLS 客户端 / 工具契约 | 通过 | 本机证书握手与角色/版本测试；9 项 Python 工具测试 |
| Rust 外部 adapter 可执行单测 | 通过 | macOS 上 9 项；设备槽位、模板替换、工件日志和 guest argv 策略 |
| Rust Linux 全目标类型检查 | 通过 | `cargo check --locked --all-targets`，源码稳定；未执行 Linux 原生测试或 KVM |
| 协议生成 / OpenAPI 兼容 | 通过 | 现有 protobuf 再生成一致；原公开 API 兼容检查 |
| 部署离线渲染 | 通过 | 两个 namespace，原工作负载保留、协调服务与权限边界、凭据分离 |
| AgentENV 补丁重放 | 通过 | 相对原版精确重放 64 个改动路径；内容及可执行位一致 |
| Linux 原生测试 / Docker / systemd / KVM / K8S / 真实 CSI | 未执行 | 交付入口及 [14](14-agentenvadapter-real-environment-test-plan.md) 矩阵；不由 fixture 或类型检查替代 |

`foundation/report.json` 未直接启动 PG 或 Rust，因此其相应项为未执行；[总报告](../sandbox-storage/validation/control-service/summary.json) 关联 `database`、`runtime`、`rust-linux` 的独立结果。最终 Go 与 Rust 检查的源码哈希均与交付代码一致。

- Go 全量单测及独立 PostgreSQL 集成已执行，覆盖迁移重放、共享容量、暂停/恢复、writer/引用、Resolve 幂等、并发重放和未知结果固定节点。
- 轻量客户端执行真实本机 mTLS 握手测试；公共 handler 验证角色分离、版本拒绝、drain 允许清理。
- 原 Scheduler 测试原策略调用、明确容量失败重选、未知 Prepare 只 Inspect 同一个 launch。
- Rust adapter crate 运行可执行单测；Linux 目标 `cargo check --all-targets` 结果单独记录，不替代 Linux 可执行测试。
- 原 OpenAPI 兼容、现有 Node/原生 protobuf 生成、依赖边界、两个 namespace 部署渲染、补丁重放都有检查入口。
- Linux 原生 Rust 测试、KVM 实际启动、systemd/Docker 镜像运行、Kubernetes/真实 CSI、HTTP/WS/P2P/recorder 真机语义未在本机执行。交付 [14](14-agentenvadapter-real-environment-test-plan.md) 的操作和证据要求。

## 6. 必要侵入点

原 Gateway 的请求关联和可信转发不可用客户私有 header 替代；原 Scheduler 选择策略及 health registry 是唯一计算选点入口。节点 hook 必须位于 VMM 副作用与冻结屏障内，普通 custom extension 无法提供这类可靠顺序。设备边界和最终本地工件删除必须由原生宿主验证。持久 metadata/manifest 中的通用引用和稳定盘位必须跟随暂停、模板和 Fork。

因此本轮主要减少原仓库内独立实现与依赖数量，不通过移除上述必要边界来追求文件数量。生产代码、独立模块、生成代码、测试、部署和依赖分别统计；保留用户部署修复，不按来源不明的 diff 直接回滚。

## 7. 原生侵入量前后对照

均相对 AgentENV `6cccaa78`，包含用户已有修复；独立测试文件单列，原生产文件内嵌测试仍计入原有核心。下表是 diff 行数，不是新功能行数。

| 分类 | 改前文件 | 改后文件 | 改前新增/删除行 | 改后新增/删除行 |
| --- | ---: | ---: | ---: | ---: |
| 原有核心（含内嵌测试） | 38 | 38 | +2907 / −294 | +2842 / −293 |
| 新增独立集成模块/bridge | 15 | 8 | +2576 / −0 | +900 / −0 |
| 生成代码 | 5 | 5 | +936 / −99 | +712 / −75 |
| 独立测试文件 | 8 | 8 | +651 / −67 | +661 / −67 |
| 依赖/配置 | 3 | 4 | +280 / −0 | +27 / −0 |
| SSF 部署增量 | 11 | 0 | +360 / −1 | +0 / −0 |
| SDK/CLI | 1 | 1 | +61 / −5 | +61 / −5 |
| 合计 | 81 | 64 | +7771 / −466 | +5203 / −440 |

主要减少的是独立集成实现、依赖和部署在原仓库中的负担。原有核心改动文件仍为 38 个，新增行由 2,907 降到 2,842；必须位于生命周期/设备/持久化边界的 hook 继续保留。框架代码外移不等于这些职责消失，也不等于真实环境已经验收。
