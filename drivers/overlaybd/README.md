# AgentENV OverlayBD Driver

设计依据是工作区 `design/27-agentenv-overlaybd-csi-reuse-design.md`。
**当前仍是增量实现，不能部署为完整可用的 OverlayBD CSI Driver。**
Controller、Node、原生宿主、活动快照和离线扩容主链路已经接入；设备创建及 restack
中断恢复仍有缺项，完整生命周期与真实环境验收尚未完成。不能以 generic CSI 结果替代。

## 已实现的边界

- `journal.go`、`catalog.go`：独立 PostgreSQL schema，固定创建名称、native ID、输入摘要、
  规范 handle、初始内容和拓扑；已删除登记不能被旧创建回执复活。
- `csi_create.go`、`csi_delete.go`、`csi_attachment.go`、`csi_controller.go`：
  标准 Controller 创建/删除、Attach/Detach、能力校验和注册函数。创建复用原生 worker；
  Attach 固定已发布 head；删除调用公共账本回收屏障。Linux 服务入口为 `cmd/overlaybd-csi`，支持 controller/node/worker 三种角色；增量部署见 `deploy/README.md`，未执行真机部署。
- `sessions.go`、`node_operations.go`、`node_capture.go`、`node_release.go`：
  Node 设备会话、同操作 Inspect 恢复、捕获和条件发布、最终内容与释放证据固定。
- `csi_node.go`、`block_linux.go`：标准 Stage/Publish/Unpublish/Unstage、真实 Block bind mount、
  按签名 binding 隔离 stage、持久路径 claim；设备发布前验证 head/FS UUID/容量/boot。
- `node_lifecycle.go`、`node_finalize.go`：公共 Node 通过 `ManagedStorageDriver` 在 CSI 清理前
  完成准备释放或运行停止后的最终 capture/publication；响应丢失恢复固定同一个 capture。
  该扩展只允许私有 CSI 端点访问；运行时停止证据还会向已配置的 mTLS verifier 独立核验。
- OverlayBD profile 禁止 generic 格式化，使用 ControllerPublish 固定的已发布 FS UUID。
  Linux bind mount 与端到端 VM 路径仍未执行，联合快照等生命周期实现仍未补齐。
- `native`：版本化 protobuf/gRPC over UDS 宿主可执行入口。POSIX 实例复用 AgentENV
  `aenv-volume-core`，调用原生专属 daemon 的 Prepare/Inspect/Capture/Release。
  主机进程退出不会通过客户端析构杀掉 daemon。宿主已支持 POSIX/OSS 配置工厂，
  `ExpandVolume` 已接入维护设备、ext4 核验、容量 generation 和 head 发布；
  Controller 通过 `offlineExpansion` 显式启用，要求宿主协商 `offline-expansion-v1`。
- 原生共享库包含原卷引擎、POSIX/OSS catalog、层发布和物化算法，以及新增持久操作日志、
  不可变 head 与条件发布。原 SnapshotManager 仍管理 memory/rootfs、模板和原生工件。

SSF 公共账本拥有使用权、暂停 hold、维护和回收授权；Driver 自己的表不能签发 writer。
设备回执中的 FS UUID 固定于已发布 head；不能将其当作已完成 guest 或设备内容核验。

## 安全行为

未知创建只检查原操作；仅身份完全匹配且明确表示尚未执行的 Pending 回执，才允许使用
同一操作继续 Ensure。没有回执、超时、旧设备编号和旧 daemon 实例都不能触发新 formatter。

准备会话不伪造 VMM stop。可写运行会话释放必须绑定停止后的最终 capture 和已发布 head；
释放请求先落盘，结果未知时保留屏障。释放开始后禁止新 capture、publication 和接纳。
原生 daemon 重启后的旧会话目前保留 NeedsRecovery，**完整离线 upper 恢复尚未实现**。

## 本机检查与 Linux 入口

```bash
go test -race ./drivers/overlaybd ./ledger ./gateway
go vet ./drivers/overlaybd ./ledger ./gateway
python3 scripts/check_generated.py
```

独立 PostgreSQL：`scripts/native_database_test.py --postgres-bin <本机PG/bin> --output <报告目录>`。
没有 `SSF_TEST_DATABASE_URL` 时，直接 `go test` 会跳过数据库项目。
数据库测试用真实临时 PostgreSQL 和协议 fixture；不会运行 OverlayBD、ublk 或 VM。

Linux 完整依赖环境：`scripts/check_agentenv_linux.sh` 已包含共享库 content 测试、
daemon 库测试、宿主 device-host 类型/可执行构建入口。真实设备/VM 测试应使用
AgentENV 既有 capability runner 和独立测试状态目录，不使用在线实例状态。

宿主构建：

```bash
cargo build --locked --manifest-path drivers/overlaybd/native/Cargo.toml \
  --features device-host --bin ssf-overlaybd-native
```

宿主参数是一个 JSON 配置文件，结构见 `native/config.posix.example.json`。该文件只是
配置格式示例，**不是已验证的部署方案**：需要专属 daemon、原生 OverlayBD 配置和私有
0700 UDS 目录；本地 POSIX 设置真实节点拓扑，共享 POSIX 使用空的 accessible_nodes。
repository 必须与 legacy repository 隔离。不得将示例直接套入在线环境。

最新实现缺项和验证状态见工作区 `design/28-overlaybd-csi-implementation-status.md`。

Linux Go Driver 入口：

```bash
go build -o /tmp/overlaybd-csi ./cmd/overlaybd-csi
/tmp/overlaybd-csi -config /etc/ssf-overlaybd/node.json
```

`node.config.example.json` 和 `controller.config.example.json` 仅说明配置格式，不能视为
完整部署。Node 的 `root` 必须与公共 SSF Node 完全一致；`nodeUID` 来自真实 K8S Node，
`csiNode` 必须与宿主 `node_id` 一致，boot ID 直接读取本机内核。`evidenceURL` 应指向
该部署真实的 adapter verifier 路径，证书必须满足其 mTLS 授权，不能原样照抄示例地址。
Controller 示例的空 `nodes` **仅适用于真实共享 repository**；本地 repository 必须列出
唯一真实节点，且其供卷 worker 确实位于该节点。跨节点使用 `worker` 角色及 Controller repository 的 `endpoint` 字段（与 `socket` 恰好选一个）。
CSI socket 目录权限必须为 0700，socket 为 0600；拥有它就具有节点控制权限，禁止开放
未认证 TCP 或向普通工作负载共享。启动过程不会部署集群资源或修改旧 StorageClass。

OSS 宿主配置格式见 `native/config.oss.example.json`。OSS 传输直接使用从原 AgentENV
抽出的共享 `OssClient`，保留凭据刷新、条件写、multipart 上传和监控指标。
支持显式静态凭据文件或原生 `credential_process`，可选 security token 与寻址方式；
必须使用与 legacy 隔离的非空 prefix。OverlayBD 全局配置也必须配置同一个后端及
其读取凭据，因为数据 I/O 仍由原生引擎执行。真实 OSS 验证尚未执行。

`worker.config.example.json` 是本地卷供卷 worker 的配置结构；部署时需使用真实证书
URI SAN、controller peer allowlist 和该节点的专属 repository ID。Controller 的远程
repository 配置使用 `endpoint: "<worker-certificate-DNS>:9455"` 和唯一 `nodes`，并配置
客户端证书/CA。worker 只转发 Ensure/Inspect/Resolve/Delete；Node 设备 RPC 不对该
控制通道开放。私有 UDS worker 与 mTLS 远程 worker 共用原生持久操作协议。

双仓镜像构建入口（Docker 构建尚未执行）：

```bash
# 在 sandboxstorage 目录执行，且两个仓库都必须是当前工作区版本。
docker build -f sandbox-storage/drivers/overlaybd/Dockerfile \
  -t ssf-overlaybd:workspace .
```

该镜像包含 Go Driver、Rust 宿主和专属 daemon 二进制。它不包含擅自终止 daemon 的
多进程 shell supervisor；部署必须显式管理进程/容器及退出顺序。完整 Pod overlay 和
联合 drain 尚待交付，不能仅凭镜像构建入口就认为部署已完成。


### 快照来源与查询的当前边界

Driver schema 5 持久保存快照、删除 tombstone 与 clone 引用，源卷删除和快照注册通过同一行锁互斥。
`CreateVolume` 可从已经可信登记的不可变快照恢复；同一请求的未知结果持有来源 pin，
目标 head 发布及登记完成后才释放。删除来源后的成功请求重试复用持久结果。
原生 checkpoint 已接通公共 Node 捕获及 CSI CreateSnapshot/DeleteSnapshot/ListSnapshots；
仅配置可信 KubernetesCaptureAuthority 时广告快照能力。直接由用户创建的 K8S Snapshot 已接通空闲卷 published head 路径；活动卷原生屏障
协调仍未完成，当前返回 Pending，不应将这一部分称为完整快照支持。

ControllerGetVolume/ListVolumes 已实现，包括分页、发布节点和原始 content source。
查询数据来自 Driver catalog，不通过查询操作触发设备创建或 native 内容准备。


### 分阶段捕获和后台恢复

Node 显式协商 `ssf.dev/managed-capture=v1`，通过公共 Node 转发的 PrepareCapture /
InspectCapture / PublishCapture 分开冻结内捕获与后续发布。捕获意图、稳定回执、发布
结果均持久化；新屏障不能越过未知捕获。只读捕获固定已有 head，不发布新 head。

节点配置必须包含 mTLS `captureEvidenceURL`（AgentENV 为 `/v1/capture/evidence`）。
首次捕获独立验证实时 native 冻结证据；已持久捕获的重试不要求 VMM 再次冻结。
后台恢复只继续已捕获结果的发布及已授权释放，不启动新设备，不猜测旧 VMM 已停。

Coordinator 的原生 checkpoint 已调用这组 RPC，先捕获全部参与卷，再发布并固定公共
账本记录；CSI Snapshot 核验真实 K8S UID 后消费该记录。当前仍在整个发布期间保留
原生冻结屏障；捕获全部稳定后提前 thaw 的时序尚未完成。
旧 daemon/upper 的完整中断恢复和部署验收仍未完成；扩容主链路见文末，真实设备/VM 测试未执行。


### Controller 清单渲染与只读探针

`scripts/render_overlaybd_controller.py` 输出 Controller Deployment、配置、独立 ServiceAccount
和 RBAC，不访问集群。必须显式选择本地节点或真实共享 repository。例如：

```bash
python3 scripts/render_overlaybd_controller.py \
  --namespace ssf-overlaybd-test --image ssf-overlaybd:workspace \
  --repository test-node-a --worker-endpoint worker.test.example:9455 \
  --local-node node-a > /tmp/overlaybd-controller.json
```

worker 地址必须可解析且匹配其 TLS 证书；上述名称是占位示例。外部前置依赖包括：原生
worker、PostgreSQL、Snapshot CRD/controller、`ssf-overlaybd-database` Secret 的
`database-url` 文件，以及 `ssf-overlaybd-controller-tls` Secret 的 `tls.crt/tls.key/ca.crt`。
不创建 Namespace、不包含凭据、不修改 StorageClass。集群级 RBAC 需要管理员预先审查；
隔离 namespace 不能替代 cluster-scoped 资源的授权边界。本轮仅渲染，未 apply。

provisioner 固定 v5.2.0，snapshotter 固定 v8.2.0，两者携带 `--extra-create-metadata`。
沿用仓库的固定版本，并不代表已完成真实 K8S 兼容验证。Attach 继续由 SSF Gateway
和账本控制，不新增另一套 external-attacher。resizer 仅随显式离线扩容配置添加，恢复与真机验证状态见文末。

Driver readiness 使用 `overlaybd-csi -config <配置> -probe`，只调用本地 CSI Identity.Probe，
不会执行迁移、启动后端或操作设备。Node verifier 示例端口已修正为 TLS 的 9444；9445
是原生 hook 的明文 loopback 端口，不能用于 mTLS evidence 请求。

节点 Pod 增量、worker inventory 和 drain 已增加，见 [部署材料](deploy/README.md)。
本机只完成离线结构检查，真实退出顺序和异常恢复仍未验证。


Standalone 的独立 systemd 单元见 `deploy/standalone/README.md`。已定义进程及停止顺序，
但未在 Linux 执行，也未完成联合 drain 验收；不能把单元文件交付视为整个部署完成。

### 空闲卷捕获前置模块

公共账本 schema 8 增加 `ssf_idle_captures`，区分 Capturing、Ready、Released。捕获中阻止
writer、其他捕获及维护进入；Ready 保留来源引用但允许新运行，Released 永久阻止迟到
创建和回执复活。运行、暂停 hold、未知 attachment、未释放准备状态都不能走空闲捕获。
此操作没有 sandbox/run，不用伪造 VMM stop 或冻结证据。

`idle_snapshot.go` 在该保护下读取原生已发布 head，核对后端身份、FS UUID、容量、拓扑
及内容摘要，并先持久固定回执。重试只返回既有不可变回执，即使新运行已经启动，也不
重新查询当前 head。错误不会退回空卷，也不会自动解除未知捕获的保护。

直接 Kubernetes Snapshot 的空闲卷 fallback 已接入 Controller，具体保护与配置见下节。
活动卷原生屏障仍须完成；活动/暂停/未知引用不会退回读取 published head。
当前完整 checkpoint 路径继续使用原生捕获协议；此模块不替代活动 VM 的冻结屏障。

schema 升级后重新应用 `deploy/ledger-roles.sql`，以补齐 consumer 对捕获和 backend
删除 tombstone 表的权限。旧服务不识别 schema 8 时会明确拒绝，不能混用旧执行版本。
`native_invariants.py` 新增空闲捕获与使用冲突、缺失回执、回收引用冲突的只读检查。


### 直接 Kubernetes 空闲快照

Controller 现在可处理原生 VolumeSnapshot → CSI CreateSnapshot 的空闲卷路径。须使用
显式 VolumeSnapshotClass，driver 为 `overlaybd.csi.ssf.dev`，来源为已 Bound 的受管
Block PVC；保留原生 Snapshot/Content finalizer。SnapshotClass 的 Delete/Retain 策略
均保留原生语义。用户无需伪造 SSF capture annotation 或调用额外登记接口。

公共 ledger schema 9 固定直接快照的 Snapshot UID、Content UID、来源 PVC/PV 及请求
身份。Guard 可立即识别这些记录；Controller 安装记录后再次读取对象，确认原生
finalizer、UID 和 spec 未发生替换，之后才进入空闲捕获。后台恢复只取消尚未登记且
原 K8S 对象正在删除/已消失的只读捕获；取消与成功登记共用锁，不删除已登记的 Retain
快照。源引用在 Driver 快照删除后才释放，不能在收到 DELETE 请求时提前释放。

Guard 新配置 `SnapshotControllerUsers` 必须填写真实 snapshot-controller 与 CSI
snapshotter 的 ServiceAccount 身份。该配置独立于 `CoordinatorUsers`，不会授予 PVC
协调权限。示例中的 namespace/account 需按实际部署替换；普通用户即使有 update 权限
也不能移除受管快照 finalizer。直接快照允许在原生 finalizer 保护下接收 DELETE，真正
CSI 删除仍检查公共引用和 clone pin；Snapshot/Content 任一引用都会保护这对对象。

Controller 每次解析快照都检查 `ssf-guard` 的快照 API group/version、CREATE/UPDATE/DELETE、
Namespaced/Cluster 范围、Fail 策略以及未被 selector 缩窄。渲染器已给 Controller 增加
只读访问该 webhook 配置的最小 RBAC。旧 schema/Guard 与新 Controller 不可混用。

原生 finalizer 名称及 metadata 参数采用
[external-snapshotter v8.2.0 官方定义](https://github.com/kubernetes-csi/external-snapshotter/blob/v8.2.0/pkg/utils/util.go)。
本机验证使用真实 PostgreSQL、fake Kubernetes API 和 native backend fixture；真实
Kubernetes finalizer 时序、OverlayBD 数据内容和 VM 路径尚未运行。活动卷直接快照、
旧 upper 的未知中断恢复仍有缺项，扩容主链路已接入。原生 snapshot 已接入 Seal→原生 thaw→Publish：
普通 CSI 参与卷先等待稳定 Snapshot，OverlayBD 卷先封存，所有参与卷固定后才允许解冻。
真实 VM 时序尚未执行。


最新增量：静态 PV 登记工具 `overlaybd-register`、活动/暂停/空闲 K8s 直接快照链路、
封存后 thaw、Node drain 和原工作负载增量 overlay 已加入。详见 `deploy/README.md` 与
设计 28 的最新状态。封存前中断的未知捕获、旧 upper 和扩容的完整中断恢复仍未闭合，
不将上述代码及本机回归视为完整端到端验收。

## 离线扩容主链路（恢复验收尚未完成）

新增 `expand.go`、`csi_expand.go`、`expand_catalog.go`：PVC 请求经标准 resizer 进入
Controller，先取得公共账本维护占用，再固定源 head 和 operation。活动 writer、
paused hold、准备与 attachment 阻止扩容。原生宿主使用独立维护请求，不制造 sandbox run。
维护设备复用原 runtime/ublk/restack，执行 e2fsck/resize2fs，核验 UUID/容量并释放设备；
宿主持久发布新 head 和容量代数后，Driver 更新目录、完成维护并返回 NodeExpansionRequired=false。

部署启用需要 inventory 的 offline_expansion、原工具只读 volume、Controller 的
offlineExpansion、StorageClass allowVolumeExpansion 与 Guard/Coordinator 一致的 profile。
未开启时继续原有行为。Guard 只放行容量意图，不写账本、不操作设备；独占检查由 Controller
调用公共账本实现。已在途的有状态操作不会因 RPC 超时更换 identity。

文件系统工具启动前固定 PID/start/boot，并通过 stdin gate 等待该身份落盘；仍存活的
旧进程阻止重试。已退出的 resize 先恢复检查再继续，不能格式化。daemon 重启后维护设备
恢复、初始创建和未知 restack 的完整恢复仍需补齐；不能据此宣称全部完成或真机通过。


维护封存恢复目前可继续“文件系统工具均已成功、原设备消失”的原 upper，以及
“restack config 已落盘、sealed 回执未落盘”的原封存层。恢复固定原维护操作和源层栈，
核验目标容量与 FS UUID，不重建 VM 设备。managed 物化/创建失败保留原目录；legacy
仍按原逻辑回滚。首次设备创建未知、工具中断后设备丢失和内部 restack 未落盘的情况
尚未全部恢复，不能删除 WAL 或另起 operation 绕过。
