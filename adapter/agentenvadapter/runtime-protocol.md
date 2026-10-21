# AgentENV 原生存储内部协议

公共沙箱 API 不承载内部凭据。Gateway 剥离所有客户 `x-agentenv-storage-*` 字段，用角色密钥签名原 body digest、request ID 与创建上下文；Scheduler 校验后经原调度生成完整 NativeDispatch。节点绑定 native node ID、Kubernetes Node UID、物理 boot ID、owner/run、资源、模板 pin、原 body digest、完整 SignedPlan、初始化与扩容授权。序列化及字段以 `types.go`、`native_control.go`、`auth/envelope.go` 和原生 `sandbox/storage/mod.rs` 为准。

协商版本：公共 `ssf.runtime.v2`；原生 `agentenv.storage.v2`；hook `3`；完整 profile `agentenv-capture-clone-v1`。旧/未知 hook、超出 256KiB 的完整计划、不匹配的签名/身份/请求体、缺少能力均拒绝准入。Ready 必须关联本 run 的真实 guest 设备证明；Stopped 需要真实进程与设备释放证明。重复事件幂等，旧 epoch 不能提交新 run。

| 边界 | 认证与调用 |
| --- | --- |
| 原 Gateway → 原 Scheduler | 原内部服务链路与 storage 角色签名；不信任外部同名 header |
| ControlPlugin → NodePlugin | mTLS，独立 scheduler URI 身份、完整签名 dispatch |
| 原生节点 → 本 Pod NodePlugin | loopback HTTP + 独立 Bearer hook-token，持久事件/outbox |
| NodePlugin → 公共 SSF Node | mTLS，限定 node 身份、plan 公钥验证 |
| Driver | 本机标准 CSI UDS；管理操作仍由 Kubernetes sidecars 发起 |

本地端点 `/storage/admit` 承接已签名创建；standalone 使用 `/storage/local-admit` 原 body；`/storage/compute` 为 builder/recorder 独立准入；`/storage/volumes` 为 provider 管理；`/storage/checkpoints` 为联合捕获；`/storage/control` 转发 pause、resume、fork、delete、capture.retire、cleanup.status 等生命周期命令。具体 action 白名单按代码校验，端点不是新的公开沙箱 API。

原节点认证内部只读投影 `/internal/storage/defaults`、`/internal/storage/capacity`、模板元数据复用原管理器。capacity 的总额与 sandbox IDs 来自同一原生元数据锁快照，不能将不同时间的路由 ID 和新资源总额拼接。普通与 SSF 构建、协议生成、公开 OpenAPI 兼容及 Rust 绑定生成均有交付检查入口。

模板显式替换通过 `replace_mounts`、`relocations` 与持久槽位布局传递通用转换计划。所有旧捕获设备在原槽位保持独立克隆，隐藏到 `/.agentenv-storage-retained/`，新挂载使用空槽。不得复用父 writer 或将旧内存绑定到无关磁盘。GC 只在所有引用与运行已终结后释放私有卷及原生捕获目录。

### 离线扩容

原生 Volume API 新增 `POST /volumes/{volumeID}/expand`，请求 `{"sizeMB":2048}`。只支持 SSF 可写 Block/ext4，拒绝缩容；legacy 返回 501。202 表示持久请求尚未完成，200 表示 Controller 扩容完成，ext4 扩容在下次 VMM 启动前完成。StorageClass 必须允许扩容，当前接入要求 CSI Node 支持 NodeExpandVolume。

原生卷描述分开记录 `expansionRequested`（待处理目标）和 `expansionTarget`（已进入维护的设备/文件系统目标）。活跃绑定、暂停 UseHold 或未分离 attachment 存在时，不修改 PVC；待处理请求不阻止原 owner 暂停/恢复。公共 `ssf_volume_maintenance` 与 Acquire 共用物理卷行锁，在真正修改 PVC 前阻止新绑定和回收许可，无超时自动解锁。Controller 的 PV 容量与 PVC/PV UID 校验通过后解锁，原生描述中的恢复状态继续保留到完成。

控制面在取得消费绑定后重新读取扩容目标，随完整签名 NativeDispatch 下发 binding→bytes。NodePlugin 串行处理同一 operation，先调用公共 NodeExpandVolume，再重新验证设备路径/major/minor/容量、校验 ext4 UUID、检查设备没有宿主挂载，执行 `e2fsck -p` 和 `resize2fs`。不调用 mkfs 执行扩容；修复需要人工介入、容量不足、只读卷、已派发 VMM 或身份变化均阻止启动。WAL 保留目标，崩溃后重复扩容仍使用同一设备和文件系统身份。

### Standalone

原生 `AENV_STORAGE_PROVIDER_STANDALONE=true` 仅改变准入来源：认证后的原创建 body 原样交给本机 `/storage/local-admit`，客户提供的私有 storage headers 全部剥离。本机插件加载 `standalone_config`，复用 NativeControl、持久预留、数据库与 NodePlugin，固定一个实际宿主 placement；不增加公开沙箱 API。宿主服务配置见 `deploy/standalone/README.md`。集群模式继续使用原 Gateway/Scheduler，不使用本机创建入口。
