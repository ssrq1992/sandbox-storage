# AgentENV 原生 standalone 存储接入

保留现有 `agentenv.service` 的二进制、启动参数、监听和 API Key。仅给原服务加环境变量 drop-in，新增 `agentenv-storage-node`、`ssf-node` 两个宿主服务。没有公开的替代沙箱 API，也没有 workers/templates 清单。创建、删除、暂停、恢复、快照和 Fork 继续调用原 AgentENV API。

## 前提及配置

本方案的 standalone 指 AgentENV 原生单机部署；PVC/PV、Snapshot、CSI Controller 和 Guard 仍由 Kubernetes 管理。宿主必须是配置中真实的 Kubernetes Node，具备匹配的 CSINode、CSI Node socket、Linux/KVM 和原 AgentENV 运行环境。PostgreSQL、CSI 管理网关与其 mTLS 身份沿用 SSF 安装方案。

1. 将三个 JSON 样例复制到 `/etc/agentenv-storage/`。替换 `worker-1`、租户 namespace、Driver 名称、ControllerEndpoint、NodeEndpoint。`node.json.native_node_id`、`control.json.standalone.node_id` 和原生 `AENV_NODE_ID` 必须一致；两个 `kube_node/node_name` 必须指向本机真实 Kubernetes Node。
2. 配置只授予所需权限的 kubeconfig。将 PostgreSQL DSN 写到 root 可读的 `/etc/agentenv-storage/control.env`：`SSF_DATABASE_URL=...`，文件权限 0600。加载插件时执行幂等公共/原生 schema 迁移。
3. 使用同一可信 CA 签发独立客户端/服务器证书。`node.json` 证书 URI 为 `spiffe://ssf/agentenv/node`；`control.json` 为 `spiffe://ssf/agentenv/scheduler`；`ssf-node.json` 为 `spiffe://ssf/agentenv/ssf-node`。本机两个服务器证书均须包含 `localhost` DNS 和 `127.0.0.1` IP SAN。控制客户端与 NodePlugin 服务器不能误用同一身份。
4. 生成随机 hook-token、dispatch-key（至少 32 字节），以及公共计划 Ed25519 密钥对；原生服务和插件读取同一 hook-token，计划公钥与私钥匹配。将原生 API Key 放到样例 `agent_key_file` 指定的只读文件。
5. 将本目录 unit 安装到 systemd 配置目录；将 `agentenv-storage.conf` 安装为现有 `agentenv.service.d/storage.conf`。不覆盖原服务的 ExecStart。三个进程使用宿主 PID 和挂载 namespace，访问同一 `/var/lib/ssf`、`/var/lib/agentenv-external` 和 `/var/lib/agentenvadapter` 持久目录；不要给单个服务设置 PrivateMounts/PrivateDevices 隔离这些共享资源。

本机 hook 只绑定 loopback 并验证 token；控制端点使用 mTLS。原生创建中客户提供的 `x-agentenv-storage-*` header 被剥离，本机插件自行生成固定节点的完整签名计划。集群模式不启用 `AENV_STORAGE_PROVIDER_STANDALONE`，仍由原 Gateway/Scheduler 完成调度。

## 启动、验证与停止

安装者完成配置后运行 systemd daemon-reload，依次启动 SSF Node、存储插件和原 AgentENV。启动期间插件的后台恢复可能因原生 API 尚未就绪重试；不释放未知运行资源。原生健康检查就绪后执行 `scripts/native_lifecycle_e2e.py --help`，将其 endpoint 指向原生监听地址，使用原 API Key。先使用预检和只读证据工具，再按测试方案显式执行有副作用的 E2E。

正常停止时先停止接收原生创建、排空原生沙箱，再停止原生服务、插件和 SSF Node。systemd 的反向依赖顺序保证原生服务先停止，但不替代真实 VMM 停止证据；未知停止状态保留 WAL/attachment，重启后恢复。不要清空 WAL 或强制卸载正在使用的设备。

当前样例要求 `agentenv-capture-clone-v1`（hook version 3），表示已实现的生命周期协议。宿主 systemd、KVM 与真实 CSI 执行尚未在本机 macOS 验证；Go 协议/安全测试与 Rust 交叉类型检查不能替代这些真机测试。

## 本地凭据生成

在 SSF 目录运行 `python3 scripts/native_credentials.py --out /secure/native-credentials --native-api-key-file /secure/AENV_API_KEY --database-url-file /secure/ledger-url`。输出中 `agentenv-storage-control-tls`、`agentenv-storage-node-tls`、`agentenv-ssf-node-tls` 各自包含证书；证书包含 localhost DNS 和 127.0.0.1 IP SAN。每个 `*-files` 目录包含对应 Secret 的私有文件。按 JSON 路径安装，node TLS 目录在样例中名为 `/etc/agentenv-storage-tls`；将生成的 node TLS 映射到此目录。`agentenv-storage-node-files` 提供 hook-token/dispatch-key/node-key/plan.pub，control-files 提供计划私钥。不要把 CA 私钥分发到节点。

`native_preflight.py --mode standalone --context CONTEXT ...` 不要求 Gateway/Scheduler Deployment 或节点 DaemonSet，仍核对 Kubernetes CSI/存储管理入口。原生宿主服务的运行证据须另行采集。
