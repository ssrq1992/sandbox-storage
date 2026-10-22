# 原生 standalone 接入

保留原 `agentenv.service` 的二进制、ExecStart、端口和 API Key；安装 storage drop-in。新增 `ssf-coordinator.service`、`ssf-node.service`、`agentenv-storage-node.service`，不启动公开替代沙箱 API。

1. 将本目录四个 JSON 放到 `/etc/agentenv-storage`，设置真实 Kubernetes Node、namespace、Driver、CSI socket 及固定 native node ID。standalone 仍使用 K8S/CSI/Guard 管理卷。
2. 数据库 DSN 仅由 `ssf-coordinator.service` 从 `/etc/agentenv-storage/ledger.env` 读取（0600）。NodePlugin 不再接受旧 standalone_config/scheduler_endpoint。
3. 安装四类证书中本机所需的 coordinator/node/ssf-node 身份；按 JSON 的路径放置，URI SAN 为 `spiffe://ssf/agentenv/<角色>`，DNS/IP SAN 包括 localhost/127.0.0.1。NodePlugin 的服务器和控制客户端可以引用同一 node 身份的证书文件。
4. 安装 hook-token、node-key、dispatch-key、计划密钥、原 API Key。计划私钥只给协调服务，节点仅计划公钥。凭据工具 `scripts/native_credentials.py` 可在本机离线生成，CA 私钥不分发。
5. 安装三个附加 unit、原 service drop-in；操作者 daemon-reload 后启动。协调服务可先运行并重试未就绪节点；不得以原 API 未就绪为由释放未知资源。

原 Node、NodePlugin 和 SSF Node 使用同一 host PID/设备/mount 可见域及持久 WAL；不设置 PrivateDevices/PrivateMounts。停止时先排空原沙箱，再停原 Node、插件/SSF Node、协调服务；真实停止证据始终是释放依据。

[部署和验证](../../../../../design/13-agentenvadapter-installation-and-deployment.md)。本机未执行 systemd/KVM/CSI 真机测试。
