# AgentENV + 独立 SSF coordinator 部署

更新：2026-10-21。仅用于新测试环境。本文给出操作者执行步骤；本轮未自动部署。原 Gateway、Scheduler、Node DaemonSet、headless Service、心跳、P2P、探针和优雅退出继续使用原 base。

## 1. 目录和构建

```text
sandboxstorage/
  AgentENV/
  sandbox-storage/
  design/
```

Go 1.26 工具链可构建双方；Scheduler 使用普通 `go.mod`。Rust 的兄弟目录依赖由两个仓库联合构建，使用本次交付的 Cargo.lock。本机 Linux 构建检查入口是 `sandbox-storage/scripts/check_agentenv_linux.sh`。

在 `sandboxstorage` 下构建五个镜像：

```bash
docker build -f sandbox-storage/Dockerfile -t ssf:test sandbox-storage
docker build -f sandbox-storage/adapter/agentenvadapter/Dockerfile -t agentenvadapter:test sandbox-storage
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.scheduler -t agentenv-scheduler:test .
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.gateway -t agentenv-gateway:test .
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.agentenv -t agentenv-runtime:test .
```

最后的 runtime 镜像保留原依赖安装、tools、OverlayBD、ublk-daemon 和 setup-only 阶段。首次构建需要原项目依赖下载环境；镜像构建及运行本轮未在本机执行。部署时将镜像改为集群能拉取的已推送地址或手工导入测试集群；助手不会推送镜像。

## 2. 先准备公共设施

1. Linux/KVM、原 AgentENV 已验证可用的 guest 镜像、CSI Driver（Controller/Node）、StorageClass、Node/CSINode/topology 准备好。
2. PostgreSQL 由独立服务提供。协调服务使用消费/迁移账号；Guard 使用受限读取账号。启动协调服务会幂等执行公共及 adapter schema。不要清理旧 DB 来替代迁移核验。
3. 按 `sandbox-storage/deploy` 部署 Guard 和必要的 CSI Gateway。参考 [05 Kubernetes 集成](05-kubernetes-integration.md)；使用新协调服务 URI `spiffe://ssf/agentenv/coordinator` 授权 Controller Gateway。
4. 卷准备 Guard 的可信协调用户改为 `system:serviceaccount:<namespace>:ssf-coordinator`。`scripts/render_volume_preparation.py` 已生成此配置；保留原厂商 provisioner/parameters，不批量改现有 SC。

## 3. 生成凭据并离线渲染

在 `sandbox-storage` 下执行，输入文件由操作者准备且不放入 Git：

```bash
python3 scripts/native_credentials.py   --out /secure/ssf-new-credentials   --namespace agentenv-system   --native-api-key-file /secure/AENV_API_KEY   --database-url-file /secure/ledger-url

python3 scripts/render_agentenv_native.py   --namespace agentenv-system   --api-key-file /secure/AENV_API_KEY   --driver YOUR_CSI_DRIVER   --controller-endpoint YOUR_CONTROLLER_GATEWAY:9443   --agent-image agentenv-runtime:test   --scheduler-image agentenv-scheduler:test   --gateway-image agentenv-gateway:test   --adapter-image agentenvadapter:test   --ssf-image ssf:test   --output /secure/agentenv-ssf.yaml
```

凭据工具拒绝覆盖已有目录，输出四种 TLS 身份：scheduler、coordinator、node、ssf-node。Gateway/node/dispatch/hook 密钥分离。`secrets.json`、渲染 YAML 含 Secret，文件权限 0600。CA 私钥不分发到节点。协调服务、NodePlugin、SSF Node 与 Controller Gateway 应使用同一可信 CA 或明确配置互信；一次性凭据工具生成的新 CA 不会自动加入已有 CSI Gateway 的信任配置。

渲染器复制原 base 与框架 overlay 到临时联合目录；补齐原 base 的 agentenv.toml，保留命名空间参数和原 API Key。原仓库的 `deploy/k8s/overlays/ssf` 已移走，新的权威目录是 `sandbox-storage/adapter/agentenvadapter/deploy/k8s`。

操作者审查 YAML，确认驱动地址、证书、数据库、镜像和挂载路径后，手动应用到明确的新测试集群。凭据与工作负载是两个独立文件，渲染器不会把前一步的 TLS/账本 Secret 自动合入 YAML。命名空间先存在，再安装凭据和工作负载：

```bash
kubectl create namespace agentenv-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f /secure/ssf-new-credentials/secrets.json
kubectl apply -f /secure/agentenv-ssf.yaml
```

上述命令由操作者执行，本轮未执行。公共 RBAC/Guard/CSI Gateway 按第 2 节先安装；不要使用旧控制配置或将旧 NodePlugin 与新服务混用。

## 4. 核对进程与权限

| 工作负载 | 应有内容 |
| --- | --- |
| 原 Gateway | 原鉴权/路由/代理；gateway 签名 key；storage overlay 中 timeout 300s |
| 原 Scheduler | 原 gRPC 9090、原探针；新增只读 mTLS 9447；只挂 scheduler.json 与自身 TLS；没有 SSF_DATABASE_URL 或计划私钥 |
| ssf-coordinator | 9446 mTLS、9096 `/livez`/`/readyz`；2 副本；共同 PostgreSQL、control.json/profile、计划签名和消费 RBAC |
| 原 Node Pod | 原容器、两常驻 sidecar、shared PID、共享设备/WAL路径、双向 mount propagation；hook 仅 127.0.0.1:9445 |
| Guard | 独立 Pod；有限账本读取、profile/default/finalizer 准入，不执行设备操作 |

`/readyz` 校验账本可用；Node 身份/能力、原心跳和实际容量在每次准入检查。Immediate 自动准备不等待沙箱；WFFC 在原 Scheduler 选点后绑定和准备。用户不创建 init/holder Pod。

## 5. 验证与停止

先按 [20 手动 PVC 场景](20-kubernetes-pvc-agentenv-mount-validation.md) 完成 PVC → A 写 → 删 A → B 读；再按 [14](14-agentenvadapter-real-environment-test-plan.md) 做生命周期和故障回归。SSF/adapter 的 HTTP 和数据库就绪不能替代 guest 数据持久性证据。

停止时先限制新创建、排空原生沙箱，再停止原 Node/sidecar，最后协调服务。服务 drain 不伪造停止或删除 DB/WAL/PVC；未确认的资源保留并在重启后恢复。

## 6. standalone

使用框架目录 `adapter/agentenvadapter/deploy/standalone` 的四个 JSON 和三个附加 unit：`ssf-coordinator.service`、`agentenv-storage-node.service`、`ssf-node.service`。保留原 `agentenv.service`，仅加 storage drop-in。

数据库变量只放在 coordinator 的 `/etc/agentenv-storage/ledger.env`（0600）。NodePlugin 仅配置 coordinator 的 loopback HTTPS 端点；不要设置旧 `standalone_config`。按各 JSON 路径安装对应身份证书，新增 coordinator 证书 URI 与集群相同，证书需含 localhost/127.0.0.1。standalone 仍需要 Kubernetes/CSI/Guard，表示原运行时单机部署而非脱离 K8S 管理卷。
