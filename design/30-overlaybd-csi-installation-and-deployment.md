# 30 — AgentENV + SSF + 原生 OverlayBD CSI 完整安装部署

更新时间：2026-10-22。适用代码：同一工作区的 `AgentENV/` 与 `sandbox-storage/`。

本文按 **准备环境 → 构建镜像 → 数据库 → 证书 → 渲染 → 安装 → PVC 验证** 给出新环境部署步骤。
主线采用一个 Linux Worker、本地 POSIX repository；共享文件系统、OSS、扩容及 standalone 在后文说明。
所有集群命令由部署人员手动执行；本文编写期间未连接或部署集群。

> **版本状态：测试部署说明，不是生产可用声明。** 当前仍有跨 daemon 未知设备、活动 capture 内部 restack、封存前快照中断等恢复缺项。已通过的本机测试和未执行项见 [28 实施状态](28-overlaybd-csi-implementation-status.md)。Docker 镜像构建、Linux/KVM/ublk、Kubernetes 及真实后端端到端尚未执行。不要将本流程用于现有业务卷迁移。

## 1. 部署完成后有哪些组件

| 组件 | 部署位置 | 职责 |
| --- | --- | --- |
| AgentENV Gateway | 原 Deployment | 原鉴权、API、代理 |
| AgentENV Scheduler | 原 Deployment | 原节点发现、健康过滤、计算选点；调用独立 Coordinator |
| SSF Coordinator | 独立 Deployment | 公共消费协调、卷准备、使用账本及 adapter 控制逻辑 |
| PostgreSQL | 独立数据库服务 | 公共 ledger、adapter 状态和隔离的 OverlayBD Driver schema |
| SSF Guard | `ssf-system` 独立 Deployment | PVC/PV/快照保护、profile 默认化；不操作设备 |
| OverlayBD Controller | 独立 Deployment | 标准 CSI Controller、provisioner、snapshotter；可选 resizer |
| CSI Gateway | 本文加到 Controller Pod 的容器 | mTLS 转接该 Pod 内 CSI socket；共享 ledger 校验 Attach/Detach |
| AgentENV 节点 | 原 DaemonSet | 原 Orchestrator/VMM、legacy 功能、SSF Node、runtime adapter 与 OverlayBD 节点容器 |
| OverlayBD 节点容器 | 原 Node Pod 内 | 原生库宿主、专属 ublk daemon、worker、CSI Node、registrar |
| snapshot-controller | 集群公共组件 | Kubernetes VolumeSnapshot 对象调谐 |

业务 I/O：**guest → Firecracker → ublk → 原 OverlayBD 引擎 → repository**。
CSI、宿主 UDS、Coordinator RPC 是管理链路，不承载业务读写。Controller 通过 worker 供卷，不调用 CSI Node 供卷。
不添加每卷 Pod、初始化 Pod、Holder Pod，也不再部署第二套公开沙箱 API。

## 2. 环境和变量

### 2.1 必备条件

- 一个专用测试 Kubernetes 集群，或管理员已审核集群级资源影响的环境。namespace 不能隔离 CSIDriver、StorageClass、ClusterRole 和 webhook。
- Linux Worker 能运行当前 AgentENV 的 KVM 模式；有 `/dev/kvm`、`/dev/net/tun`、`/dev/ublk-control`，内核支持实际使用的 io_uring/ublk 能力。不能只根据内核版本号判断可用。
- 本流程选 Kubernetes 1.33 或以上，使用已稳定的 native sidecar 机制；这只是功能前提，不是本项目已验证的版本矩阵。参见 [Kubernetes sidecar 文档](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)。
- Worker 允许特权容器、设备访问、hostPath 和双向 mount propagation。集群的准入策略必须允许本测试 namespace 使用这些原生运行时能力。
- 可连接的 PostgreSQL，以及数据库管理员提供的建库/迁移权限。本流程不把数据库放在尚未安装的 OverlayBD PVC 上。
- 管理机：Docker/BuildKit、kubectl、kustomize、Go（以仓库 go.mod 为准）、Python 3、PyYAML、openssl、jq、psql。
- 容器构建需要访问各 Dockerfile 固定的基础镜像和原 AgentENV 依赖下载地址。若使用镜像代理，应保留版本/摘要，不换成任意 latest。

在 Worker 上检查：

```bash
uname -a
ls -l /dev/kvm /dev/net/tun /dev/ublk-control
lsmod | grep ublk
findmnt -o TARGET,PROPAGATION /
```

ublk 模块未加载时，由节点管理员检查发行版配置并执行 `sudo modprobe ublk_drv`。
如果这些设备无法使用，先解决原 AgentENV/Linux 环境，不能通过改用普通文件冒充 block endpoint。

### 2.2 管理机工作目录

以下命令使用 Bash。将路径、节点名、仓库地址改成真实值，固定两个 namespace 以便与仓库公共模板一致：

```bash
export ROOT='/你的路径/sandboxstorage'
export SSF="$ROOT/sandbox-storage"
export KUBE_CONTEXT='你的测试context'
export NS='agentenv-system'
export SSF_NS='ssf-system'
export WORKER='kubectl get nodes 显示的真实节点名'
export WORKER_ENDPOINT='Controller可以访问的节点DNS或IP:9455'
export REGISTRY='你的镜像仓库/ssf-test'
export TAG='overlaybd-test-20261022'
export OUT="$HOME/ssf-overlaybd-install-$TAG"
umask 077
mkdir -p "$OUT"
chmod 700 "$OUT"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
k get nodes -o wide
k version
```

`OUT` 放在 Git 仓库外，保存明文凭据和渲染清单，不能提交到 Git。
本流程还会给目标 Worker 添加 `ssf.dev/overlaybd-test=true`，用于收窄原 DaemonSet；不覆盖原 Linux selector。

## 3. 构建并准备六个镜像

在双仓共同父目录执行。前三个联合 Rust/Go 构建必须能同时看到兄弟目录：

```bash
cd "$ROOT"
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.agentenv \
  -t "$REGISTRY/agentenv-runtime:$TAG" .
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.scheduler \
  -t "$REGISTRY/agentenv-scheduler:$TAG" .
docker build -f sandbox-storage/adapter/agentenvadapter/deploy/Dockerfile.gateway \
  -t "$REGISTRY/agentenv-gateway:$TAG" .
docker build -f sandbox-storage/drivers/overlaybd/Dockerfile \
  -t "$REGISTRY/overlaybd:$TAG" .
docker build -f sandbox-storage/Dockerfile \
  -t "$REGISTRY/ssf:$TAG" sandbox-storage
docker build -f sandbox-storage/adapter/agentenvadapter/Dockerfile \
  -t "$REGISTRY/agentenvadapter:$TAG" sandbox-storage
```

本地 YAML 组装步骤需要 PyYAML，可在单独虚拟环境安装 `python3 -m pip install PyYAML` 后执行。

保留 Dockerfile 对应的 `.dockerignore`，防止把 target、基线备份和验证报告加入联合上下文。
记录构建输出与最终镜像 digest；六个镜像必须来自本次同一份源码，不混用旧协议版本。

把镜像推送到 Worker 可拉取的仓库，或按测试集群的镜像导入流程加载。例如逐项执行：

```bash
docker push "$REGISTRY/agentenv-runtime:$TAG"
docker push "$REGISTRY/agentenv-scheduler:$TAG"
docker push "$REGISTRY/agentenv-gateway:$TAG"
docker push "$REGISTRY/overlaybd:$TAG"
docker push "$REGISTRY/ssf:$TAG"
docker push "$REGISTRY/agentenvadapter:$TAG"
```

私有仓库需要在对应 namespace 设置 imagePullSecrets，或使用已有节点凭据。最终生产候选应使用已验证 digest。
官方 CSI sidecar 的固定版本由渲染器给出：provisioner v5.2.0、snapshotter v8.2.0、registrar v2.13.0；可选 resizer v1.13.2。必须让节点能拉取这些镜像。

## 4. 准备 PostgreSQL 与角色

使用一个**全新测试数据库**，公共账本、adapter 和 Driver 连接同一数据库；Driver 自己管理隔离 schema。
不要把 Guard 指向另一个没有账本数据的库。

由数据库管理员在 psql 中执行以下交互步骤；角色名为本文示例，若已存在则先核对用途，不能覆盖旧环境：

```sql
CREATE ROLE ssf_test_owner LOGIN;
\password ssf_test_owner
CREATE ROLE ssf_test_guard LOGIN;
\password ssf_test_guard
CREATE ROLE ssf_test_gateway LOGIN;
\password ssf_test_gateway
CREATE DATABASE ssf_overlaybd_test OWNER ssf_test_owner;
```

在 `OUT` 创建三个仅本用户可读的文件，分别填入 owner、Guard、Gateway 的完整连接串：

```text
ledger-url          postgresql://ssf_test_owner:URL编码的密码@数据库地址:5432/ssf_overlaybd_test?sslmode=verify-full
ledger-guard-url    postgresql://ssf_test_guard:URL编码的密码@数据库地址:5432/ssf_overlaybd_test?sslmode=verify-full
ledger-gateway-url  postgresql://ssf_test_gateway:URL编码的密码@数据库地址:5432/ssf_overlaybd_test?sslmode=verify-full
```

数据库 TLS CA 按实际数据库配置安装到客户端和容器，或使用数据库提供的、在容器内可读的 `sslrootcert` 路径及 Secret 挂载。
**不要填写只在管理机存在的 CA 路径而不挂到 Pod。** 隔离测试网若明确采用非 TLS PostgreSQL，连接串应如实配置；不能把它描述成已启用传输加密。

```bash
chmod 600 "$OUT/ledger-url" "$OUT/ledger-guard-url" "$OUT/ledger-gateway-url"
cd "$SSF"
SSF_DATABASE_URL="$(cat "$OUT/ledger-url")" go run ./cmd/ssfctl -action migrate
```

随后数据库管理员连接 `ssf_overlaybd_test`，执行 `sandbox-storage/deploy/ledger-roles.sql`（需要创建组角色权限，不能直接用上面普通 owner 登录角色执行）。在管理员 psql 会话中，先用 `\i /实际绝对路径/sandbox-storage/deploy/ledger-roles.sql` 加载文件，再授权：

```sql
GRANT ssf_guard_reader TO ssf_test_guard;
GRANT ssf_gateway_reader TO ssf_test_gateway;
```

Coordinator 和 Driver 在本测试流程使用 owner 账号，启动时分别执行 adapter/Driver 幂等迁移；Scheduler、原生 AgentENV、NodePlugin 不持有数据库凭据。
这不是最终最小权限生产账号设计。`ssfctl migrate` 只迁移公共 ledger，不能据此认为 adapter/Driver schema 已启动验证。

## 5. 生成原生凭据和补充证书

### 5.1 原生四种身份及签名密钥

新环境可生成 API Key；已有原 API Key 时改用已有文件，不能让 Gateway 与原生节点使用不同值。

```bash
openssl rand -hex 32 > "$OUT/AENV_API_KEY"
cd "$SSF"
python3 scripts/native_credentials.py \
  --out "$OUT/credentials" --namespace "$NS" \
  --native-api-key-file "$OUT/AENV_API_KEY" \
  --database-url-file "$OUT/ledger-url"
```

该工具生成 CA、scheduler/coordinator/node/ssf-node TLS、计划签名密钥和独立 hook/dispatch/node/gateway key；拒绝覆盖已存在的输出目录。
CA 私钥只保留在管理机，不挂入 Pod。叶证书有效期为工具固定的 90 天，需记录轮换日期。

### 5.2 Guard、CSI Gateway、OverlayBD Node/Controller

四者需要与上一步同一 CA。以下函数只生成本地证书，不部署任何组件：

```bash
issue_cert() {
  name="$1"; san="$2"
  mkdir "$OUT/credentials/$name"
  cat > "$OUT/credentials/$name/extensions.cnf" <<CERT
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=$san
CERT
  openssl req -new -newkey rsa:2048 -nodes -sha256 -subj "/CN=$name" \
    -keyout "$OUT/credentials/$name/tls.key" -out "$OUT/credentials/$name/request.csr"
  openssl x509 -req -sha256 -days 90 \
    -in "$OUT/credentials/$name/request.csr" \
    -CA "$OUT/credentials/ca/ca.crt" -CAkey "$OUT/credentials/ca/ca.key" \
    -set_serial "0x$(openssl rand -hex 16)" \
    -extfile "$OUT/credentials/$name/extensions.cnf" \
    -out "$OUT/credentials/$name/tls.crt"
  openssl verify -CAfile "$OUT/credentials/ca/ca.crt" "$OUT/credentials/$name/tls.crt"
}
issue_cert ssf-guard-tls "DNS:ssf-guard.$SSF_NS.svc"
issue_cert ssf-csi-gateway-tls "DNS:ssf-csi-gateway.$NS.svc,URI:spiffe://ssf/csi-gateway"
issue_cert ssf-overlaybd-controller-tls 'URI:spiffe://ssf/overlaybd-controller'
```

OverlayBD Node 证书需要覆盖 `WORKER_ENDPOINT` 的主机部分，以及 loopback 证据链路。
若 endpoint 是 DNS：

```bash
export WORKER_HOST="${WORKER_ENDPOINT%:9455}"
issue_cert ssf-overlaybd-node-tls \
  "DNS:$WORKER_HOST,IP:127.0.0.1,DNS:localhost,URI:spiffe://ssf/overlaybd-node"
```

若 endpoint 是 IPv4，改为 `IP:$WORKER_HOST`，不要写成 DNS SAN。多节点当前共用该 Secret，需要证书包含 inventory 全部 worker 地址 SAN；本文单节点主线只含一个地址。
证书 URI 必须精确匹配上述字符串，不能仅有 CN。不把 `overlaybd-node` 证书替换为 `agentenv/node` 证书。

## 6. 安装公共快照组件

先检查集群是否已安装，已安装则核对版本、SA 与健康状态，不重复装第二套 controller：

```bash
k get crd volumesnapshots.snapshot.storage.k8s.io \
  volumesnapshotcontents.snapshot.storage.k8s.io volumesnapshotclasses.snapshot.storage.k8s.io
k -n kube-system get deployment snapshot-controller
```

未安装时，下载并审核仓库固定基线 v8.2.0，再安装：

```bash
git clone --depth 1 --branch v8.2.0 \
  https://github.com/kubernetes-csi/external-snapshotter.git "$OUT/external-snapshotter"
k apply -f "$OUT/external-snapshotter/client/config/crd/"
# v8.2.0 仓库中的示例 Deployment 仍引用 controller:v8.0.1。
# 在下载副本中显式统一为本次采用的 v8.2.0，再应用；不改集群其他组件。
python3 - <<'PY_SNAPSHOT'
from pathlib import Path
import os
p=Path(os.environ['OUT'])/'external-snapshotter/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml'
s=p.read_text()
assert 'snapshot-controller:v8.0.1' in s
p.write_text(s.replace('snapshot-controller:v8.0.1','snapshot-controller:v8.2.0'))
PY_SNAPSHOT
k apply -k "$OUT/external-snapshotter/deploy/kubernetes/snapshot-controller/"
k -n kube-system rollout status deployment/snapshot-controller --timeout=180s
```

以 [官方 v8.2.0 安装目录](https://github.com/kubernetes-csi/external-snapshotter/tree/v8.2.0/deploy/kubernetes/snapshot-controller) 为来源，核对其 namespace/ServiceAccount；如果集群现有 controller 使用不同 SA，后面的 Guard `SnapshotControllerUsers` 必须同步修改。

## 7. 准备 inventory 并离线渲染

### 7.1 本地 POSIX inventory

```bash
cd "$SSF"
cp drivers/overlaybd/deploy/nodes.local.example.json "$OUT/nodes.json"
python3 - <<'PY'
import json, os
from pathlib import Path
p=Path(os.environ['OUT'])/'nodes.json'
d=json.loads(p.read_text())
d['nodes'][0].update(node=os.environ['WORKER'], worker_endpoint=os.environ['WORKER_ENDPOINT'],
                     accessible_nodes=[os.environ['WORKER']])
p.write_text(json.dumps(d,indent=2)+'\n')
PY
```

示例 repository ID `agentenv-overlaybd-node-a` 是稳定后端身份，可以保留，不能在使用后随意改名。
默认后端路径 `/var/lib/ssf-overlaybd-repository` 必须与 legacy 数据目录隔离；WAL/upper 路径为 `/var/lib/ssf/overlaybd`。
两者都必须位于节点持久磁盘，重建 Pod 不会自动搬迁本地数据。

### 7.2 三份主清单

```bash
python3 scripts/render_agentenv_native.py \
  --namespace "$NS" --api-key-file "$OUT/AENV_API_KEY" \
  --driver overlaybd.csi.ssf.dev \
  --controller-endpoint "ssf-csi-gateway.$NS.svc:9443" \
  --agent-image "$REGISTRY/agentenv-runtime:$TAG" \
  --scheduler-image "$REGISTRY/agentenv-scheduler:$TAG" \
  --gateway-image "$REGISTRY/agentenv-gateway:$TAG" \
  --adapter-image "$REGISTRY/agentenvadapter:$TAG" \
  --ssf-image "$REGISTRY/ssf:$TAG" \
  --overlaybd-inventory "$OUT/nodes.json" --overlaybd-image "$REGISTRY/overlaybd:$TAG" \
  --output "$OUT/agentenv-overlaybd.yaml"
python3 scripts/render_overlaybd_controller.py --namespace "$NS" \
  --image "$REGISTRY/overlaybd:$TAG" --inventory "$OUT/nodes.json" \
  > "$OUT/overlaybd-controller.json"
python3 scripts/render_overlaybd_classes.py --inventory "$OUT/nodes.json" \
  --default-snapshot-class > "$OUT/overlaybd-classes.json"
```

最后的默认快照类注解只适用于本新环境；同一 Driver 已有默认类时，先核对并去掉该开关，不能新增第二个默认类。

### 7.3 收窄 DaemonSet，并给 Controller 添加 CSI Gateway

当前渲染器不生成 Gateway 工作负载。不要把 Gateway 指到另一台机器上的 UDS，也不能给 Gateway 单独创建一个没有 Driver socket 的 emptyDir。
以下仅修改 `OUT` 下生成清单，复用 Controller Pod 的 `socket` 卷：

```bash
python3 - <<'PY'
import json, os, yaml
from pathlib import Path
out=Path(os.environ['OUT']); ns=os.environ['NS']
image=os.environ['REGISTRY']+'/ssf:'+os.environ['TAG']
p=out/'agentenv-overlaybd.yaml'; docs=list(yaml.safe_load_all(p.read_text()))
for d in docs:
    if d and d.get('kind')=='DaemonSet':
        d['spec']['template']['spec'].setdefault('nodeSelector',{})['ssf.dev/overlaybd-test']='true'
p.write_text(yaml.safe_dump_all(docs,sort_keys=False)); p.chmod(0o600)
p=out/'overlaybd-controller.json'; d=json.loads(p.read_text())
cfg=dict(Listen=':9443',HealthListen=':9090',Cert='/etc/gateway-tls/tls.crt',
         Key='/etc/gateway-tls/tls.key',CA='/etc/gateway-tls/ca.crt',
         AllowedPeers=['spiffe://ssf/agentenv/coordinator'],DriverName='overlaybd.csi.ssf.dev',
         DriverEndpoint='unix:///run/csi/private/csi.sock')
d['items'].append(dict(apiVersion='v1',kind='ConfigMap',metadata=dict(name='ssf-csi-gateway',namespace=ns),
                       data={'config.json':json.dumps(cfg)}))
pod=next(v for v in d['items'] if v['kind']=='Deployment')['spec']['template']['spec']
pod['volumes'] += [dict(name='gateway-config',configMap=dict(name='ssf-csi-gateway')),
                   dict(name='gateway-tls',secret=dict(secretName='ssf-csi-gateway-tls'))]
pod['containers'].append(dict(name='gateway',image=image,command=['ssf-csi-gateway'],
    args=['-config','/etc/gateway/config.json'],
    env=[dict(name='SSF_DATABASE_URL',valueFrom=dict(secretKeyRef=dict(name='ssf-gateway-ledger',key='url')))],
    securityContext=dict(runAsUser=0,allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities=dict(drop=['ALL'])),
    ports=[dict(name='gateway',containerPort=9443)],
    readinessProbe=dict(httpGet=dict(path='/livez',port=9090)),
    volumeMounts=[dict(name='socket',mountPath='/run/csi'),dict(name='gateway-config',mountPath='/etc/gateway',readOnly=True),
                  dict(name='gateway-tls',mountPath='/etc/gateway-tls',readOnly=True)]))
d['items'].append(dict(apiVersion='v1',kind='Service',metadata=dict(name='ssf-csi-gateway',namespace=ns),
                       spec=dict(selector={'app':'ssf-overlaybd-controller'},ports=[dict(name='grpc',port=9443,targetPort='gateway')])))
p.write_text(json.dumps(d,indent=2)+'\n')
PY
```

Gateway 使用 UID 0 是为了访问 Controller 建立的私有 socket 目录，capabilities 全部移除；此进程没有设备权限，数据库使用只读 Gateway 角色。`/livez` 只是进程探针，不能代替 CSI Attach 验收。

## 8. 生成 Guard 配置与清单

**不要整份应用 `deploy/base.yaml`**：其中包含独立 `ssf-node` DaemonSet，本方案已经在原 Node Pod 内部署 SSF Node。
以下只提取 Guard 资源，保留原模板权限，并从 Coordinator 的最终配置复制同一 profile：

```bash
cd "$SSF"
python3 - <<'PY'
import os,json,base64,yaml
from pathlib import Path
out=Path(os.environ['OUT']); ns=os.environ['NS']; ssfns=os.environ['SSF_NS']
assert ssfns=='ssf-system', '公共模板使用 ssf-system，本步骤没有做任意命名空间替换'
objects=[v for v in yaml.safe_load_all(Path('deploy/base.yaml').read_text())
         if v and (v['kind']=='Namespace' or v['metadata']['name']=='ssf-guard')]
for v in objects:
    if v['kind']=='Deployment':
        v['spec']['template']['spec']['containers'][0]['image']=os.environ['REGISTRY']+'/ssf:'+os.environ['TAG']
(out/'guard-workloads.yaml').write_text(yaml.safe_dump_all(objects,sort_keys=False))
rendered=list(yaml.safe_load_all((out/'agentenv-overlaybd.yaml').read_text()))
cm=next(v for v in rendered if v and v['kind']=='ConfigMap' and 'control.json' in v.get('data',{}))
profiles=json.loads(cm['data']['control.json'])['volume_profiles']
cfg=json.loads(Path('deploy/guard-preparation.example.json').read_text())
cfg.update(VolumeProfiles=profiles,ManagedNamespaces=[ns],
           CoordinatorUsers=['system:serviceaccount:'+ns+':ssf-coordinator'],
           SnapshotControllerUsers=['system:serviceaccount:kube-system:snapshot-controller',
                                    'system:serviceaccount:'+ns+':ssf-overlaybd-controller'])
(out/'guard-config.json').write_text(json.dumps(cfg,indent=2)+'\n')
ca=base64.b64encode((out/'credentials/ca/ca.crt').read_bytes()).decode()
valid=yaml.safe_load(Path('deploy/webhook.yaml').read_text())
mutate=yaml.safe_load(Path('deploy/volume-preparation-webhook.yaml').read_text())
for v in [valid,mutate]:
    for w in v['webhooks']: w['clientConfig']['caBundle']=ca
mutate['webhooks'][0]['namespaceSelector']['matchExpressions'][0]['values']=[ns]
(out/'guard-webhooks.yaml').write_text(yaml.safe_dump_all([valid,mutate],sort_keys=False))
PY
```

OverlayBD profile 的 `initializeBlank` 必须为 **false**。不要使用 `render_volume_preparation.py` 的 generic profile 去替代它。
验证 webhook 涉及集群级 PV/Content，按模板是集群范围；Guard 故障会按 Fail 策略影响匹配请求。安装前必须审核影响，不能误认为只影响 `NS`。

## 9. 安装顺序

### 9.1 namespace、标签与 Secret

```bash
k create namespace "$NS" --dry-run=client -o yaml | k apply -f -
k create namespace "$SSF_NS" --dry-run=client -o yaml | k apply -f -
k label node "$WORKER" ssf.dev/overlaybd-test=true
k apply -f "$OUT/credentials/secrets.json"
# 新环境固定原生 sandbox access token 的 seed；以后重启/升级保留该值。
openssl rand -hex 32 > "$OUT/sandbox-access-token-hash-seed"
k -n "$NS" create secret generic agentenv-runtime-secrets \
  --from-file=sandbox-access-token-hash-seed="$OUT/sandbox-access-token-hash-seed" \
  --dry-run=client -o yaml | k apply -f -

put_tls() {
  namespace="$1"; name="$2"
  k -n "$namespace" create secret generic "$name" \
    --from-file=tls.crt="$OUT/credentials/$name/tls.crt" \
    --from-file=tls.key="$OUT/credentials/$name/tls.key" \
    --from-file=ca.crt="$OUT/credentials/ca/ca.crt" \
    --dry-run=client -o yaml | k apply -f -
}
put_tls "$SSF_NS" ssf-guard-tls
put_tls "$NS" ssf-csi-gateway-tls
put_tls "$NS" ssf-overlaybd-controller-tls
put_tls "$NS" ssf-overlaybd-node-tls
k -n "$SSF_NS" create secret generic ssf-ledger \
  --from-file=url="$OUT/ledger-guard-url" --dry-run=client -o yaml | k apply -f -
k -n "$NS" create secret generic ssf-gateway-ledger \
  --from-file=url="$OUT/ledger-gateway-url" --dry-run=client -o yaml | k apply -f -
k -n "$NS" create secret generic ssf-overlaybd-database \
  --from-file=database-url="$OUT/ledger-url" --dry-run=client -o yaml | k apply -f -
k -n "$SSF_NS" create configmap ssf-guard-config \
  --from-file=config.json="$OUT/guard-config.json" --dry-run=client -o yaml | k apply -f -
```

Secret 名与 key 名不能混淆：Driver 用 `database-url`，公共 Guard/Gateway/Coordinator 用 `url`。
镜像拉取 Secret、数据库 CA Secret 如有需要，应在此时安装并将挂载加入生成清单。

### 9.2 先 Guard，再 webhook

```bash
k apply -f "$OUT/guard-workloads.yaml"
k -n "$SSF_NS" rollout status deployment/ssf-guard --timeout=180s
k -n "$SSF_NS" get endpoints ssf-guard
k apply -f "$OUT/guard-webhooks.yaml"
```

Guard 不就绪时先看数据库/证书日志，不把 webhook 改成 Ignore 来继续创建受管卷。

### 9.3 原 AgentENV + Coordinator + OverlayBD Controller

```bash
cd "$SSF"
go run ./tools/check-native-deploy -file "$OUT/agentenv-overlaybd.yaml" -namespace "$NS"
k apply --dry-run=server -f "$OUT/agentenv-overlaybd.yaml"
k apply --dry-run=server -f "$OUT/overlaybd-controller.json"
k apply -f "$OUT/agentenv-overlaybd.yaml"
k apply -f "$OUT/overlaybd-controller.json"
k -n "$NS" get deployments,daemonsets,pods -o wide
```

首次安装过程中 worker、Coordinator、Controller 可能互相等待依赖，应一起部署后观察；不要要求 Controller 在节点 worker 尚未运行时就完成供卷。
原 DaemonSet 名称以清单为准，用 label 等待即可：

```bash
k -n "$NS" rollout status deployment/agentenv-gateway --timeout=600s
k -n "$NS" rollout status deployment/agentenv-scheduler --timeout=600s
k -n "$NS" rollout status deployment/ssf-coordinator --timeout=600s
k -n "$NS" rollout status deployment/ssf-overlaybd-controller --timeout=600s
k -n "$NS" rollout status daemonset/agentenv-node --timeout=900s
```

如本地原 base 的对象名称已定制，使用前一条 `get` 的真实名字，不另建替代工作负载。

### 9.4 核对 CSI 注册后创建类

```bash
k get csidriver overlaybd.csi.ssf.dev
k get csinode "$WORKER" -o json | jq '.spec.drivers[] | select(.name=="overlaybd.csi.ssf.dev")'
k get node "$WORKER" -o json | jq '.metadata.labels["overlaybd.ssf.dev/node"]'
k apply -f "$OUT/overlaybd-classes.json"
k get sc -o json | jq '.items[] | select(.provisioner=="overlaybd.csi.ssf.dev") | {name:.metadata.name,mode:.volumeBindingMode,repository:.parameters.repository}'
k get volumesnapshotclass overlaybd-native-retain
```

本地 repository 的 topology 必须是实际 Worker；不能通过删除 allowedTopologies 让本地卷任意调度。
当前没有 external-attacher：Attach 由 SSF Gateway/共享账本负责，不能再装第二套 Attach 权威。

## 10. 检查实际部署结构

```bash
k -n "$NS" get pods -l app.kubernetes.io/name=agentenv-node -o json |
  jq '.items[] | {pod:.metadata.name,node:.spec.nodeName,init:[.spec.initContainers[]|{name,restartPolicy}],containers:[.spec.containers[].name]}'
k -n "$NS" logs deployment/ssf-coordinator --tail=100
k -n "$NS" logs deployment/ssf-overlaybd-controller -c driver --tail=100
k -n "$NS" logs deployment/ssf-overlaybd-controller -c gateway --tail=100
k -n "$SSF_NS" logs deployment/ssf-guard --tail=100
```

核对以下事实：

- 原 Gateway/Scheduler/AgentENV 容器仍在；Scheduler 未挂 DB URL、计划私钥或 CSI socket。
- 原 Node Pod 包含 `overlaybd-config` init、OverlayBD native sidecars、SSF Node 和 storage adapter。
- worker 9455 从 Controller Pod 可达；仅网络连通不等于 mTLS 身份正确。
- 原生 hook 使用 Pod loopback 9445；不发布到公网。API Server 能访问 Guard Service 443。
- Coordinator 9446、Scheduler 内部观测 9447、adapter 9444、SSF Node 9443 按清单互通；原生 Gateway/节点业务端口按原 base 保留。
- `/var/lib/ssf/overlaybd`、repository、原 AgentENV workspace 均持久；emptyDir 只用于可重建配置/socket，不能替代 WAL/upper。
- Guard 与 Coordinator 的 profile 完全一致。新协议缺 capability 时更新整套镜像，不跳过校验。

## 11. 第一次功能验证

先验证**无卷原生冷启动**，确认镜像、KVM、guest agent 和原 Gateway 链路正常，再测试存储。
`AENV_IMAGE` 是原 `/sandboxes-cold` 请求中的沙箱工作负载 image，不是六个服务端镜像之一。
如果尚无可启动的 workload 镜像，先按当前 AgentENV 原生部署和镜像使用说明完成无卷启动，不能任意填一个 CSI 镜像。

本地访问 Gateway 可在单独终端执行端口转发，服务端口以 `k get svc agentenv-gateway` 为准：

```bash
k -n "$NS" get service agentenv-gateway
k -n "$NS" port-forward service/agentenv-gateway 8080:8080
```

随后按 [29 — 两只沙箱共用一份笔记](29-overlaybd-pvc-manual-validation.md) 手动执行：

1. Kubernetes 创建 Block PVC。
2. 原创建 API 在 `volumeMounts` 中引用 `pvc.namespace/name`，挂到 `/data`。
3. A 写文件；核对实际 OverlayBD 层和 `/dev/ublkb*` 会话。
4. 删除 A，保留 PVC，等待最终发布与释放。
5. B 引用同一 PVC，读取原文件。

Immediate 和 WFFC 分别用渲染出的类验证。WFFC 没有沙箱时 Pending 是正常现象；不要创建 Pod 触发绑定。
`aenv exec` 是原生 CLI；不在 PATH 时先安装/构建 CLI 或使用其绝对路径，不能把服务器二进制作为 CLI。

## 12. 共享 POSIX/NFS、OSS 与离线扩容

### 12.1 共享文件系统

用已验证的原 AgentENV 共享存储方式，把 inventory 的 repository volume 改成真实共享挂载，例如：

```json
"repository_volume": {"nfs":{"server":"实际NFS服务器","path":"/专用SSF导出目录"}}
```

同一 repository 的节点使用同一 repository ID、后端配置，`accessible_nodes: []`；每个节点仍有独立 worker endpoint。
共享仓库的 worker 当前固定为 inventory 中第一条，没有自动 worker 故障转移。
不要用每节点普通 hostPath 加空拓扑来冒充共享目录。用于承载 repository 的已有共享 PVC 也不能来自这套尚在引导中的 OverlayBD Driver。

### 12.2 OSS

依据 [原生 OSS 配置](../sandbox-storage/drivers/overlaybd/native/config.oss.example.json)，在每条节点记录加入 `repository_backend: "oss"`、`accessible_nodes: []` 和完整 `oss` 对象。
使用独立 bucket/prefix；access key 值放文件，配置只写 `/etc/ssf-overlaybd/oss/access-key-id` 和 `secret-access-key` 路径。

```bash
k -n "$NS" create secret generic ssf-overlaybd-oss \
  --from-file=access-key-id="$OUT/oss-access-key-id" \
  --from-file=secret-access-key="$OUT/oss-secret-access-key" \
  --dry-run=client -o yaml | k apply -f -
```

上传、条件发布及原 OverlayBD 读取链路都必须验证。ConfigMap 中不放明文 key；共享 OSS 不意味着本地 upper/WAL 可以丢弃。
切换 backend 要创建新隔离环境/新仓库，不能给已有 repository ID 直接换 bucket 或 prefix。

### 12.3 离线扩容

初次主线默认不开启。新环境需要测试扩容时，在渲染前向 inventory 增加：

```json
"offline_expansion": true,
"overlaybd_tools_volume": {"hostPath":{"path":"/实际原生OverlayBD工具目录","type":"Directory"}}
```

目录内必须有原 AgentENV 已验证版本的 `bin/overlaybd-resize`，并且每个 Worker 路径一致。
重新生成三份清单和 Guard 配置，确认 profile 有 `controllerOfflineExpansion: true`、SC 有 `allowVolumeExpansion: true`、Controller 有 resizer。
对已有 PVC 不修改不可变 profile revision。完整步骤见 [Driver 部署说明](../sandbox-storage/drivers/overlaybd/deploy/README.md) 和 [29 第 6 节](29-overlaybd-pvc-manual-validation.md#6-可选通过原生-pvc-请求离线扩容)。
暂停 hold 仍会阻止扩容；先删除测试沙箱并确认释放，不能用 pause 代替离线。

## 13. 常见问题

| 现象 | 先检查 | 不应采用的处理 |
| --- | --- | --- |
| ImagePullBackOff | 六个镜像及官方 sidecar 的地址、仓库凭据、架构 | 改成不匹配协议的旧镜像 |
| Node init 不通过 | inventory 是否含真实 Node 名、SAN、配置路径 | 默认回退其他节点 repository |
| x509/permission denied | CA、SAN、SPIFFE URI、socket 目录权限 | 关闭 mTLS、放宽所有 peer |
| Guard 不就绪 | 公共 schema 已迁移、只读 grant、DB URL/CA | 忽略 webhook 继续建卷 |
| PVC Immediate 长期 Pending | provisioner、Controller → worker、K8S 事件 | 手工 mkfs 或新建初始化 Pod |
| WFFC Pending | 首次消费前属正常；消费后检查 Scheduler 与绑定授权 | 创建 Holder Pod 绕开选点 |
| 已 Bound 但无法创建 VM | preparation、Attach、Node、guest 核验、原生 KVM | 把目录存在视为挂载成功 |
| NeedsRecovery | 原 operation、设备/session 身份、停止证据、upper/WAL | 删除 WAL、删除设备或重新格式化 |
| 删除 PVC 不结束 | 是否存在运行/暂停/快照/准备引用 | 手工去 finalizer 强制回收 |
| missing durable-restack-v1 | daemon、宿主、Node/worker 镜像是否同套 | 伪造能力列表 |

查看节点日志时，native sidecar 属于 `initContainers`，仍使用普通 `kubectl logs POD -c 容器名`。
证据可能含内部身份和 API 响应 token，分享前脱敏，不上传整个 Secret 或数据库 URL。

## 14. 停止、升级和卸载

1. 停止提交新创建/恢复请求，使用原 API 结束测试沙箱；检查最终发布、FD 解除、Node/attachment 释放。
2. 按测试记录处理快照和 PVC。Retain 后端可能继续保留，这是预期行为，不自动删除 repository。
3. 保留数据库、WAL、upper、manifest 和凭据备份，再安排组件维护。
4. 正常 Pod 退出依赖 native sidecar 反向顺序；3600 秒宽限期不是断电恢复保证，不使用强制删 Pod 代替 drain。
5. 升级须保持协议、schema、profile、镜像和证书一致。本版本没有旧运行在线迁移方案，不能承诺升级/回滚到旧二进制可兼容现有状态。
6. 卸载 webhook/Guard 前确认已无受管对象和引用；卸载 cluster-scoped 对象前确认没有其他部署使用。不要直接 `kubectl delete -f` 所有文件或删除 namespace 来当作数据清理方案。

## 15. Standalone 边界与验收记录

standalone 保留原 `agentenv.service`，附加公共 Coordinator、SSF Node、adapter、专属 daemon/宿主/CSI Node/worker。
按 [adapter standalone](../sandbox-storage/adapter/agentenvadapter/deploy/standalone/README.md) 和 [OverlayBD standalone](../sandbox-storage/drivers/overlaybd/deploy/standalone/README.md) 安装对应 unit 和配置。
PVC 用法仍依赖 Kubernetes、公共快照组件、Guard、CSI sidecar 和 PostgreSQL；不能把 standalone 理解为无需 K8S 的 PVC 服务。原无 K8S legacy 用法不需要这组新服务。

本部署指南的主线是 Kubernetes；现有 systemd 材料尚未完成联合 drain 真机验收，不能宣称与上面的 Kubernetes 步骤具有同等验证结果。

| 验收项 | 本文编写时状态 | 部署后填写证据 |
| --- | --- | --- |
| 代码/模板/命令参数核对 | 已完成 | 源码 revision/工作区哈希 |
| 文档配置与本地清单组装 | 本机离线检查，非集群验证 | 清单及镜像 digest |
| 六个镜像构建并启动 | 未执行 | 构建日志/Pod 版本 |
| Guard/Coordinator/Driver 就绪 | 未执行 | 事件、日志 |
| 原生无卷 API、代理、legacy | 未执行 | 原生兼容回归 |
| Immediate/WFFC 数据保留主场景 | 未执行 | PVC/PV UID、会话/head、文件读回 |
| 快照/Fork/模板/暂停恢复 | 未执行 | 生命周期回归 |
| 扩容与异常恢复 | 未执行；部分恢复代码仍缺失 | 故障点、恢复结果、未完成项 |
| 共享 POSIX/OSS | 未执行 | 真实后端证据 |

安装成功应以真实 API/guest/存储证据判定，不能只看 Pod Running。后续测试入口统一从 [29 手动验证](29-overlaybd-pvc-manual-validation.md) 开始，当前能力边界以 [28](28-overlaybd-csi-implementation-status.md) 为准。

文档离线复核记录：22 个 Bash 代码块通过语法检查；3 段 Python inventory/清单组装代码已实际执行；原生 Kustomize 渲染、部署结构检查、profile 一致性、容器卷引用和本地文档链接检查通过。使用测试输入，未执行凭据签发、数据库安装、镜像构建或任何集群命令。
