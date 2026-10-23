# OverlayBD PVC 手动验证：两只沙箱共用一份笔记

本文是手动验证步骤；下文已填入本次环境的实际参数，并在第 7 节记录了一次实际执行结果。当前实现和恢复缺项见 [28](28-overlaybd-csi-implementation-status.md)。
只在隔离测试环境执行。先用一个 Worker，减少节点选择对排查的干扰；不需要初始化 Pod、
手工格式化或 `/volumes import`。

主场景：**Kubernetes 创建 PVC → A 挂载并写入 → 删除 A → B 挂同一 PVC 并读回**。
业务动作只有建 PVC 和调用原 AgentENV API；下面的查询用于人工确认结果。

## 1. 填写已有环境参数

需要原 Gateway、Scheduler、AgentENV 节点，以及已安装的 SSF/OverlayBD 增量。
安装说明在 [Driver 部署文档](../sandbox-storage/drivers/overlaybd/deploy/README.md)。

```bash
export KUBE_CONTEXT='kubernetes-admin@kubernetes'
export TEST_NAMESPACE='ssf-overlaybd-test'
export OVERLAYBD_SC='overlaybd-agentenv-overlaybd-node-a-d252b89f-immediate'
export AENV_URL='http://127.0.0.1:18080'
export AENV_IMAGE='docker.io/library/ubuntu:24.04'
export TEST_DIR="$(mktemp -d /tmp/overlaybd-notes.XXXXXX)"
```

若要验证 WFFC，把 `OVERLAYBD_SC` 换成
`overlaybd-agentenv-overlaybd-node-a-d252b89f-wffc`，并换一个新的 PVC 名称。

`AENV_IMAGE` 是**沙箱工作负载镜像引用**，直接复用你此前调用 `/sandboxes-cold` 成功时
提交的 `image`。它不是 Gateway、AgentENV 节点或 CSI Driver 的部署镜像。
API Key 使用当前环境的 `AENV_API_KEY`；有私有 CA 时配置 `CURL_CA_BUNDLE`。

本机需要 `kubectl`、`curl`、`jq` 和原生 `aenv` CLI。确认：

```bash
command -v aenv
kubectl --context "$KUBE_CONTEXT" get namespace "$TEST_NAMESPACE"
kubectl --context "$KUBE_CONTEXT" get sc "$OVERLAYBD_SC" -o json |
  jq '{name:.metadata.name,driver:.provisioner,binding:.volumeBindingMode,profile:.metadata.annotations["ssf.dev/volume-profile"]}'
```

预期 Driver 为 `overlaybd.csi.ssf.dev`，profile 为 `agentenv-overlaybd-ext4-v1`。
SC 名称由部署 inventory 生成，不要照抄 generic CSI 的 `agentenv-block`。

若 `aenv` 不在 PATH，使用已有 CLI 的绝对路径；在当前源码构建了 release CLI 的情况下，
默认产物为 `AgentENV/target/release/aenv`，没有构建就不会存在。不要把 `agentenv` 服务端
二进制当成 CLI。后面的 `aenv exec` 仅用于向 guest 执行命令，不参与卷创建或导入。
执行 `aenv auth`，填写与 `AENV_URL` 相同的 Gateway 和 API Key。

## 2. 通过 Kubernetes 创建 PVC

先确认同名 PVC 不存在；若已存在，换一个专用名称，不删除未知卷。

```bash
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" get pvc overlaybd-notes
```

返回 NotFound 后创建：

```bash
kubectl --context "$KUBE_CONTEXT" create -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: overlaybd-notes
  namespace: ${TEST_NAMESPACE}
spec:
  storageClassName: ${OVERLAYBD_SC}
  volumeMode: Block
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
YAML
```

观察：

```bash
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" get pvc overlaybd-notes -o json |
  jq '{uid:.metadata.uid,phase:.status.phase,pv:.spec.volumeName,preparation:.metadata.annotations["ssf.dev/preparation-state"]}'
```

- **Immediate**：没有沙箱也应最终 Bound / VolumeReady，然后进入下一步。
- **WFFC**：此时 Pending / WaitingConsumer 可以是正常状态，直接进入下一步，由原 Scheduler
  选择计算节点并完成绑定。不要创建 Pod 来“触发绑定”。

出现 Failed / NeedsRecovery 时停止主场景，保留卷及日志排查，不删除 WAL 或重新格式化。

## 3. 原 API 创建 A，写入笔记

```bash
jq -n --arg image "$AENV_IMAGE" --arg ns "$TEST_NAMESPACE" \
  '{image:$image,timeout:3600,volumeMounts:[{pvc:{namespace:$ns,name:"overlaybd-notes"},path:"/data"}]}' \
  > "$TEST_DIR/create.json"

curl -sS --fail-with-body -w '\nHTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" -H 'Content-Type: application/json' \
  --data-binary @"$TEST_DIR/create.json" -o "$TEST_DIR/a.json" \
  "$AENV_URL/sandboxes-cold"
cat "$TEST_DIR/a.json"
```

预期 HTTP **201**、非空 `sandboxID`。若响应丢失，先查原生列表及请求日志确认结果，
不要盲目再创建一次。

```bash
export SANDBOX_A="$(jq -er '.sandboxID' "$TEST_DIR/a.json")"
aenv exec "$SANDBOX_A" sh -c 'grep " /data " /proc/mounts'
aenv exec "$SANDBOX_A" sh -c 'printf "hello from OverlayBD PVC\n" > /data/notes.txt && sync && cat /data/notes.txt'
```

第一条必须看到 `/data`、ext4、rw；目录存在不等于卷已挂载。第二条应读到完整笔记。

## 4. 确认实际走 OverlayBD

记录 PVC/PV 身份：

```bash
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" get pvc overlaybd-notes -o json |
  jq '{uid:.metadata.uid,pv:.spec.volumeName}' | tee "$TEST_DIR/pvc-before.json"
export TEST_PV="$(jq -r '.pv' "$TEST_DIR/pvc-before.json")"
kubectl --context "$KUBE_CONTEXT" get pv "$TEST_PV" -o json |
  jq '{uid:.metadata.uid,driver:.spec.csi.driver,handle:.spec.csi.volumeHandle}' |
  tee "$TEST_DIR/pv-before.json"
export VOLUME_HANDLE="$(jq -r '.handle' "$TEST_DIR/pv-before.json")"
kubectl --context "$KUBE_CONTEXT" -n agentenv-system get pods \
  -l app.kubernetes.io/name=agentenv-node -o wide
```

Driver 必须是 `overlaybd.csi.ssf.dev`，handle 以 `obd1.` 开头。单 Worker 环境下，将原
AgentENV 节点 Pod 名称填入下方；多 Worker 则需要检查实际运行 A 的节点。

```bash
export NODE_POD='agentenv-node-tfnb8'
kubectl --context "$KUBE_CONTEXT" -n agentenv-system exec "$NODE_POD" \
  -c overlaybd-daemon -- overlaybd-evidence -volume-handle "$VOLUME_HANDLE" |
  tee "$TEST_DIR/a-storage.json"
```

结果应包含对应卷的 `sessions`：`live_block:true`、`/dev/ublkb*`、非空 identity；session
的 `head` 应在 `heads` 中有同 revision、非零 layers 的记录。该工具只读取已记录状态及
设备信息，不会创建、挂载或修复设备。它也不是生产清理授权。

只有 HTTP 201 或 guest 目录可写不够：没有这些原生记录，不能把 generic CSI 结果记为
OverlayBD 验证通过。此查询需要测试管理员权限，用户创建沙箱时不需要提交 handle。

## 5. 删除 A，再创建 B 读回

```bash
curl -sS --fail-with-body -X DELETE -w '\nHTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" "$AENV_URL/sandboxes/$SANDBOX_A"
```

预期 HTTP **204**。等待原最终发布/释放完成；如果后续创建报告旧运行尚未释放，查看
相关任务和日志，不强制删除 attachment。PVC 应仍存在，UID/PV 保持不变。

已知问题：释放时 `/var/lib/ssf/publish/<hash>` 可能残留 devtmpfs bind-mount，导致设备
stop 返回 EBUSY。手工 `umount` 该挂载点后，重试通常可以继续。

```bash
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" get pvc overlaybd-notes -o json |
  jq '{uid:.metadata.uid,pv:.spec.volumeName,deletionTimestamp:.metadata.deletionTimestamp}'

curl -sS --fail-with-body -w '\nHTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" -H 'Content-Type: application/json' \
  --data-binary @"$TEST_DIR/create.json" -o "$TEST_DIR/b.json" \
  "$AENV_URL/sandboxes-cold"
export SANDBOX_B="$(jq -er '.sandboxID' "$TEST_DIR/b.json")"
printf 'A=%s\nB=%s\n' "$SANDBOX_A" "$SANDBOX_B"
aenv exec "$SANDBOX_B" sh -c 'grep " /data " /proc/mounts && cat /data/notes.txt'
```

预期：A、B ID 不同，B 仍挂 ext4 `/data`，读到 `hello from OverlayBD PVC`。
再次执行第 4 节的 PV 和设备查询，保存为 `pv-after.json`、`b-storage.json`。PVC/PV UID
和 handle 必须保持不变；B 有新的运行会话，其 source head 应反映 A 最终发布后的版本。

这就完成了最小主场景。换另一个绑定模式的专用 StorageClass、新 PVC 名称，重复一次，
分别记录 Immediate 和 WFFC 结果。

## 6. 可选：通过原生 PVC 请求离线扩容

仅在部署显式启用 offlineExpansion、profile 和 resizer 时执行。先删除 B，确认运行资源
释放；暂停 hold 也会阻止离线扩容，不能用 pause 代替删除。

```bash
curl -sS --fail-with-body -X DELETE -w '\nHTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" "$AENV_URL/sandboxes/$SANDBOX_B"
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" patch pvc overlaybd-notes \
  --type merge -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}'
kubectl --context "$KUBE_CONTEXT" -n "$TEST_NAMESPACE" get pvc overlaybd-notes -o json |
  jq '{requested:.spec.resources.requests.storage,capacity:.status.capacity.storage,conditions:.status.conditions}'
```

等待 PVC/PV 容量均为 2Gi 后，使用原创建请求创建 C：

```bash
curl -sS --fail-with-body -w '\nHTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" -H 'Content-Type: application/json' \
  --data-binary @"$TEST_DIR/create.json" -o "$TEST_DIR/c.json" \
  "$AENV_URL/sandboxes-cold"
export SANDBOX_C="$(jq -er '.sandboxID' "$TEST_DIR/c.json")"
aenv exec "$SANDBOX_C" sh -c 'cat /data/notes.txt && df -B1 /data'
```

应读到原笔记，文件系统容量增加。再次运行第 4 节证据命令，当前 session 对应 head 的
`filesystem_uuid` 应与扩容前一致，`capacity_bytes` 为 2147483648、`generation` 增加。
`df` 的可用字节包含 ext4 元数据开销，不能简单要求它恰好等于 2Gi。
扩容错误或 NeedsRecovery 保留原维护任务，不通过创建新卷或删除 upper 来修复。

## 7. 记录与清理

| 检查 | 手工填写：通过/失败/未执行 |
| --- | --- |
| Immediate 无沙箱时自动就绪 | 通过 |
| WFFC 无初始化 Pod 首次使用 | 通过 |
| A 实际挂载、写入 | 通过 |
| PV 属于 OverlayBD，存在实际 ublk 会话及匹配层 head | 通过 |
| A 删除后 PVC 保留，最终内容发布 | 通过 |
| B ID 不同、卷 UID/handle 不变、笔记读回 | 通过 |
| 可选离线扩容后内容和 FS UUID 保留 | 未执行 |

本次主场景不覆盖全部暂停、快照、Fork、模板、混合卷及故障恢复矩阵。只有实际观察到
的项填写“通过”；未执行项保留“未执行”。证据文件可能包含沙箱访问 token，妥善保管。

测试完成后先通过原 API 删除仍在运行的测试沙箱，再删除**本次创建且已核对 UID 的**
PVC。StorageClass 的 Retain 表示后端可能保留；不要删除 finalizer、WAL 或直接删除
repository 目录。后端回收另按管理员批准的测试资源清单处理。
