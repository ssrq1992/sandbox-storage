# 手动端到端测试：用同一个 PVC 保存一份笔记

> 当前部署使用独立 `ssf-coordinator`，安装入口见 [13](13-agentenvadapter-installation-and-deployment.md)，原用户 API 和以下数据验证主线保持不变。服务/故障追加验收见 [14](14-agentenvadapter-real-environment-test-plan.md)。旧 Scheduler 内嵌控制部署不适用于本轮。

## 1. 场景与通过标准

通过 Kubernetes 创建一个 PVC，创建沙箱 A 将它挂到 `/data`，写入一份笔记；删除 A 后，创建全新的沙箱 B 挂载同一个 PVC，读回笔记。

```text
Kubernetes 创建 PVC ssf-notes
          ↓
沙箱 A：挂载 /data → 写入 notes.txt → 删除 A
          ↓ PVC 保留
沙箱 B：挂载 /data → 读到 A 写入的 notes.txt
```

**通过标准：A、B 是不同的沙箱，同一个 PVC/PV 没有被替换，B 能从实际挂载的 `/data` 读取 A 写入的完整内容。**

本文全部为手动操作，请按步骤逐段执行并检查结果，不需要运行自动化测试脚本。这里细分步骤是为了观察结果；业务操作仍然只有“Kubernetes 建卷”和“原 AgentENV 接口创建沙箱并引用卷”。无需初始化 Pod、手动格式化或 `/volumes import`。

本文提供待执行步骤，不代表已在你的集群验证通过。测试覆盖新建空白 Block 卷的准备、挂载、写入、沙箱删除及跨沙箱持久化；不代表快照、Fork、故障恢复等完整生命周期验收。

## 2. 开始前：确认环境和测试参数

假设已完成 [部署增量](22-volume-preparation-implementation.md#部署增量)，使用支持自动准备及直接 PVC 引用的新版本 Gateway/Scheduler/节点插件，已启用 Guard 和 `agentenv-block-ext4-v1` profile。下面使用部署中已有的专用 StorageClass，不需要测试人员重新部署组件。

| 参数 | 本文示例 | 需要确认的条件 |
| --- | --- | --- |
| Kubernetes namespace | `agentenv-system` | 已存在，且 API Key 对应主体有权使用其中的 PVC |
| StorageClass | `agentenv-block` | 支持 Block；绑定模式可以是 Immediate 或 WaitForFirstConsumer |
| 卷准备 profile | `agentenv-block-ext4-v1` | StorageClass 的 `ssf.dev/volume-profile` 注解已指向它 |
| PVC | `ssf-notes` | 专用于本次测试，开始前不存在 |
| 沙箱镜像 | 按实际填写 | 已验证能通过原冷启动接口启动，包含 envd、`sh`、`grep`、`cat` 和 `sync` |
| 本地工具 | `kubectl`、`curl`、`jq`、`aenv` | 能访问同一套测试环境 |

所有命令在同一个终端执行。先查看当前 Kubernetes 上下文和 StorageClass：

```bash
kubectl config current-context
kubectl get namespace agentenv-system
kubectl get storageclass agentenv-block -o json |
  jq '{name:.metadata.name,provisioner:.provisioner,bindingMode:.volumeBindingMode,reclaimPolicy:.reclaimPolicy,profile:.metadata.annotations["ssf.dev/volume-profile"]}'
kubectl get pvc ssf-notes -n agentenv-system
```

预期：上下文是目标测试集群，profile 为 `agentenv-block-ext4-v1`，PVC 查询返回 `NotFound`。记录绑定模式，后面决定是否等待自动准备。若同名 PVC 已存在，请改用新测试名称，并同步修改后文的 YAML、JSON 和查询命令；不要删除不明用途的卷。

填写原 Gateway 地址（末尾不带 `/`）、镜像和 API Key：

```bash
export AENV_URL='https://你的原Gateway地址'
export AENV_IMAGE='你的已验证可启动镜像'
export AENV_API_KEY='你的测试API密钥'
export TEST_DIR="$(mktemp -d /tmp/ssf-notes-manual.XXXXXX)"
echo "$TEST_DIR"
```

`TEST_DIR` 只保存本次请求和响应，方便后续人工检查。有私有 CA 时为 `curl` 配置 `CURL_CA_BUNDLE`，并确保 CLI 也信任该 CA。

配置原生 CLI：

```bash
aenv auth
```

在交互提示中输入与 `AENV_URL` **相同**的 Gateway 地址和同一个 API Key。`aenv exec` 读取 `aenv auth` 保存的配置，不会直接读取上面的 `AENV_URL` 环境变量。

## 3. 创建 PVC

执行：

```bash
kubectl create -f - <<'YAML'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ssf-notes
  namespace: agentenv-system
spec:
  storageClassName: agentenv-block
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
YAML
```

预期输出：

```text
persistentvolumeclaim/ssf-notes created
```

这里不填写 PV、CSI volume handle 或节点。动态供给时，由 StorageClass 对应的 CSI provisioner 创建后端卷和 PV。

## 4. 查看绑定和准备状态

执行以下命令；需要等待时，隔一会儿手动再执行一次：

```bash
kubectl get pvc ssf-notes -n agentenv-system -o json |
  jq '{uid:.metadata.uid,phase:.status.phase,pv:.spec.volumeName,preparation:.metadata.annotations["ssf.dev/preparation-state"]}'
```

按第 2 节看到的绑定模式处理：

| StorageClass 绑定模式 | 此时预期 | 下一步 |
| --- | --- | --- |
| `Immediate` | 没有沙箱也会自动准备，最终 `phase=Bound`、`preparation=VolumeReady` | 看到这两个状态后，进入第 5 节 |
| `WaitForFirstConsumer`（WFFC） | 可以保持 `Pending`、`WaitingConsumer` | **直接进入第 5 节，不要在这里等待 Bound**；创建沙箱时 Scheduler 选点并推进绑定和准备 |

`Bound` 表示 PVC/PV 绑定成功，`VolumeReady` 表示内容准备和临时资源释放完成，两者都不单独证明 guest 内已经挂载。刚创建时准备注解可能尚未出现；持续不推进或出现 `Failed` / `NeedsRecovery` 时，按第 11 节检查。

## 5. 创建沙箱 A，直接引用 PVC

先生成创建请求文件：

```bash
jq -n --arg image "$AENV_IMAGE" '{
  image:$image,
  timeout:3600,
  volumeMounts:[{
    pvc:{namespace:"agentenv-system",name:"ssf-notes"},
    path:"/data"
  }]
}' > "$TEST_DIR/create.json"
cat "$TEST_DIR/create.json"
```

关键字段只有 `volumeMounts[].pvc` 和 `path`。使用 PVC 来源时，同一项不再填写卷别名 `name`；PVC 对象内部的 `name` 是 PVC 名称。用户不需要提供 UID。

调用原 AgentENV 冷启动接口：

```bash
curl -sS --fail-with-body \
  -D "$TEST_DIR/a-headers.txt" -o "$TEST_DIR/a.json" \
  -w 'HTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" \
  -H 'Content-Type: application/json' \
  --data-binary @"$TEST_DIR/create.json" \
  "$AENV_URL/sandboxes-cold"
cat "$TEST_DIR/a.json"
```

预期 **HTTP 201**，响应包含非空 `sandboxID`。WFFC 首次创建会包含选点、供卷、准备和挂载过程，可能比复用已就绪卷耗时更长。

确认成功后保存 A 的 ID：

```bash
export SANDBOX_A="$(jq -er '.sandboxID' "$TEST_DIR/a.json")"
echo "$SANDBOX_A"
```

如果创建失败，不要继续执行后续步骤。若网络断开导致结果未知，先通过原生沙箱查询及服务端请求日志确认是否已经创建成功，不要盲目重复 POST。

## 6. 在 A 中确认挂载并写入笔记

先确认 `/data` 是实际挂载点：

```bash
aenv exec "$SANDBOX_A" sh -c 'grep " /data " /proc/mounts'
```

预期输出包含挂载路径 `/data`、文件系统类型 `ext4` 和挂载选项 `rw`。设备名称由运行时决定，不要求固定为 `/dev/vdb`。若没有输出，或是只读挂载，先停止测试并检查，不能仅凭目录存在就判定挂载成功。

写入并落盘：

```bash
aenv exec "$SANDBOX_A" sh -c 'printf "hello from sandbox A\nthis file lives on the PVC\n" > /data/notes.txt && sync && cat /data/notes.txt'
```

预期输出：

```text
hello from sandbox A
this file lives on the PVC
```

此时记录 PVC/PV 身份，用于之后人工对比：

```bash
kubectl get pvc ssf-notes -n agentenv-system -o json |
  jq '{pvcUID:.metadata.uid,pv:.spec.volumeName,phase:.status.phase}' |
  tee "$TEST_DIR/pvc-before.json"
export TEST_PV="$(kubectl get pvc ssf-notes -n agentenv-system -o jsonpath='{.spec.volumeName}')"
kubectl get pv "$TEST_PV" -o json |
  jq '{pv:.metadata.name,pvUID:.metadata.uid}' |
  tee "$TEST_DIR/pv-before.json"
```

这只是测试观察，不是创建接口的必填输入。查询 PV 需要集群级只读权限；没有权限时请管理员协助记录，不影响普通用户通过 PVC 创建沙箱。

## 7. 删除 A，保留 PVC

只删除沙箱：

```bash
curl -sS --fail-with-body -X DELETE \
  -H "X-API-Key: $AENV_API_KEY" \
  -w '\nHTTP %{http_code}\n' \
  "$AENV_URL/sandboxes/$SANDBOX_A"
```

预期 **HTTP 204**。查看卷：

```bash
kubectl get pvc ssf-notes -n agentenv-system -o json |
  jq '{pvcUID:.metadata.uid,pv:.spec.volumeName,phase:.status.phase,deletionTimestamp:.metadata.deletionTimestamp}'
```

预期 PVC 仍为 `Bound`，UID 和 PV 名称与第 6 节一致，`deletionTimestamp` 为空。**不要执行 `kubectl delete pvc`，下一步还要用它。**

## 8. 创建沙箱 B，读取 A 的笔记

复用同一份请求，创建一个新的沙箱：

```bash
curl -sS --fail-with-body \
  -D "$TEST_DIR/b-headers.txt" -o "$TEST_DIR/b.json" \
  -w 'HTTP %{http_code}\n' \
  -H "X-API-Key: $AENV_API_KEY" \
  -H 'Content-Type: application/json' \
  --data-binary @"$TEST_DIR/create.json" \
  "$AENV_URL/sandboxes-cold"
cat "$TEST_DIR/b.json"
```

预期 **HTTP 201**。确认成功后：

```bash
export SANDBOX_B="$(jq -er '.sandboxID' "$TEST_DIR/b.json")"
printf 'A=%s\nB=%s\n' "$SANDBOX_A" "$SANDBOX_B"
aenv exec "$SANDBOX_B" sh -c 'grep " /data " /proc/mounts && cat /data/notes.txt'
```

预期 A、B 的 ID 不同；`/data` 仍为 ext4 实际挂载点，文件内容仍为：

```text
hello from sandbox A
this file lives on the PVC
```

再确认 B 可以继续写入：

```bash
aenv exec "$SANDBOX_B" sh -c 'printf "hello from sandbox B\n" >> /data/notes.txt && sync && cat /data/notes.txt'
```

预期原两行保留，末尾新增 `hello from sandbox B`。

检查卷身份：

```bash
kubectl get pvc ssf-notes -n agentenv-system -o json |
  jq '{pvcUID:.metadata.uid,pv:.spec.volumeName,phase:.status.phase}'
kubectl get pv "$TEST_PV" -o json |
  jq '{pv:.metadata.name,pvUID:.metadata.uid}'
cat "$TEST_DIR/pvc-before.json"
cat "$TEST_DIR/pv-before.json"
```

人工对比前后 PVC UID、PV 名称、PV UID，均应相同。到这里，主场景验证完成。

如果 B 创建时返回使用权/writer 冲突，先查看 A 的删除清理是否完成；API 删除成功与后台资源释放需要结合日志确认。不得通过删除使用权记录或强制解绑绕过冲突。

## 9. 填写测试结果

| 检查项 | 预期 | 实际结果（手工填写） |
| --- | --- | --- |
| 集群、SC、Driver、绑定模式、测试时间 | 记录真实环境信息 | |
| PVC 创建 | 创建成功 | |
| Immediate 自动准备（仅 Immediate） | 无沙箱时 Bound / VolumeReady | |
| WFFC 首次使用（仅 WFFC） | 不创建初始化 Pod；创建沙箱后完成绑定和准备 | |
| A 创建 | HTTP 201；记录 A ID | |
| A 挂载与写入 | `/data` 为 ext4、rw；两行文本正确 | |
| A 删除 | HTTP 204；PVC 保留 | |
| B 创建 | HTTP 201；B ID 与 A 不同 | |
| B 读写 | 读到 A 两行原文；可追加第三行 | |
| 卷身份 | PVC UID、PV 名称和 UID 均未变化 | |
| 总结 | 通过 / 失败 / 未执行，并记录原因 | |

使用哪一种绑定模式，就记录哪一种的结果；未执行的另一种填写“未执行”，不能推断通过。若要覆盖两种模式，使用另一个已配置的 StorageClass 和全新测试 PVC，手动重复同一场景即可。

## 10. 测试结束后清理（可选）

先删除 B：

```bash
curl -sS --fail-with-body -X DELETE \
  -H "X-API-Key: $AENV_API_KEY" \
  -w '\nHTTP %{http_code}\n' \
  "$AENV_URL/sandboxes/$SANDBOX_B"
```

预期 **HTTP 204**。可以保留 PVC，之后再创建沙箱检查数据。

如果确认不再需要笔记，且当前 PVC UID 仍与本次记录一致，再手动删除测试 PVC：

```bash
kubectl get pvc ssf-notes -n agentenv-system -o jsonpath='{.metadata.uid}{"\n"}'
cat "$TEST_DIR/pvc-before.json"
```

确认后执行：

```bash
kubectl delete pvc ssf-notes -n agentenv-system --wait=false
kubectl get pvc ssf-notes -n agentenv-system
```

最终预期 `NotFound`；中间可能处于 `Terminating`，等待 SSF 排空引用。若之前某步失败，请先确认本次创建的所有沙箱都已删除。不要强制移除 finalizer。StorageClass/PV 的回收策略为 `Delete` 时，删除 PVC 可能进一步删除后端数据；为 `Retain` 时，PV 和后端卷由管理员按原 Kubernetes 流程后续处理。

## 11. 失败时只检查对应环节

先保留失败响应和现场，执行：

```bash
kubectl describe pvc ssf-notes -n agentenv-system
kubectl get pvc ssf-notes -n agentenv-system -o yaml
kubectl get pods -n agentenv-system
```

根据实际 Pod 和容器名查看 Scheduler、节点插件或 Guard 日志：

```bash
kubectl logs -n agentenv-system <实际Pod名称> -c <实际容器名称> --tail=200
```

| 现象 | 优先检查 |
| --- | --- |
| 创建 PVC 被拒绝 | namespace 权限、配额、StorageClass/profile、Guard 返回原因 |
| Immediate 长期 Pending | PVC Events、CSI provisioner、存储容量和拓扑 |
| WFFC 在创建沙箱前 Pending | 正常，继续第 5 节；不需要初始化 Pod |
| Bound 后一直没有 VolumeReady | profile 注解、准备协调器、可用节点、节点通信及 CSI Node 服务 |
| Failed / NeedsRecovery | 准备失败原因和原 operation 的恢复记录；不要手动 mkfs 或换操作绕过恢复 |
| 创建接口 401/403 | API Key、Gateway 地址、主体对 PVC namespace 的授权 |
| 创建接口报不支持 PVC/协议 | 确认部署的是本次更新后的完整组件版本 |
| curl 成功但 aenv exec 失败 | `aenv auth` 是否指向同一 Gateway，CLI 信任证书、envd 和原生代理是否正常 |
| A/B 的 `/data` 没有挂载记录 | 节点 RuntimeMount、guest 核验和启动日志；停止数据验证 |
| B 读不到文件 | PVC/PV UID 是否一致、A 是否写入并 sync、两次实际挂载路径；不要重新初始化卷 |
| 删除 PVC 后一直 Terminating | 是否仍有准备、运行、暂停或快照引用；查看排空日志，不移除 finalizer |

本测试不要求修改部署。需要排查安装配置或开展更广的生命周期验收时，再参考 [实现、部署与验证记录](22-volume-preparation-implementation.md)。共享测试记录时不包含 API Key 或其他认证凭据。
