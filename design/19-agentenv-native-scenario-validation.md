# 已部署环境的逐步功能验证：一份笔记本数据的完整生命周期

> 当前部署使用独立 `ssf-coordinator`，安装入口见 [13](13-agentenvadapter-installation-and-deployment.md)，原用户 API 和以下数据验证主线保持不变。服务/故障追加验收见 [14](14-agentenvadapter-real-environment-test-plan.md)。旧 Scheduler 内嵌控制部署不适用于本轮。

> 2026-10-21 自动卷准备增量：实现、部署、验证结果及未完成边界统一见 [22](22-volume-preparation-implementation.md)；直接 PVC 两步场景见 [20](20-kubernetes-pvc-agentenv-mount-validation.md)。本文件原生命周期说明继续适用，旧未启用 profile 的卷策略不代表新 profile 规则。
本文保留原 AgentENV Volume facade 的生命周期场景。直接由 Kubernetes 创建 PVC 并挂载的主线见 [20](20-kubernetes-pvc-agentenv-mount-validation.md)，公共自动准备及当前验收状态见 [22](22-volume-preparation-implementation.md)。

本文面向已经部署好的 AgentENV + SSF 环境。以一个简单场景验证功能：沙箱 A 把笔记写入 CSI 卷，暂停和恢复后继续编辑；拍摄 v1 快照后把原件改成 v2；Fork 两份副本分别编辑；删除原沙箱后，用原卷和快照恢复不同版本。

全部请求都通过**原 AgentENV Gateway API**，guest 文件读写通过原 envd 代理。本文提供操作步骤与判据，不代表这些步骤已经在你的集群执行。

## 1. 先看场景与通过标准

```text
CSI 卷 notes ──挂载──> A：写入 v1
                       │ 暂停 → 恢复 → v1 仍在
                       │ 快照 S（v1）
                       │ 原件修改为 v2
                       ├─ Fork B：修改为 fork-b
                       ├─ Fork C：仍为 v2
                       └─ 删除 A
CSI 原卷 notes ──────> A2：仍为 v2
快照 S ─────────────> T：恢复为 v1；编辑 T 不影响 A2/B/C
快照 S + 空挂载 ────> E：看不到原笔记
快照 S + 替换卷 ────> R：新卷为空，可写入 replacement
```

| 步骤 | 验证内容 | 通过标准 |
| --- | --- | --- |
| 3～4 | CSI 卷创建、原生创建、guest 挂载与读写 | provider 为 ssf；创建 201；guest 能读回 v1；PVC Bound |
| 5 | 排他写与暂停持有 | 活动和暂停期间第二 writer 都被 409 拒绝；恢复 ID 不变、数据仍为 v1 |
| 6 | 快照 | 源沙箱保持可读写；快照保存 v1，源可继续改成 v2 |
| 7 | Fork | 两个不同 child ID；只修改 B 后，C 和 A 仍为 v2 |
| 8 | 删除与持久卷 | 删除 A 后原卷仍存在；新建 A2 挂同一卷读到 v2 |
| 9 | 快照与模板继承 | 源 A 删除后仍能从 S 恢复 v1；T 的修改不影响其他实例 |
| 10 | 模板空挂载/替换 | `[]` 不继承可见挂载；替换卷没有旧笔记 |
| 11（可选） | 离线扩容 | 活动时 pending；释放原卷后完成扩容；重建后容量增加、数据保留 |
| 12 | 清理 | 先删本次沙箱，再删本次模板和已核对的测试卷；保留证据 |

主线默认验证**动态供卷 + 当前选定 StorageClass 的绑定模式 + 单个可写 Block/ext4 卷**。首次建议先完成步骤 1～9，再执行空挂载/替换和扩容。全部运行时最多同时保留约 6 个沙箱；Fork/模板还会创建独立卷和内部保留卷，请为这些实例和克隆预留容量。

## 2. 准备一个操作终端

### 2.1 填写环境参数

在能访问 Gateway 和 Kubernetes 的机器上操作，进入本次代码的 `sandboxVolume/SSF` 目录。使用同一个 **Bash** 会话顺序执行下文命令，便于保留变量。客户端只需要 `curl`、`jq`、`python3`、`kubectl`，不需要在客户端安装 KVM/Rust。

```bash
cd /你的代码路径/sandboxVolume/SSF
bash
set -euo pipefail
umask 077

export AENV_URL='https://你的原生Gateway地址'   # 不追加 /v1 或 /sandboxes
export KUBE_CONTEXT='你的kubectl-context'
export SSF_NAMESPACE='agentenv-system'       # 本次测试 PVC 所在 namespace
export SSF_STORAGE_CLASS='你的Block存储类'
export SSF_SNAPSHOT_CLASS='你的快照类'
export AENV_IMAGE='你的已验证可启动的镜像'
export SSF_TENANT='agentenv'                 # control 配置 principal.tenant，非 namespace
export AENV_ENVD_PORT='49983'
export AENV_CA_FILE=''                      # 系统已信任时留空；自签 CA 填绝对路径

read -r -s -p 'AgentENV API Key: ' AENV_API_KEY
printf '\n'
export AENV_API_KEY

export RUN_ID="ssf-notes-$(date +%Y%m%d-%H%M%S)-$(python3 -c 'import secrets; print(secrets.token_hex(3))')"
export RUN_DIR="$PWD/validation/manual/$RUN_ID"
export VOLUME="${RUN_ID}-data"
mkdir -p "$RUN_DIR"
printf '%s\n' "$RUN_ID" > "$RUN_DIR/run-id.txt"
printf '本次证据目录：%s\n' "$RUN_DIR"
```

镜像须具有原 AgentENV 可用的 envd，以及 `/bin/sh`、`cat`、`grep`、`sync` 等基础工具。扩容步骤还使用 GNU `stat -f -c`。如果使用 standalone，`AENV_URL` 填原生服务地址；跳过下面 Gateway/Scheduler 工作负载检查，其余公开 API 步骤相同。

响应文件可能含 guest 访问 token，目录以 0700/文件以 0600 创建；共享结果时只共享后面的脱敏汇总，不直接发布完整响应文件。下文不使用 `curl -k`。

### 2.2 确认已部署组件和存储规格

```bash
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }

k -n "$SSF_NAMESPACE" get deploy,ds,pods,svc -o wide
k get sc "$SSF_STORAGE_CLASS" -o json > "$RUN_DIR/storage-class.json"
k get volumesnapshotclass "$SSF_SNAPSHOT_CLASS" -o json > "$RUN_DIR/snapshot-class.json"
k get csinodes

jq '{provisioner,volumeBindingMode,reclaimPolicy,allowVolumeExpansion}' "$RUN_DIR/storage-class.json"
jq '{driver,deletionPolicy}' "$RUN_DIR/snapshot-class.json"

test "$(jq -r .provisioner "$RUN_DIR/storage-class.json")" = \
     "$(jq -r .driver "$RUN_DIR/snapshot-class.json")"
```

预期：原 Gateway、Scheduler 和节点工作负载就绪，节点上的 `agentenv`、`agentenv-storage`、`ssf-node` 就绪；SC 与 SnapshotClass 属于同一 Driver，按部署方案设置 Retain，Driver 支持 Block/ext4 和快照/克隆。扩容需要 `allowVolumeExpansion: true` 及 Driver 对应能力。

如果部署工作负载和测试卷不在同一 namespace，第一条 `get` 改成工作负载 namespace；`SSF_NAMESPACE` 保持测试卷 namespace。现有 `native_preflight.py` 还检查**执行机器本身**的 `/dev/kvm`、宿主目录和构建工具，适合在相应 Linux 节点做补充检查，不要把管理终端缺少 KVM 误判为集群故障。

### 2.3 定义请求和 guest 操作函数

以下函数只定义辅助操作，此时不会创建资源。`api` 保存原始响应及 HTTP 状态；状态不符或网络超时时停止，不自动重试有副作用的创建。

```bash
TLS_ARGS=()
if [[ -n "$AENV_CA_FILE" ]]; then TLS_ARGS=(--cacert "$AENV_CA_FILE"); fi

api() {
  local label="$1" method="$2" path="$3" expected="$4" body="${5:-}"
  local status
  local -a body_args=()
  if [[ -n "$body" ]]; then body_args=(--data-binary "@$body"); fi
  if ! status=$(curl --silent --show-error --connect-timeout 15 --max-time 600 \
      "${TLS_ARGS[@]}" -X "$method" \
      -H "X-API-Key: $AENV_API_KEY" -H 'Content-Type: application/json' \
      "${body_args[@]}" -o "$RUN_DIR/$label.json" -w '%{http_code}' \
      "${AENV_URL%/}$path"); then
    printf '请求结果未知：%s %s；先检查服务和已创建资源，不要直接重试 create。\n' "$method" "$path" >&2
    return 1
  fi
  printf '%s\n' "$status" > "$RUN_DIR/$label.http"
  printf '%s %s -> HTTP %s（预期 %s）\n' "$method" "$path" "$status" "$expected"
  case ",$expected," in
    *",$status,"*) ;;
    *) printf '状态不符，检查 %s/%s.json\n' "$RUN_DIR" "$label" >&2; return 1 ;;
  esac
}

cat > "$RUN_DIR/guest.py" <<'PY'
import argparse, json, os, ssl, sys, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'scripts'))
from native_lifecycle_e2e import Suite, NoRedirect
# 复用已有工具的原生 envd Connect 流解析及 exitCode 检查。
s = Suite.__new__(Suite)
s.args = argparse.Namespace(url=os.environ['AENV_URL'],
    envd_port=int(os.environ.get('AENV_ENVD_PORT', '49983')), timeout=120)
s.key = os.environ['AENV_API_KEY']
s.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(
    context=ssl.create_default_context(cafile=os.environ.get('AENV_CA_FILE') or None)))
print(s.guest(json.loads(Path(sys.argv[1]).read_text()), sys.argv[2]), end='')
PY

guest() { python3 "$RUN_DIR/guest.py" "$1" "$2"; }
```

`guest` 第一个参数必须使用**创建/恢复返回的完整响应文件**，这样能携带该实例的 envd/流量 token。不要用只有 sandboxID 的自制 JSON 替换。

## 3. 创建一个 SSF 笔记卷

```bash
jq -n --arg name "$VOLUME" --arg ns "$SSF_NAMESPACE" \
  --arg sc "$SSF_STORAGE_CLASS" --arg snap "$SSF_SNAPSHOT_CLASS" \
  '{name:$name,provider:"ssf",sizeMB:1024,mode:"exclusive",
    storage:{namespace:$ns,storageClass:$sc,snapshotClass:$snap}}' \
  > "$RUN_DIR/create-volume.request.json"

api 03-volume POST /volumes 201 "$RUN_DIR/create-volume.request.json"
jq '{volumeID,name,provider,status,sizeMB,mode,capabilities}' "$RUN_DIR/03-volume.json"
jq -e --arg name "$VOLUME" '.provider=="ssf" and .name==$name' "$RUN_DIR/03-volume.json"

# 当前 adapter 的动态 PVC 名称由 tenant/name 确定，便于只观察本次卷。
export PVC_NAME="$(python3 - <<'PY'
import hashlib, os
print('aenv-' + hashlib.sha256((os.environ['SSF_TENANT']+'/'+os.environ['VOLUME']).encode()).hexdigest()[:40])
PY
)"
k -n "$SSF_NAMESPACE" get pvc "$PVC_NAME" -o yaml > "$RUN_DIR/03-pvc.yaml"
k -n "$SSF_NAMESPACE" get pvc "$PVC_NAME"
```

`capabilities.snapshot/clone` 描述独立 Volume API 的能力；当前值为 false 不表示原沙箱联合快照/Fork 不可用，后续仍通过原沙箱生命周期接口验证。

**预期：** HTTP 201，provider 为 `ssf`，容量至少 1024 MB。Immediate 可以已 Bound，也可能仍在供给；WFFC 在首次创建沙箱前 Pending 是正常状态，不要在这里无限等待 Bound。若 PVC 找不到，先核对 `SSF_TENANT` 与实际控制配置，不根据猜测操作其他 PVC。

## 4. 创建沙箱 A，写入第一版笔记

```bash
jq -n --arg image "$AENV_IMAGE" --arg volume "$VOLUME" \
  '{image:$image,timeout:7200,volumeMounts:[{name:$volume,path:"/data"}]}' \
  > "$RUN_DIR/create-a.request.json"
api 04-a POST /sandboxes-cold 201 "$RUN_DIR/create-a.request.json"
export A_ID="$(jq -er .sandboxID "$RUN_DIR/04-a.json")"
export A_JSON="$RUN_DIR/04-a.json"
printf '%s\n' "$A_ID" >> "$RUN_DIR/sandbox-ids.txt"

guest "$A_JSON" 'grep " /data " /proc/mounts; printf "hello-ssf\nversion=1\n" > /data/notes.txt; sync; cat /data/notes.txt' \
  | tee "$RUN_DIR/04-write.txt"
guest "$A_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=1")"'

api 04-get GET "/sandboxes/$A_ID" 200
jq '{sandboxID,state,volumeMounts}' "$RUN_DIR/04-get.json"
api 04-list GET /v2/sandboxes 200
jq -e --arg id "$A_ID" 'any(.[]; .sandboxID==$id)' "$RUN_DIR/04-list.json"
k -n "$SSF_NAMESPACE" get pvc "$PVC_NAME" -o json > "$RUN_DIR/04-pvc.json"
jq -e '.status.phase=="Bound" and .spec.volumeMode=="Block"' "$RUN_DIR/04-pvc.json"
export PV_NAME="$(jq -er .spec.volumeName "$RUN_DIR/04-pvc.json")"
k get pv "$PV_NAME" -o yaml > "$RUN_DIR/04-pv.yaml"
```

**预期：** 原生创建返回 201 与 native sandboxID，GET 显示 running，卷映射 `/data`，guest 读出 `hello-ssf` 和 `version=1`。guest 挂载应为 ext4，PVC 为 Bound/Block。沙箱使用虚拟机，不应为每个沙箱产生用来持卷的业务 Pod。

此处同时验证了原 Gateway 路由与 envd 流式代理：只看到 HTTP 201 或 PVC Bound，而 guest 不能读写，还不能判定存储接入通过。

## 5. 验证排他写、暂停和恢复

### 5.1 A 活动时拒绝第二个 writer

```bash
api 05-conflict-active POST /sandboxes-cold 409 "$RUN_DIR/create-a.request.json"
guest "$A_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=1")"'
```

**预期：** 409，而不是另一个 201；原 A 数据不变。若意外返回 201，响应已保存在 `05-conflict-active.json`，保留该 sandboxID 并停止后续步骤，按失败排查，不能忽略第二个实例。

### 5.2 暂停仍保留逻辑使用权，恢复保持 ID

```bash
api 05-pause POST "/sandboxes/$A_ID/pause" 204
api 05-paused GET "/sandboxes/$A_ID" 200
jq -e '.state=="paused"' "$RUN_DIR/05-paused.json"
api 05-conflict-paused POST /sandboxes-cold 409 "$RUN_DIR/create-a.request.json"

printf '{"timeout":7200}\n' > "$RUN_DIR/resume.request.json"
api 05-resume POST "/sandboxes/$A_ID/resume" 201 "$RUN_DIR/resume.request.json"
test "$(jq -er .sandboxID "$RUN_DIR/05-resume.json")" = "$A_ID"
export A_JSON="$RUN_DIR/05-resume.json"
guest "$A_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=1")"'
```

**预期：** 暂停为 204、状态 paused；暂停期间新 writer 仍被 409 拒绝；恢复为 201，同一 sandboxID，v1 数据保留。这证明暂停释放本次运行资源时，逻辑 UseHold 仍在。

再验证原生 connect 自动恢复入口：

```bash
api 05-pause-again POST "/sandboxes/$A_ID/pause" 204
api 05-connect POST "/v2/sandboxes/$A_ID/connect" 201 "$RUN_DIR/resume.request.json"
test "$(jq -er .sandboxID "$RUN_DIR/05-connect.json")" = "$A_ID"
export A_JSON="$RUN_DIR/05-connect.json"
guest "$A_JSON" 'cat /data/notes.txt'
```

本步是连接触发的恢复；到期自动暂停/驱逐还可用文末兼容测试另行验证。

## 6. 为 v1 拍快照，然后把原件改成 v2

```bash
jq -n --arg name "${RUN_ID}-v1" '{name:$name}' > "$RUN_DIR/snapshot.request.json"
api 06-snapshot POST "/sandboxes/$A_ID/snapshots" 201 "$RUN_DIR/snapshot.request.json"
export SNAPSHOT_ID="$(jq -er .snapshotID "$RUN_DIR/06-snapshot.json")"
printf '%s\n' "$SNAPSHOT_ID" > "$RUN_DIR/snapshot-id.txt"

guest "$A_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=1")"; printf "hello-ssf\nversion=2\n" > /data/notes.txt; sync; cat /data/notes.txt' \
  | tee "$RUN_DIR/06-source-v2.txt"
k -n "$SSF_NAMESPACE" get volumesnapshots -l ssf.dev/managed=true -o json \
  > "$RUN_DIR/06-volume-snapshots.json"
```

**预期：** snapshotID 非空；拍快照后 A 仍能执行命令并改成 v2，说明拍快照没有把原沙箱永久暂停。第 9 步再验证 S 的数据保持 v1。观察对应 CSI VolumeSnapshot/Content 的 UID 与 readyToUse；不要把 namespace 内其他测试的快照数量作为本次成功依据。

## 7. Fork 两份可独立编辑的副本

```bash
printf '{"count":2,"timeout":7200}\n' > "$RUN_DIR/fork.request.json"
api 07-fork POST "/sandboxes/$A_ID/fork" 201 "$RUN_DIR/fork.request.json"

# 先记录所有成功 child，即使本次返回部分失败，也保留清理依据。
jq -r '.[] | .sandbox.sandboxID // empty' "$RUN_DIR/07-fork.json" >> "$RUN_DIR/sandbox-ids.txt"
jq -e 'length==2 and all(.[]; .sandbox.sandboxID!=null and (.error==null))' "$RUN_DIR/07-fork.json"
jq '.[0].sandbox' "$RUN_DIR/07-fork.json" > "$RUN_DIR/b.json"
jq '.[1].sandbox' "$RUN_DIR/07-fork.json" > "$RUN_DIR/c.json"
export B_ID="$(jq -er .sandboxID "$RUN_DIR/b.json")"
export C_ID="$(jq -er .sandboxID "$RUN_DIR/c.json")"
test "$B_ID" != "$C_ID" && test "$B_ID" != "$A_ID" && test "$C_ID" != "$A_ID"

guest "$RUN_DIR/b.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"; printf "hello-ssf\nversion=fork-b\n" > /data/notes.txt; sync'
guest "$RUN_DIR/c.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
guest "$A_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
guest "$RUN_DIR/b.json" 'cat /data/notes.txt'
```

**预期：** B/C 与 A 是三个不同 ID，B 写入 `fork-b` 后，C 和 A 仍为 v2。Fork 的 201 表示“已尝试”，必须检查数组中的每一项，不能只检查总 HTTP 状态。

记录 B/C 的真实卷名，供最后按精确身份清理；Fork 成功后的可见用户卷不会自动随沙箱删除：

```bash
api 07-b-detail GET "/sandboxes/$B_ID" 200
api 07-c-detail GET "/sandboxes/$C_ID" 200
jq '{sandboxID,volumeMounts}' "$RUN_DIR/07-b-detail.json" "$RUN_DIR/07-c-detail.json"
```

## 8. 删除 A，再用同一原卷启动 A2

```bash
api 08-delete-a DELETE "/sandboxes/$A_ID" 204
api 08-a-absent GET "/sandboxes/$A_ID" 404
api 08-volume-retained GET "/volumes/$VOLUME" 200
jq -e '.provider=="ssf" and .status!="deleted"' "$RUN_DIR/08-volume-retained.json"
k -n "$SSF_NAMESPACE" get pvc "$PVC_NAME"

api 08-a2 POST /sandboxes-cold 201 "$RUN_DIR/create-a.request.json"
export A2_ID="$(jq -er .sandboxID "$RUN_DIR/08-a2.json")"
export A2_JSON="$RUN_DIR/08-a2.json"
printf '%s\n' "$A2_ID" >> "$RUN_DIR/sandbox-ids.txt"
test "$A2_ID" != "$A_ID"
guest "$A2_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"; cat /data/notes.txt'
```

**预期：** A 消失，原用户卷/PVC 保留；A2 是新 native ID，但同一原卷上的 v2 数据保留。若原删除 204 后尚在存储释放收尾，新创建仍被拒绝，应检查停止/释放进度；先确认该创建明确失败且没有实例，再手动重试。网络超时不能据此推断没创建成功。

## 9. 删除源 A 后，仍从快照 S 恢复 v1

```bash
jq -n --arg template "$SNAPSHOT_ID" '{templateID:$template,timeout:7200}' \
  > "$RUN_DIR/restore.request.json"
api 09-template POST /v2/sandboxes 201 "$RUN_DIR/restore.request.json"
export T_ID="$(jq -er .sandboxID "$RUN_DIR/09-template.json")"
printf '%s\n' "$T_ID" >> "$RUN_DIR/sandbox-ids.txt"

guest "$RUN_DIR/09-template.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=1")"; printf "hello-ssf\nversion=template\n" > /data/notes.txt; sync'
guest "$A2_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
guest "$RUN_DIR/b.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=fork-b")"'
guest "$RUN_DIR/c.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
api 09-t-detail GET "/sandboxes/$T_ID" 200
jq '{sandboxID,volumeMounts}' "$RUN_DIR/09-t-detail.json"
```

**预期：** 创建请求未传 `volumeMounts`，T 自动继承快照中的卷并读到 v1；A 已删除不影响 S；修改 T 后 A2/B/C 数据均不变。

到这里，主线场景通过：原生创建/路由/读写、排他写、暂停持有、恢复、快照、Fork、模板继承、删沙箱保卷已验证。

## 10. 用同一快照验证空挂载和显式替换

### 10.1 显式空数组不继承可见卷

```bash
jq -n --arg template "$SNAPSHOT_ID" '{templateID:$template,timeout:7200,volumeMounts:[]}' \
  > "$RUN_DIR/empty.request.json"
api 10-empty POST /v2/sandboxes 201 "$RUN_DIR/empty.request.json"
export E_ID="$(jq -er .sandboxID "$RUN_DIR/10-empty.json")"
printf '%s\n' "$E_ID" >> "$RUN_DIR/sandbox-ids.txt"
guest "$RUN_DIR/10-empty.json" '! grep -q " /data " /proc/mounts; test ! -e /data/notes.txt'
api 10-e-detail GET "/sandboxes/$E_ID" 200
jq -e '(.volumeMounts // []) | length==0' "$RUN_DIR/10-e-detail.json"
```

**预期：** `/data` 不再是继承的可见卷，旧笔记不存在。内部为恢复内存保留的隐藏文件系统不属于公共挂载列表，不要求所有内部 PVC 立即消失。

### 10.2 显式替换为一块新卷

```bash
export REPLACEMENT="${RUN_ID}-replacement"
jq --arg name "$REPLACEMENT" '.name=$name' "$RUN_DIR/create-volume.request.json" \
  > "$RUN_DIR/replacement-volume.request.json"
api 10-new-volume POST /volumes 201 "$RUN_DIR/replacement-volume.request.json"
jq -n --arg template "$SNAPSHOT_ID" --arg volume "$REPLACEMENT" \
  '{templateID:$template,timeout:7200,volumeMounts:[{name:$volume,path:"/data"}]}' \
  > "$RUN_DIR/replacement.request.json"
api 10-replacement POST /v2/sandboxes 201 "$RUN_DIR/replacement.request.json"
export R_ID="$(jq -er .sandboxID "$RUN_DIR/10-replacement.json")"
printf '%s\n' "$R_ID" >> "$RUN_DIR/sandbox-ids.txt"
guest "$RUN_DIR/10-replacement.json" 'test ! -e /data/notes.txt; printf "replacement\n" > /data/notes.txt; sync; cat /data/notes.txt'
guest "$A2_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
```

**预期：** R 的 `/data` 是新卷，初始没有旧笔记；可写入 replacement，原卷仍为 v2。若模板预留盘位不足，应根据实际模板槽位配置检查，不能通过替换仍被内存引用的旧盘绕过校验。文末自动 E2E 另有“捕获前打开 FD、恢复后继续读旧内容”的更深验证。

## 11. 可选：给原笔记卷做离线扩容

仅当本次 SC/Driver 支持扩容时执行。此时原笔记卷只由 A2 使用；B/C/T 使用独立克隆，R 使用 replacement 卷。

```bash
api 11-volume-before GET "/volumes/$VOLUME" 200
export EXPAND_MB="$(jq -er '.sizeMB * 2' "$RUN_DIR/11-volume-before.json")"
guest "$A2_JSON" "stat -f -c '%b %S' /data" > "$RUN_DIR/11-capacity-before.txt"
jq -n --argjson size "$EXPAND_MB" '{sizeMB:$size}' > "$RUN_DIR/expand.request.json"
api 11-expand POST "/volumes/$VOLUME/expand" 202 "$RUN_DIR/expand.request.json"
guest "$A2_JSON" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'

# 暂停仍有 hold，不能作为完成离线扩容的条件；这里删除 A2 释放原卷。
api 11-delete-a2 DELETE "/sandboxes/$A2_ID" 204
expanded=false
for attempt in $(seq 1 120); do
  api 11-volume-poll GET "/volumes/$VOLUME" 200
  if jq -e --argjson size "$EXPAND_MB" '.status=="ready" and .sizeMB >= $size' \
       "$RUN_DIR/11-volume-poll.json" >/dev/null; then
    expanded=true
    break
  fi
  sleep 5
done
test "$expanded" = true

api 11-a3 POST /sandboxes-cold 201 "$RUN_DIR/create-a.request.json"
export A3_ID="$(jq -er .sandboxID "$RUN_DIR/11-a3.json")"
printf '%s\n' "$A3_ID" >> "$RUN_DIR/sandbox-ids.txt"
guest "$RUN_DIR/11-a3.json" 'test "$(cat /data/notes.txt)" = "$(printf "hello-ssf\nversion=2")"'
guest "$RUN_DIR/11-a3.json" "stat -f -c '%b %S' /data" > "$RUN_DIR/11-capacity-after.txt"
python3 - <<'PY'
from pathlib import Path
import os
p=Path(os.environ['RUN_DIR'])
def capacity(name):
    blocks,size=map(int,(p/name).read_text().split())
    return blocks*size
before=capacity('11-capacity-before.txt'); after=capacity('11-capacity-after.txt')
assert after>before, (before,after)
print('guest 文件系统容量增加：',before,'->',after)
PY
printf '{"sizeMB":1}\n' > "$RUN_DIR/shrink.request.json"
api 11-shrink POST "/volumes/$VOLUME/expand" 409 "$RUN_DIR/shrink.request.json"
```

**预期：** 活动时扩容为 202，旧数据继续可读；删除原卷使用者后控制面完成扩容；A3 启动前完成 NodeExpand/ext4 扩容，guest 实际容量增加，v2 不变；缩容被拒绝。单看 Volume 的 sizeMB 变大不足以证明 guest 文件系统已扩容。

## 12. 采集结果与清理本次资源

### 12.1 先保存证据

```bash
python3 scripts/native_evidence.py --context "$KUBE_CONTEXT" \
  --namespace "$SSF_NAMESPACE" --output "$RUN_DIR/cluster-evidence"

# 在删除前，保存所有仍存在的实例的真实卷映射；404 可来自前面已删除的 A/A2。
sort -u "$RUN_DIR/sandbox-ids.txt" > "$RUN_DIR/sandbox-ids.unique.txt"
while IFS= read -r id; do
  api "12-detail-$id" GET "/sandboxes/$id" 200,404
done < "$RUN_DIR/sandbox-ids.unique.txt"

printf '%s\n' "$VOLUME" > "$RUN_DIR/volume-candidates.txt"
if [[ -n "${REPLACEMENT:-}" ]]; then printf '%s\n' "$REPLACEMENT" >> "$RUN_DIR/volume-candidates.txt"; fi
for file in "$RUN_DIR"/12-detail-*.json; do
  jq -r '.volumeMounts[]?.name // empty' "$file" >> "$RUN_DIR/volume-candidates.txt"
done
sort -u "$RUN_DIR/volume-candidates.txt" > "$RUN_DIR/volume-candidates.unique.txt"
cat "$RUN_DIR/volume-candidates.unique.txt"
```

故障排查时保留 `*.http`、请求 JSON、响应、guest 输出，以及对应时段日志。`native_evidence.py` 的资源采集不是功能通过证据，功能结论仍以每步断言和数据对比为准。若共享环境无全局资源读取权限，该采集工具可能报失败；记录缺失证据，按权限补采。

### 12.2 删除本次沙箱，再删除模板

```bash
while IFS= read -r id; do
  api "12-delete-$id" DELETE "/sandboxes/$id" 204,404
done < "$RUN_DIR/sandbox-ids.unique.txt"

api 12-delete-template DELETE "/templates/$SNAPSHOT_ID" 204
```

如果删除模板仍被引用保护拒绝，检查 child/恢复实例是否确已删除、清理 outbox 是否完成，再重试同一个模板删除。不要直接删除 VolumeSnapshotContent 或移除保护来强行通过。

### 12.3 核对卷清单后删除测试卷

普通删除沙箱会保留用户卷，包括 Fork/模板成功暴露的可写克隆。先核对 `volume-candidates.unique.txt` 是否全部来自本次实例；默认停在这里即可保留数据供检查。确认后，复制清单作为明确的删除名单，再执行：

```bash
cp "$RUN_DIR/volume-candidates.unique.txt" "$RUN_DIR/volumes-approved-for-delete.txt"
# 若需保留某卷，先编辑上面的 approved 文件删除对应行。
while IFS= read -r volume; do
  [[ -n "$volume" ]] || continue
  api "12-delete-volume-$volume" DELETE "/volumes/$volume" 204,409
done < "$RUN_DIR/volumes-approved-for-delete.txt"
```

204 表示 Volume API 删除完成；409 表示 drain/引用清理仍在进行或被拒绝，**不能算删除通过**。检查响应及停止/回收状态后再重试对应卷。Retain 策略下 PV/后端数据仍可保留；本手册不直接删除 PV 或后端数据。不要在前面的读写验证阶段调用 Volume DELETE，因为它会持久设置 drain，影响后续复用。

若测试中途失败，先查看所有成功创建响应，补录未进入 ID 清单的成功实例（例如期望 409 却收到 201），再按上述依赖顺序清理。网络超时的创建结果先对账，不当作失败后重新创建。

## 13. 结果登记与下一轮复测

将下表复制到 `$RUN_DIR/result.md`，由实际执行者填写；跳过的可选步骤填“未执行”。

| 项目 | 结果：通过/失败/未执行 | 证据或异常 |
| --- | --- | --- |
| SSF 卷 + 原生 cold + guest v1 | 待填写 | 03/04 响应、04-write.txt、PVC/PV |
| 活动/暂停排他写 | 待填写 | 两次 409、paused 状态 |
| resume/connect 同 ID 与数据保留 | 待填写 | 05 响应与 guest 断言 |
| 快照源保持运行、保存 v1 | 待填写 | 06、09 |
| Fork 两个副本互不影响 | 待填写 | 07 数组与三份数据 |
| 删除源后原卷保留、A2 读 v2 | 待填写 | 08 |
| 源删除后模板继承 v1，修改隔离 | 待填写 | 09 |
| 模板空数组/替换 | 待填写 | 10 |
| 离线扩容/缩容拒绝 | 待填写 | 11 的 guest 容量前后对比 |
| 清理与 Retain 状态 | 待填写 | 12，注明未清理资源 |

### 自动复跑同类场景

手工主线通过后，可使用现有 E2E 自动复跑。它会创建一套**新的**卷/沙箱，不复用本次手工数据；内含扩容，因此要求后端支持扩容：

```bash
PY_TLS_ARGS=()
if [[ -n "$AENV_CA_FILE" ]]; then PY_TLS_ARGS=(--ca "$AENV_CA_FILE"); fi
python3 scripts/native_lifecycle_e2e.py --execute --url "$AENV_URL" \
  "${PY_TLS_ARGS[@]}" --image "$AENV_IMAGE" --namespace "$SSF_NAMESPACE" \
  --storage-class "$SSF_STORAGE_CLASS" --snapshot-class "$SSF_SNAPSHOT_CLASS" \
  --driver-kind vendor --envd-port "$AENV_ENVD_PORT" --output "$RUN_DIR/automatic-e2e"

jq '{status,cases,limitations}' "$RUN_DIR/automatic-e2e/report.json"
python3 scripts/native_cleanup.py --url "$AENV_URL" "${PY_TLS_ARGS[@]}" \
  --report "$RUN_DIR/automatic-e2e/report.json"
```

使用测试 fixture 时改为 `--driver-kind fixture`；验证 legacy/SSF 混合卷可另一次加 `--mixed`。自动清理默认只打印计划，加 `--execute` 才执行；快照/显式创建的卷分别加 `--templates`/`--volumes`。当前自动报告不枚举所有成功 Fork/模板派生的用户卷，若要完全清理，应在删除 child 前像步骤 12 一样保存并核对其 `volumeMounts`。

换另一种绑定模式时，换成对应 Immediate/WFFC StorageClass，使用新的 RUN_ID 重走主线。静态导入、RO 共享、模板构建/P2P、故障注入属于后续专项，见 [完整测试方案](14-agentenvadapter-real-environment-test-plan.md)。本场景通过不自动代表这些专项全部通过。

## 14. 某一步失败，从哪里看

| 现象 | 优先检查 |
| --- | --- |
| 401/403 | API Key、Gateway 鉴权、namespace/tenant 授权；不绕过 Gateway 改走私有插件端点 |
| WFFC 卷在创建前 Pending | 正常；创建失败后再检查 selected-node、CSI provisioner 与拓扑约束 |
| cold 创建失败 | 原 Scheduler 准入/节点健康、PVC Bound、Controller Attach、node/SSF Node 日志；记录是否已派发 |
| 创建 201 但 guest 执行失败 | envd 端口、镜像 envd、原 Gateway 代理、响应 token；区分代理故障与磁盘故障 |
| 第二 writer 返回非 409 或意外 201 | 保存完整响应/native ID，停止后续写入；检查 owner/run 与实际 VMM，不丢弃异常实例 |
| pause/resume 失败 | 原 Orchestrator freeze/stop、StorageProvider outbox、UseHold、恢复 epoch；不直接清理 attachment |
| 快照/Fork/模板失败 | SnapshotClass/Driver 能力、快照 readyToUse、内容 UID/pin、冻结/thaw、原模板仓库和节点资源；Fork 看每项 error |
| 显式替换失败 | 模板固定设备槽位、RO 槽位、隐藏保留卷、路径冲突；保留原冻结设备身份 |
| 扩容一直 pending | 活动绑定/暂停 hold/attachment 是否释放、SC 允许扩容、Controller/NodeExpand、e2fsck/resize2fs 日志 |
| 删除后仍有资源 | 区分用户卷保留、异步清理、引用保护和未知 stop；API 404 不等于真实 VMM 已停止 |

集群常用只读命令（把 NODE_POD 替换为实际原生节点 Pod，namespace 使用工作负载所在位置）：

```bash
k -n "$SSF_NAMESPACE" describe pvc "$PVC_NAME"
k -n "$SSF_NAMESPACE" get events --sort-by=.metadata.creationTimestamp
k -n "$SSF_NAMESPACE" logs deploy/agentenv-scheduler --all-containers --since=30m
k -n "$SSF_NAMESPACE" logs NODE_POD -c agentenv --since=30m
k -n "$SSF_NAMESPACE" logs NODE_POD -c agentenv-storage --since=30m
k -n "$SSF_NAMESPACE" logs NODE_POD -c ssf-node --since=30m
```

需要账本断言时，在具备只读数据库凭据的运维终端使用 `SSF/scripts/native_invariants.py`。它只证明账本约束，不代替本场景的 guest 数据检查或实际 VMM 停止证据。
