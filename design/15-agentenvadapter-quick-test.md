# AgentENV 原生插件快速操作

> 当前部署使用独立 `ssf-coordinator`，安装入口见 [13](13-agentenvadapter-installation-and-deployment.md)，原用户 API 和以下数据验证主线保持不变。服务/故障追加验收见 [14](14-agentenvadapter-real-environment-test-plan.md)。旧 Scheduler 内嵌控制部署不适用于本轮。

> 2026-10-21 自动卷准备增量：实现、部署、验证结果及未完成边界统一见 [22](22-volume-preparation-implementation.md)；直接 PVC 两步场景见 [20](20-kubernetes-pvc-agentenv-mount-validation.md)。本文件原生命周期说明继续适用，旧未启用 profile 的卷策略不代表新 profile 规则。
最短操作使用 [20：PVC 两步验证](20-kubernetes-pvc-agentenv-mount-validation.md)。原命名卷 facade 的生命周期验证仍见 [19](19-agentenv-native-scenario-validation.md)；新 profile 与旧未启用 profile 的卷策略不同，详见 [22](22-volume-preparation-implementation.md)。

完整部署见 [13](13-agentenvadapter-installation-and-deployment.md)，测试判据见 [14](14-agentenvadapter-real-environment-test-plan.md)，当前证据见 [18](18-native-integration-implementation-status.md)。命令从 `SSF` 目录执行。客户端始终访问原 Gateway/standalone API。

```sh
python3 scripts/native_preflight.py --context CONTEXT --namespace agentenv-system --storage-class SC --snapshot-class SNAP --output /tmp/preflight.json
python3 scripts/native_compatibility_e2e.py --execute --url https://GATEWAY --image IMAGE --provider-mode enabled-no-ssf --output /tmp/native-compat
python3 scripts/native_lifecycle_e2e.py --execute --url https://GATEWAY --image IMAGE --namespace agentenv-system --storage-class SC --snapshot-class SNAP --driver-kind vendor --mixed --output /tmp/native-e2e
python3 scripts/native_evidence.py --context CONTEXT --namespace agentenv-system --output /tmp/native-evidence
python3 scripts/native_cleanup.py --url https://GATEWAY --report /tmp/native-e2e/report.json
```

API Key 从 `AENV_API_KEY` 环境变量读取，自签证书用 `--ca CA_FILE`；不关闭 TLS 校验。E2E 创建真实卷/VM，需要专用环境和足够 CPU/内存/磁盘，结果未知的请求不会自动重试。输出目录必须不存在。若使用 fixture，`--driver-kind fixture`，不得把结果记为 vendor 验证。

原生 Volume 请求示例：

```json
{"name":"ssf-data","provider":"ssf","sizeMB":1024,"mode":"exclusive","storage":{"namespace":"agentenv-system","storageClass":"SC","snapshotClass":"SNAP"}}
```

发送到原 `POST /volumes`，随后用原 `POST /sandboxes-cold`：

```json
{"image":"IMAGE","timeout":3600,"volumeMounts":[{"name":"ssf-data","path":"/data"}]}
```

后续仍用原 `/sandboxes/{id}/pause`、`resume`、`snapshots`、`fork`、DELETE，及 `/v2/sandboxes` 模板接口。扩容用 `POST /volumes/ssf-data/expand` body `{"sizeMB":2048}`；活动/暂停期间为 202，删除运行且释放 hold 后控制面完成扩容，下次启动执行 NodeExpand/ext4 扩容。普通删除沙箱保留用户卷。

如需实际清理，先审核 `native_cleanup.py` 打印的计划，再加 `--execute`；快照、卷分别加 `--templates`、`--volumes`。不要删除未知 create 的资源或清空 WAL。
