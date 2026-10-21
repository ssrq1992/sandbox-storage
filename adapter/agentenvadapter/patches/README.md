# AgentENV 原生插件补丁与重放

当前基线为 `6cccaa7842bd5be2051111d4f74d9e37aa721244`。工作区源码已直接修改；不要在当前工作区重复应用补丁。没有提交或推送。

当前工件为 [agentenv-native-progress.patch](agentenv-native-progress.patch)，内容哈希、基线和精确重放状态见 [相邻 JSON](agentenv-native-progress.patch.json)。补丁 JSON 的 `complete_plan=false` 表示补丁重放工具不判定整体计划验收；软件覆盖和真机未执行状态以设计 18 和 [自动准备增量记录](../../../../design/22-volume-preparation-implementation.md) 为准。

从当前源码重新生成并在临时基线副本验证内容及文件模式：

```sh
cd sandboxVolume/SSF
python3 scripts/agentenv_patch.py --output adapter/agentenvadapter/patches/agentenv-native-progress.patch
```

脚本不改 Git index，也不提交。补丁包含当前 AgentENV 的全部已跟踪修改及未跟踪文件，因此包括原用户改动和本次实现。用于独立、干净 checkout 时先执行 `git apply --check`，不得在不清楚既有修改的目录中覆盖。

必要侵入点按职责划分如下，具体路径以补丁 JSON 为准：

| 位置 | 原因 |
| --- | --- |
| cfg、sandbox/storage | 独立可关闭 StorageProvider、通用设备、可信事件和持久 outbox；Rust 不引入 K8s/CSI 客户端 |
| Gateway、shared/storage、调度 proto | 原 body/native API 不变；剥离客户私有 header，增加角色签名与关联数据 |
| Scheduler、独立 Go modfile/Dockerfile | 在原调度内注入 ControlPlugin，保持普通构建，复用发现与计算策略 |
| 原生内部 API、VolumeProviderMux、OpenAPI 可选字段及生成模型 | 模板/默认资源投影、provider 卷管理；SSF 错误不回退空卷 |
| Orchestrator、launch/factory、持久化 | 保持 VMM 所有权，传递每次运行上下文、删除任务、恢复身份及 artifact 保护 |
| Firecracker config/sandbox、manifest | 外部设备槽位、guest 验证、真实停止；冻结义务和不含 ticket 的 checkpoint 元数据 |
| SnapshotManager | 独立 recorder/clone 准入，禁止复用父 storage ticket |
| Kustomize overlay | 引用原 base 增加 sidecar、凭据、WAL、挂载传播与 drain，不替换原组件 |

旧独立 API/control、静态 workers/templates、custom-extension 存储原型和专用客户端已删除。真实 Linux/KVM/CSI 验证尚未执行，不能以补丁重放代替。MIT 许可保留在 [AgentENV-LICENSE](AgentENV-LICENSE)。

本次直接 PVC 增量仅在原公开 mount 模型增加可选 `pvc`（与 `name` 互斥），Rust StorageProvider 持久传递通用引用，原生能力 hook 升至 4；SDK 提供 typed PVC mount。原 Gateway/Scheduler 路径和 VMM 所有权不变。overlay 新增的是配置与既有节点 sidecar 的准备端点，不是独立沙箱控制面。
