# AgentENV SSF 原生插件

新增 [自动卷准备与直接 PVC 实现/部署/验证](../../../design/22-volume-preparation-implementation.md)：Kubernetes 创建 PVC 后，原 AgentENV 创建接口通过 `volumeMounts[].pvc` 引用。具体通过、未执行和未完成边界以该记录为准。

保留原 Gateway、Scheduler、节点、Orchestrator 和沙箱 API。SSF adapter 只负责存储接入，不提供替代沙箱控制面。完整实现说明、部署和验证入口：

- [软件实现](../../../design/12-agentenvadapter-design-and-implementation.md)
- [集群与 standalone 部署](../../../design/13-agentenvadapter-installation-and-deployment.md)
- [测试方案](../../../design/14-agentenvadapter-real-environment-test-plan.md) / [快速操作](../../../design/15-agentenvadapter-quick-test.md)
- [实际验证状态](../../../design/18-native-integration-implementation-status.md)
- [补丁和必要侵入点](patches/README.md)

入口为 `agentenvadapter node -config FILE`；ControlPlugin 编译入原 Scheduler，standalone 使用同一消费库和固定本机 placement。没有静态 workers/templates、专用沙箱客户端或旧 API 兼容层。默认 legacy provider；SSF 失败不回退空卷。

实现 Block/ext4、暂停 UseHold/恢复 epoch、联合快照、Fork 独立 RW/共享 RO、模板省略/清空/替换、离线扩容、持久停止/回收。普通删除沙箱保留用户卷。完整能力 `agentenv-capture-clone-v1` 表示软件 hook 集合；真实 KVM/K8s/厂商 CSI 尚未验收。

从 `SSF` 运行 `python3 scripts/validate_native_foundation.py`、`python3 scripts/native_database_test.py --postgres-bin PATH`。真机工具均通过原生 API，显式 `--execute` 才创建资源。未提交、推送或部署。
