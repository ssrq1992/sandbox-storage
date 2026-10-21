# 原生部署入口

集群使用 [原 Kustomize base 的 SSF overlay](../../../../AgentENV/deploy/k8s/overlays/ssf/README.md)，保留 Gateway、Scheduler 与节点 DaemonSet。standalone 使用 [本目录材料](standalone/README.md)。详细配置、构建、凭据和启停见 [部署方案](../../../../design/13-agentenvadapter-installation-and-deployment.md)。旧独立控制面和 workers/templates 部署已移除。
