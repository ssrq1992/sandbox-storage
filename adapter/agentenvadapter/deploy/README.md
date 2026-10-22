# 部署入口

SSF 专用部署归框架仓库维护，原 AgentENV Kustomize base 继续复用。

- `k8s/`：原 Gateway/Scheduler/Node + 独立 ssf-coordinator；用 `scripts/render_agentenv_native.py` 离线渲染。
- `Dockerfile.agentenv`、`Dockerfile.scheduler`、`Dockerfile.gateway`：从两个仓库共同父目录构建；使用普通 Go modfile 和固定 Cargo.lock。
- `standalone/`：新增本机协调服务，原 AgentENV ExecStart 不变。

完整步骤见 [部署文档](../../../../design/13-agentenvadapter-installation-and-deployment.md)。禁止混用旧内嵌配置；没有旧运行状态在线迁移或自动清理。
