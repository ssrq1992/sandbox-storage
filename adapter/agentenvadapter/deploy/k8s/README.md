# 原生增量 overlay

权威 overlay 由 sandbox-storage 维护，引用兄弟 AgentENV 原 base。推荐通过 `scripts/render_agentenv_native.py` 补齐 base 配置并离线渲染。

新增 `ssf-coordinator` Deployment/Service；Scheduler 仅加载 scheduler.json/TLS，无 DB Secret。节点 sidecar 直连 coordinator，原 native 容器、P2P/headless/探针和 drain 保留。Guard/CSI Gateway 在公共部署中单独安装。详见 [13](../../../../../design/13-agentenvadapter-installation-and-deployment.md)。
