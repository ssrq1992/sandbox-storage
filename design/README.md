# SSF 沙箱存储框架设计与交付

更新：2026-10-22。Kubernetes 管理 StorageClass/PVC/PV、供卷、快照、克隆、扩容和回收；SSF 管理内容准备、消费授权、挂载、引用和恢复。AgentENV 保留原 Gateway、Scheduler、Orchestrator 和原生功能。

**用户主线：Kubernetes 创建 PVC → 原 AgentENV 创建接口通过 `volumeMounts[].pvc` 挂到 `/data`。** adapter 分为供卷定制准备、沙箱内部挂载两部分；不要求初始化 Pod、手工格式化或预先 import。原 `/volumes` 保留为便捷 facade，命名卷、PVC 直接引用和 legacy 混合继续支持。

## 当前阅读入口

| 文档 | 内容 |
| --- | --- |
| [03 框架设计](03-framework-design.md) | Kubernetes/CSI/SSF/运行时职责、逻辑接口与一致性约束 |
| [23 当前实现架构](23-current-implementation-architecture.md) | 独立协调服务、原生薄客户端、节点插件的实际调用和部署图 |
| [24 公共模块与扩展点](24-ssf-framework-architecture-and-extension-points.md) | 公共能力归属与必要原生边界 |
| [26 本次实现与侵入量对照](26-independent-coordinator-implementation.md) | 版本化协议、Rust 抽离、安全时序、测试结果及改动统计 |
| [27 原生 OverlayBD CSI 复用设计](27-agentenv-overlaybd-csi-reuse-design.md) | **新设计，尚未实施**：Controller/Node/Runtime adapter 分工，复用原生卷、ublk 与 VM 接盘，设备唯一所有权、完整生命周期及验收 |
| [13 安装部署](13-agentenvadapter-installation-and-deployment.md) | 双仓联合构建、凭据、框架 overlay、standalone |
| [14 完整回归与故障验收](14-agentenvadapter-real-environment-test-plan.md) | 原生功能、完整生命周期、服务故障和真实环境测试入口 |
| [20 PVC 手动端到端场景](20-kubernetes-pvc-agentenv-mount-validation.md) | A 写笔记 → 删除 A → B 挂同一 PVC 读回 |
| [19 逐步生命周期验证](19-agentenv-native-scenario-validation.md) | 暂停恢复、快照、Fork、模板与扩容 |
| [15 快速操作](15-agentenvadapter-quick-test.md) | 原生 API 和既有工具入口 |

当前实现及验证证据以 [26](26-independent-coordinator-implementation.md) 和 [本轮报告](../sandbox-storage/validation/control-service) 为准。真实 Linux/KVM/Kubernetes/厂商 CSI 的未执行项不由本机单测或交叉类型检查替代。

新增设计以 [27](27-agentenv-overlaybd-csi-reuse-design.md) 为入口：Kubernetes 创建原生 OverlayBD 卷，CSI Node 发布真实宿主块设备，Runtime adapter 接纳已有设备并复用原生 guest 接入。图源：[模块](figures/ssf-overlaybd-modules.mmd) · [部署](figures/ssf-overlaybd-deployment.mmd) · [调用](figures/ssf-overlaybd-calls.mmd)。这些目标图不替代下方当前实现图。

## 当前职责与部署

| 能力 | 负责方 |
| --- | --- |
| 卷资源、后端供卷及回收 | Kubernetes 原生对象、官方 CSI 配套组件、厂商 Driver |
| Consumption、Preparation、Resolver/WFFC、Ledger 事务 | `sandbox-storage` 公共模块，在独立 `ssf-coordinator` 运行 |
| AgentENV 请求、模板及生命周期映射 | `sandbox-storage/adapter/agentenvadapter`，通过公共运行时注册接口接入 |
| 原发现、心跳、健康过滤及计算选择策略 | 原 Scheduler；通过轻量 mTLS Go 客户端提交所选节点 |
| VMM、冻结屏障、真实停止、设备边界及工件操作 | 原 Node/Orchestrator；Rust 协议和策略 crate 由框架仓库维护并联合构建 |
| Stage/Publish、设备准备、WAL | 原 Node Pod 的 NodePlugin/SSF Node 常驻 sidecar，调用厂商 CSI Node UDS |
| 引用和删除保护 | PostgreSQL 唯一共享账本、独立 Guard、节点 WAL |

协调服务不自行改选计算节点，不接管公开沙箱 API；原 Scheduler 不连接 SSF 数据库或 CSI。Controller Service 不调用 Node Service，业务 I/O 不经过协调服务。独立服务与旧内嵌路径不能同时拥有执行权。

可编辑架构图：[模块](figures/ssf-independent-modules.mmd) · [部署](figures/ssf-independent-deployment.mmd) · [调用](figures/ssf-independent-calls.mmd)。渲染图见 [23](23-current-implementation-architecture.md)。旧 `ssf-*`、`agentenv-native-*` 图保留为演进材料，不作为当前部署清单。

## 设计依据和历史记录

| 文档 | 使用方式 |
| --- | --- |
| [01 项目分析](01-repository-analysis.md)、[06 Substrate 分析](06-substrate-storage-analysis.md) | 原始源码参考；CubeSandbox/substrate 不是本轮实现对象 |
| [02 决策](02-csi-decision.md)、[04 集成验证](04-integration-and-validation.md)、[05 Kubernetes 集成](05-kubernetes-integration.md)、[07 复用分析](07-substrate-inspired-implementation.md) | 公共框架的设计依据；旧内嵌部署描述由 03/23/26 替代 |
| [08 部署演进](08-full-deployment-architecture.md)、[09 实施日志](09-implementation-log.md)、[10 早期验证](10-implementation-validation.md)、[11 早期覆盖矩阵](11-implementation-coverage.md) | 保留早期事实，不用于证明本轮回归通过 |
| [12 原生插件实现](12-agentenvadapter-design-and-implementation.md)、[16 原生集成设计](16-agentenvadapter-native-integration-design.md)、[17 原生改造验收](17-agentenvadapter-refactoring-and-validation.md)、[18 原生实施记录](18-native-integration-implementation-status.md) | 原生插件演进记录，原 API/生命周期原则继续有效 |
| [21 两部分 adapter 设计](21-ssf-volume-provisioning-and-runtime-adapter-design.md)、[22 自动准备实施](22-volume-preparation-implementation.md) | 保留 PVC 自动准备与直接挂载数据语义；部署归属以本轮为准 |
| [25 需求与偏差汇总](25-requirements-and-open-questions-summary.md) | 原生 OverlayBD 接入诉求与此前实现的差异；其中待决策项由 26 的独立服务实现及 27 的新设计分别承接 |
| [用户指定 K8S CSI 参考](assets/K8S-CSI架构分析.md) | 架构表达参考，原文未改 |

当前已交付接入仍为通用 CSI Block/ext4，legacy OverlayBD 保留，尚无新增 OverlayBD CSI Driver。最新设计 27 将新增复用原生实现的 OverlayBD CSI Driver，继续采用 Block/ext4 接口并保留既有能力；不新增 FileTree 或跨节点内存迁移。2026-10-22 本轮仅新增设计及图源、更新本索引，未修改软件、提交、推送或部署，也未清理环境资源。

- [28 — OverlayBD CSI 实施状态与验证记录](28-overlaybd-csi-implementation-status.md)：共享库、协议、操作日志的实际进度；完整 Driver 与生命周期尚未完成。
