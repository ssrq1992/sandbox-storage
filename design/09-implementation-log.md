# SSF 实施记录

## 2026-10-19：实施开始

- 范围：SSF 公共框架、Go/Rust 客户端、验证适配器、部署与可复现测试环境；不修改 AgentENV、CubeSandbox、Substrate。
- 公共消费协调嵌入调用方；Kubernetes 是卷生命周期唯一权威。
- 共享账本选 PostgreSQL，节点持久日志选 bbolt。Runtime 实际 guest 接入由后续项目适配。
- 本机：macOS amd64，Go 1.26.3；未发现 Docker、kubectl、kind、protoc。
- 后续条目记录实际完成与验证，未运行的 Linux/集群测试不计作通过。

## 工程与协议

- 建立独立 Go 模块、标准 CSI 客户端、`ssf.v1.RuntimeStorageNode` protobuf、官方生成的 Go 代码及 Go/Rust SDK。
- 本机缺少 protoc，使用固定版本 protocompile 和官方 protoc-gen-go / protoc-gen-go-grpc 生成；生成结果纳入代码目录。
- 默认网络入口使用 mTLS 与 URI SAN allowlist；NodeVolumePlan 用 Ed25519 签名并绑定租户、实例、节点、boot、PVC/PV、卷及 Binding。
- Go 依赖代理多次超时；使用已有模块缓存与补充下载的固定版本 bbolt/protocompile 完成构建。临时代理和构建缓存只放在 `/private/tmp`，没有写入仓库 replace 路径。

## 持久状态与直接执行

- PostgreSQL 保存请求、幂等摘要、Binding、共享 attachment、操作步骤、WFFC 选点/候选许可与排空许可。Kubernetes 仍是卷管理权威。
- 同一控制流程复用 PostgreSQL 会话；物理卷锁与 operation 锁分开。修正嵌套锁占用多个连接导致连接池耗尽的风险。
- Node 使用 bbolt 同步持久化 intent/outcome，增加 active/volume-holder 索引，历史 tombstone 不参与正常挂载扫描。
- 按 operation 和物理卷加锁；不同物理卷可并发处理。兼容的活动 attachment/stage 复用，末次引用才释放。
- 增加节点未收到 Prepare 时的取消 tombstone，拒绝迟到的重放。未知 RPC 结果保留 intent 和引用，不按超时释放 writer。
- 释放保存真实 CSI node_id；即使 Driver 能力后来变化，也不能跳过曾经执行过的 Unstage/Detach。

## Kubernetes 与保护

- 原生 PVC/PV facade、UID 固定、Immediate、动态/静态 WFFC、静态候选持久预留、原生 snapshot/clone/expand/reclaim API。
- Guard 检查删除许可、受管标签、后端身份、原生 Pod 冲突、WFFC selected-node 与静态候选许可。原生初始 Immediate 绑定继续允许。
- Coordinator 准入前检查 fail-closed webhook 配置，并拒绝尚未排空的原生 Pod/VolumeAttachment。
- Gateway 除 RPC 白名单和 mTLS 外，还读取账本核对 Binding、node_id、当前 attachment 状态和请求能力；没有复制 Substrate 的直接供卷路径。

## 部署、测试与修正

- 交付公共进程、CLI、Dockerfile、RBAC、部署清单、数据库分权 SQL、配置示例、嵌入式创建/清理示例。
- 提供 PostgreSQL 集成测试、Linux bind/loop CSI fixture、kind 原生 provisioner/resizer 环境、snapshot 安装脚本及测试结果目录。
- 首次 Linux 交叉构建发现当前 x/sys 不提供所用的 uint64 ioctl 辅助函数，改为对应 Linux ioctl 调用。
- 本机基准发现 Prepared 重试仍重复刷盘，修正为签名与实际资源验证后直接复用；重试基准从约 22.7 ms 降至约 0.14 ms。只代表当前机器、mock CSI 的重试路径，不代表冷挂载或 VM 性能。
- 本机初轮 Go 测试、race、vet、Linux 交叉构建和协议重生成检查已执行；最终结果以 [验证报告](10-implementation-validation.md) 为准。

## 设计同步

- 将本次交付范围明确为公共 Runtime 契约、验证适配器和项目接入示例；两个项目适配器留待后续。
- 实际接口以 SSF protobuf 为准，旧设计中的接口代码块保留为草案；部署图更新 Node API 为 mTLS gRPC。
- 完整部署图重新生成 SVG/Mermaid/PNG，179 个部署元素检查通过。
- 用户指定 CSI 分析原文 SHA256 保持 `9733935a2c0e2d144b67c8404361c835e7f8ed45d2b508992cb230da31b3d5dc`。
