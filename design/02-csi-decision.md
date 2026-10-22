# Kubernetes 卷管理与 Substrate 直接消费的选型

本方案采用一个管理体系、两类消费路径：Kubernetes 原生资源和 CSI 配套组件管理所有正式接入卷；原生 Pod 继续由 Kubernetes 消费，独立沙箱参考 Substrate 通过直接调用消费。沙箱侧不实现 Direct CSI 供卷，也不要求新增独立 SSF Controller Pod。

架构表达参考[用户指定分析](assets/K8S-CSI架构分析.md)，该参考文档保持不变。

2026-10-21 设计补充：SSF 管理链增加公共卷准备协调，运行时 adapter 分为“供卷定制准备”和“沙箱内部挂载”；Kubernetes 创建的 PVC 可直接被原沙箱创建请求引用，原项目 Volume API 为可选 facade。详见 [21](21-ssf-volume-provisioning-and-runtime-adapter-design.md)。这不新增 CSI provisioner，原生组件仍唯一调用后端 Create/Delete；准备操作可临时 Attach/Publish，完成后释放或接续使用权。

## 1 三个边界

| 层次 | 本方案处理 |
| --- | --- |
| 标准 CSI 协议与厂商 Driver | 复用 `csi.v1`；Controller + Identity 与 Node + Identity 保持标准服务，不修改协议 |
| Kubernetes 卷管理 | 原生 StorageClass/PVC/PV、PV Controller、external-provisioner 负责供卷和绑定；原生快照、克隆、扩容和回收按需启用 |
| 沙箱消费 | 公共消费协调模块直接 Attach；Node Agent 直接 Stage/Publish；Runtime Adapter 配置 VMM 与 guest |

CSI 定义调用方与 Driver 的协议，不要求调用方必须是 kubelet。Substrate 的 Plugin 是 CSI 客户端适配器，厂商 Driver 才是服务端；提取前者作为调用骨架，继续复用后者。[CSI 规范](https://github.com/container-storage-interface/spec/blob/v1.12.0/spec.md)、[官方部署](https://kubernetes-csi.github.io/docs/deploying.html)

Driver 复用分为代码/镜像复用、运行实例复用和同一卷共同消费，条件各不相同。能够访问 socket 并不自动取得某个卷的管理权。

## 2 对前版方案的收敛

| 前版设计 | 本版决定 | 原因 |
| --- | --- | --- |
| K8S / Direct 两种供卷 Provider | 只保留 Kubernetes 管理入口和 PVC/PV Resolver | 避免两套建卷、回收和快照权威 |
| 框架自主 CreateVolume/DeleteVolume | 项目 API 转换为创建/删除 PVC、PV 或相应原生资源 | 建卷/删卷 RPC 始终由原生组件调用 |
| 默认独立 SSF Controller Deployment | 公共模块嵌入项目控制面；共享服务仅为替代部署 | 消费协调不要求多一个默认 Pod 或业务 RPC 跳转 |
| Gateway 可代理供卷与回收 | 仅允许已授权的 Attach/Detach | 不向消费路径授予 Create/Delete/Snapshot/Expand 权限 |
| Node 准备时回调控制面 Attach | 控制面在创建 RPC 前完成 EnsureAttachment，下发计划 | 接近 Substrate 工作流，减少额外往返，节点不成为第二个 Attach 方 |
| 框架自己的 StorageClass/卷生命周期 | 原生 StorageClass；附加配置仅表达消费策略和端点 | 使用索引与原生管理声明分工清楚 |

这些调整保留持久 operation、使用账本、writer 防护、WFFC、恢复与 GC；它们保障直接调用的正确性，而不重新建立供卷控制器。

## 3 Kubernetes 原生模块对应

| 标准模块 | 本方案中的职责 | 来源 |
| --- | --- | --- |
| API Server、etcd、StorageClass/PVC/PV | 管理声明、身份、版本、配额与持久化 | 原生复用 |
| PV Controller | 静态/动态 PVC/PV 绑定和生命周期处理 | 原生复用 |
| external-provisioner + Controller Driver | 动态 CreateVolume/DeleteVolume、从快照/源 PVC 供卷 | 原生复用 |
| kube-scheduler VolumeBinding | 服务 Pod；独立沙箱由项目选点与 DelayedBinding 承接消费者职责 | 原生保留，新增沙箱桥 |
| external-attacher | 服务原生 Pod；不管理同一 SSF Direct attachment | 条件复用 |
| kubelet Volume Manager | 服务平台/原生 Pod；独立沙箱不走该卷流程 | 原生保留 |
| registrar / CSINode / Node 标签 | 保持拓扑供卷所需的真实注册输入 | 原生复用 |
| CSI Node Driver | 为 Node Agent 提供标准 Stage/Publish 等服务 | Driver 复用 |
| snapshot-controller、snapshotter、resizer | 后端 snapshot/expand 管理；沙箱另做冻结和 guest 配合 | 按需复用 |

供卷 sidecar 创建后端及 PV，PV Controller 完成绑定。CSI Controller Service 不调用 CSI Node Service。

## 4 Immediate 与 WFFC 都正式支持

| 场景 | 创建 PVC 时 | 首次消费时 |
| --- | --- | --- |
| 静态 PV + 预绑定 PVC | 已有后端资源，原生完成绑定 | 遵守既有拓扑，直接消费 |
| 提前创建 Immediate PVC | 原生供卷、生成 PV 并绑定 | 直接消费，不重复 CreateVolume |
| 动态 WFFC | Pending，通常没有对应 PV | 联合选点、计算预留、受控 selected-node 桥、唯一 provisioner 供卷、原生绑定 |
| 静态 WFFC | 已有可选 PV，PVC 可以 Pending | 匹配计算与 PV 拓扑，受控预绑定，由 PV Controller 完成绑定 |

静态/动态供给和绑定模式是不同维度。Immediate 适合提前准备的默认优化；WFFC 是同等正式的节点相关路径。先建卷可能确定 zone，不表示已经选择 Worker 或 Attach。[绑定模式](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode)

独立 VM 不会自动触发 kube-scheduler。DelayedBinding 桥接选点，严格验证原生结果；不 fork scheduler，不把 Pending 卷传给 Node。WFFC 首次供卷/绑定的 API、watch 与后端耗时完整计入启动时间，Bound 后不重复供卷。[桥接细节](05-kubernetes-integration.md)

## 5 为什么参考 Substrate 的消费层

Substrate 由 ateapi 直接调用 Controller、atelet 直接调用 Node，再由 runtime 接入 guest。本方案保留直接执行、接口分层和 host/guest 分层，替换 Actor 专属卷供应模型。[Substrate 原文](../substrate/docs/csi-volumes.md)、[源码复用清单](07-substrate-inspired-implementation.md)

| 参考内容 | 采用方式 |
| --- | --- |
| 标准生成客户端与薄 RPC 包装 | 基本复制并解耦包路径；管理 RPC 不暴露给消费模块 |
| Ensure-step 工作流 | 请求主动推进、结果持久化；后台恢复复用处理器 |
| Attach 后下发 publish_context | 创建 RPC 携带受约束计划，Node 不重新组装后端身份 |
| Stage/Publish 与资源交付 | 公共 Node 执行，项目 Adapter 完成 guest 接入 |
| virtiofs 共享树 | 文件运行时按 transport 支持情况参考，不是所有 VMM 的通用实现 |

不能原样复制固定 Mount/SingleNodeWriter、固定可写、每次 Unpublish 后立即 Unstage、能力失败后假定全支持、Node 名直接作为 CSI node_id 等假设。[实现细化](07-substrate-inspired-implementation.md)

## 6 直接消费的部署选择

默认公共消费协调模块嵌入现有 CubeMaster 或 AgentENV 权威创建/调度入口；Node Agent 是公共 DaemonSet，项目 runtime 使用 Go/Rust SDK。跨语言项目可将同一协调核心部署为内部服务；这是替代方式，不要求每个项目新增 Controller Pod。

共享消费账本需事务语义，默认外部/独立 PostgreSQL；可复用满足一致性与权限要求的项目数据库独立 schema。Guard 默认独立小型 Deployment，避免与不同项目的语言/进程耦合。Node WAL 使用 hostPath 持久化。

Controller 只有 UDS 时，同 Pod 增加 mTLS Gateway；允许身份、探测、能力和适用的卷能力只读查询，有副作用 RPC 仅代理已授权 Publish/Unpublish。原生 sidecar 继续通过原 UDS 建卷/删卷。已有认证网络端点可替代 Gateway；Node RPC 使用本机 UDS。

Direct 模式不同时创建由 external-attacher 管理的同卷 VolumeAttachment。VA/Holder Pod 路线作为单独认证的兼容模式，不进入默认高性能路径。[external-attacher](https://kubernetes-csi.github.io/docs/external-attacher.html)

## 7 Runtime 与 Driver 两种扩展

| CSI 产物 | Runtime Adapter | 当前项目 |
| --- | --- | --- |
| Filesystem target | 文件共享设备、guest 挂载与就绪 | Cube 文件路径最接近 Substrate |
| Block target | 虚拟磁盘、jailer 可见性、guest 槽位与文件系统 | AgentENV 增加 ManagedBlock 消费接口 |

新后端实现 CSI Driver，新沙箱实现 Integration/Runtime Adapter。现有 Cube 插件或 AgentENV 层存储要成为共享后端，需另建标准 Driver/facade，由原生供卷组件管理。

运行时 adapter 同时提供可选的卷规格/内容准备能力。AgentENV 的 ext4 准备属于该能力，由公共框架在 Kubernetes 供卷链中自动驱动；不能仅实现 RuntimeMount，再要求使用者通过另一接口或初始化 Pod 把卷加工成可用状态。

目录不能自动供给块设备型运行时。纯 VM 无须 guest 内再套容器，但需自己的 guest readiness/挂载实现。Driver 不负责理解所有 VMM。

## 8 兼容性与性能的准确范围

Driver 按固定版本、后端范围、身份/凭证、路径、拓扑和能力认证。依赖真实 Pod UID/token 的 Driver 不可用伪造上下文接入；可拒绝该 profile，或另做显式兼容桥。[Pod Info](https://kubernetes-csi.github.io/docs/pod-info.html)、[Token Requests](https://kubernetes-csi.github.io/docs/token-requests.html)

复用运行实例前验证 provisioner/attacher 的后台行为和资源范围，不部署两个竞争相同 identity 的供卷者。Pod 与 VM 对同一卷通过排空交接，不能用两个 staging 根隔离同一云盘的 attachment。

高性能来自移出供卷、减少协调跳转、复用已完成资源。正常消费不靠 Binding CRD 调谐；WFFC 和原生管理保留调谐，未知结果与泄漏保留后台恢复。按相同后端、语义和真实 guest 就绪口径[验收](04-integration-and-validation.md)，所有 Driver 和正确性开销都计入端到端。
