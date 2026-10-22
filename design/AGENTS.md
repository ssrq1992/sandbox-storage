# 沙箱存储设计约定

## 用户指定参考

后续沙箱存储方案优先参考 [K8S CSI 架构分析](assets/K8S-CSI架构分析.md) 的架构表达、模块职责划分和架构图样式。

该文档是参考材料。保持原文不变，除非用户后续明确要求修改；不要因调整沙箱方案、图源或链接而改写该文档。

## 架构内容

- 参考或增强 Kubernetes CSI 时，明确标准 CSI 协议、Kubernetes 集成实现和沙箱新增能力的边界。
- 对照参考文档列出模块职责及对应关系，说明哪些直接复用、哪些适配或替代、哪些新增、哪些可选。
- 分清公共框架模块、项目定制的 Integration/Runtime Adapter、后端存储 Driver 和实际存储后端。公共模块不能仅因图中采用相同颜色就被表述为直接复用了 Kubernetes 代码。
- 区分逻辑职责与部署形态。控制面职责不等于部署在控制平面节点；API 对象、进程内部模块、独立进程、容器、Pod、Deployment 和 DaemonSet 应明确标识。
- 按真实调用关系表达控制面与节点面的协调。CSI Controller Service 不直接调用 CSI Node Service；CSI 管理 RPC 不转发业务 I/O。
- 非 K8S 部署使用实际的宿主进程或服务边界；K8S 部署按真实资源展开，不机械复制参考文档的 Pod 拓扑。

## 架构图样式

- 按说明目的提供关键模块图、完整部署图和模块调用关系图，避免将部署归属、全部调用和细节塞入同一张图。
- 采用参考文档的分层分组、柔和深色背景、低饱和配色和清晰边框。蓝色表示公共模块，棕色表示 Driver 或定制实现，绿色表示存储后端，灰色虚线框表示可选模块；增加类别时提供图例或明确标签。
- 公共框架、项目适配器和存储 Driver 即使采用相近颜色，也要通过名称及分组明确区分。
- 实线表示控制调用，虚线表示对象或状态协调，粗线表示业务数据 I/O。标明关键 CSI RPC、UDS 和实际数据路径，不把并列模块画成串行调用链。
- 标题独占布局空间，避免被嵌套子框覆盖；按图幅调整字号，保证标签可读。必要时合并长连线，并用调用关系表补充，保持逻辑完整。
- Mermaid 优先保留可编辑源码；如需固定布局或导出，另提供 SVG/PNG。不可见布局边只用于排版，不表示真实调用。
- 沿用参考文档已有的状态区分思想，清楚标出卷绑定、节点 attachment、host publish、guest 就绪和数据持久化的区别。

## 后续修改范围

用户本次指定的是后续设计的参考与样式约定，不据此自动重写现有方案或修改参考分析文档。实际方案调整按后续任务范围执行。

## 已确认的沙箱存储架构方向

- 2026-10-21 用户明确：SSF 的运行时 adapter 分为两部分，供卷过程中的运行时定制准备，以及卷到沙箱内部的挂载；两者都应由框架自动编排。用户主线是 Kubernetes 创建 PVC，再调用原沙箱创建接口直接引用 PVC，不要求手工初始化 Pod 或先调项目 import。最新细化见 `21-ssf-volume-provisioning-and-runtime-adapter-design.md`；本轮仅更新设计，不据此声称实现完成。

- 最新决策：卷管理唯一使用 Kubernetes 原生 StorageClass/PVC/PV 和 CSI 配套组件；管理快照/克隆/扩容/后端回收也走原生资源。不得恢复 Direct CSI 供卷 Provider 或由消费端直接 DeleteVolume。
- 沙箱消费参考 Substrate 的客户端、ensure-step、直接 Attach/Node 调用与 host/guest 分层；必须改造固定能力、共享引用、真实 node_id、参数/Secret 和恢复假设。
- 消费协调默认公共模块嵌入项目控制面，不强制新建 ssf-controller Pod；默认新增 Node Agent、独立 Guard及条件 Gateway。共享消费服务仅替代部署，不形成另一份权威。
- 默认控制面先 EnsureAttachment，再随原有创建 RPC 下发完整 NodeVolumePlan；节点执行 Stage/Publish，Runtime Adapter 完成 guest 接入。Gateway 的有副作用 RPC 仅 Attach/Detach，允许必要身份/探测/能力只读查询。

- 主方案基于 K8S CSI 增强，同时正式支持 Immediate 与 WaitForFirstConsumer。Immediate 可作提前供卷的默认优化；节点相关卷由公共延迟绑定协调器承接沙箱选点、资源预留和供卷/绑定。
- 静态/动态供给与 Immediate/WFFC 是不同维度；WFFC 首次消费完成 Bound/CatalogReady 后，也进入同一 Node Agent 同步 CSI 路径，不能降为未来可选功能或一律拒绝 Pending PVC。
- 正常启动不以消费者 Pod、Holder Pod 或 kubelet 卷调谐为前提；后台恢复与 GC 仍保留。
- 不在创建 PVC 时永久固定未来的沙箱 Worker。Immediate 的卷内容准备可按 profile 临时选择准备节点并释放；WFFC 等首次沙箱选点后准备。节点热挂载与卷池是显式优化，不代表所有首次启动。
- CSI Driver 处理后端与宿主目录/块设备，项目 Runtime Adapter 处理 VMM 与 guest；普通 VM 也遵循此分层。
- 供卷、attachment、节点发布和回收分别固定权威；K8S PVC/PV 的独立 VM 引用保护必须补齐，不能只依赖原生 Pod protection。
