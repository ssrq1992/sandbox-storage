# SSF 新需求、架构问题与实现偏差汇总

> 2026-10-21 独立化更新：本页保留此前设计/阶段记录。当前 AgentENV 的消费与准备协调、账本事务在 `sandbox-storage` 的独立 `ssf-coordinator` 中执行；Scheduler 为轻量客户端，standalone 同样独立。以 [03](03-framework-design.md)、[23](23-current-implementation-architecture.md)、[26](26-independent-coordinator-implementation.md) 和 [13 部署](13-agentenvadapter-installation-and-deployment.md) 为准。旧内嵌宿主、`go.ssf.mod`、`-tags ssf`、原仓库 SSF overlay 不再适用。本次保留 Block/ext4 和全部原生功能，未新增 OverlayBD CSI Driver。

更新：2026-10-21。本文汇总近期讨论，供后续设计和实施确认范围使用。**需求、当前实现事实、可行性建议分别记录；讨论中的建议不视为已经批准的实施方案。** 本文不表示新需求已经实现，也不触发软件改造或集群部署。

## 1. 用户明确的目标

### 1.1 SSF 是公共存储框架，运行时通过 adapter 接入

- 类比 Kubernetes CSI 的分工，SSF 负责公共存储管理衔接、使用协调和保护，各沙箱运行时实现自己的 adapter。
- adapter 主要有两部分：创建卷时满足运行时的定制需求；将卷接入运行时自己的沙箱内部。
- 保留沙箱原有模块架构、功能、部署形态及生命周期接口，尽量少做侵入式修改。
- AgentENV 应保留原 Gateway、Scheduler、Node、Orchestrator、SDK 及创建、删除、暂停、恢复、快照、Fork、模板等原有语义。
- 不接受 adapter 另建独立沙箱控制面、绕开 Gateway/Scheduler 或接管 VMM 来替代原架构。

### 1.2 用户操作应保持两步

1. 使用 Kubernetes 创建 PVC；动态供卷由其 StorageClass 对应的存储实现完成，静态场景可预先提供 PV。
2. 调用原 AgentENV 创建沙箱接口，通过 `volumeMounts` 关联该 PVC，并在沙箱内使用挂载路径。

不应要求用户先创建初始化 Pod、手工格式化或调用 `/volumes import`。普通删除沙箱应保留用户卷和数据。自动准备可以是内部过程，但不能变成额外的人工使用门槛。

### 1.3 最新明确：AgentENV 接入要满足原生 OverlayBD 卷语义

用户明确指出的初衷是：

> agentenvadapter 配套提供 StorageClass，通过 Kubernetes 创建出来的卷应满足 AgentENV 原生 OverlayBD 卷要求，再直接接入 VM，尽量复用原有挂载能力。

这与“给 AgentENV 增加通用 CSI 块盘能力”不是同一个目标。后续设计不能只以通用块盘可挂入 Firecracker 为依据，宣称已经满足原生 OverlayBD 接入。

技术上，StorageClass 只是配置对象；实现该目标还需要真正的供卷和节点接入逻辑。adapter、CSI Driver 与原生 VolumeManager/repository 的职责边界尚需细化。

### 1.4 验证和文档要求

- 已部署环境的初步验证围绕简单场景：创建 PVC → A 挂载 `/data` 写笔记 → 删除 A → B 挂同一 PVC 读回笔记。
- 提供逐步手动命令、预期结果和失败检查点，不要求运行自动化脚本。
- 文档应明确镜像、CLI、权限和环境前提，不能留下操作者无法理解或执行的隐含依赖。
- 文档分析与可行性讨论阶段不修改软件；历史文档清理先分析，不直接删除。

## 2. 当前实现事实及与需求的偏差

### 2.1 当前新增路径

```text
Kubernetes PVC / StorageClass
    ↓
通用 CSI Driver 提供块卷
    ↓
SSF Preparation + AgentENV 准备插件
检查设备、按规则准备 ext4、保存回执并释放准备资源
    ↓
SSF Consumption 获取运行使用权、Attach、生成计划
    ↓
AgentENV 新增 PreparedExternalDrives 路径
    ↓
Firecracker / guest 挂载 /data
```

原生 legacy OverlayBD 路径仍保留，但**新 PVC 数据卷路径不经过原生 OverlayBD 卷创建和层管理流程**。

### 2.2 差异表

| 维度 | 用户最新明确的目标 | 当前实现 |
| --- | --- | --- |
| 供卷结果 | AgentENV 原生 OverlayBD 卷及其语义 | 通用 CSI Block 卷 |
| 定制准备 | 原生卷结构、层描述及元数据衔接 | 空白检查、ext4 初始化/核验 |
| 运行时接入 | 尽量复用原 OverlayBD/ublk/ExtraDrive 路径 | 新增外部块设备接入 |
| 快照与发布 | 对齐原生层封存、restack、发布及引用 | 通用 CSI 快照/克隆与原生命周期协同 |
| OverlayBD CSI 供卷 | 需要落实的能力 | 本次实现尚未交付 |

此前计划采用“Block/ext4 首规格、不强制把 legacy OverlayBD 改成 CSI Driver”的边界，实施因此走向通用块盘接入。用户后续明确了原生 OverlayBD 目标，需重新对齐这部分设计；不能把此前边界继续作为已经满足最新目标的理由。

## 3. 关于原生 OverlayBD 的问题与澄清

### 3.1 原生 AgentENV 如何挂载

```text
VolumeRecord：卷 ID、容量、backing_layers、状态
    ↓
repository 解析层引用，物化节点本地 image.json
    ↓
ExtraDrive::Overlaybd
    ↓
UblkDeviceManager / ublk daemon 创建运行设备
    ↓
宿主块设备 → Firecracker 虚拟磁盘
    ↓
guest 挂载磁盘内的文件系统
```

`image.json` 是描述，不是完整磁盘数据。原生空卷创建也可以先创建 ext4，再封装成 OverlayBD 层；所以 ext4 与 OverlayBD 并非互斥概念，关键区别是是否保留完整的分层结构、元数据和生命周期。

### 3.2 挂载的是远端存储，还是 OverlayBD 卷

需要区分三个层次：

| 层次 | 作用 |
| --- | --- |
| 对象存储、镜像仓库或 POSIX 目录 | 保存层数据、制品和元数据 |
| OverlayBD 逻辑卷 | 将只读层、运行时可写层组合成块设备视图 |
| VM 内磁盘/文件系统 | guest 识别虚拟磁盘，将其文件系统挂到 `/data` |

VM 接入的是 OverlayBD 提供的逻辑磁盘，guest 挂载的是文件系统，不是直接把对象存储目录挂入 VM。远端存储是数据来源/持久化位置，不与逻辑卷构成二选一。

### 3.3 是否必须远端存储、写入是否立即远端持久化

- 当前原生代码有 POSIX 和 OSS repository 后端，层来源可以是本地文件或远端来源；具体取决于部署配置。
- 读取可以利用本地层/缓存，缺失内容按配置从远端获取。
- 可写卷的修改先进入运行时 upper，后续通过封存、快照或 backing 发布流程形成可引用的层。
- guest `sync` 不能单独证明最新 upper 已完成远端发布；原生 `Uploading / Ready / Failed` 状态和发布证据仍然重要。
- Kubernetes 登记成功不自动赋予本地 writable upper 跨节点共享能力。

## 4. 关于测试 Driver 和部署组件的问题

### 4.1 `agentenv-block` 是什么

它是 StorageClass 名字，不是 Driver 名字。实际后端由 `provisioner` 决定：若为 `test.block.ssf.dev`，则使用测试块 Driver。`ssf.dev/volume-profile` 决定准备规则，不会自动把后端变为 OverlayBD。

用户集群中该 StorageClass 的实际 provisioner 尚未在本次讨论中通过查询输出确认。

### 4.2 `ssf-fixture/ssf-block-*` 做什么

- 提供测试 CSI Controller/Node 及配套 sidecar。
- Block 模式通过本地文件保存数据，`losetup` 生成块设备，bind mount 发布。
- 快照以复制后端文件实现；Controller Attach 主要模拟节点关联。
- 执行真实文件和 Linux 设备操作，但不是云盘/分布式存储，不具备由此推导的高可用、复制和跨节点共享能力。
- 该 Pod 中基础容器包括 driver、registrar、provisioner、resizer；其他容器须查实际清单，不能由 `6/6` 推断名称。

代码入口：[main.go](../sandbox-storage/cmd/ssf-test-driver/main.go)、[driver.go](../sandbox-storage/testing/csidriver/driver.go)、[render_fixture.py](../sandbox-storage/scripts/render_fixture.py)。

### 4.3 用户提供的其他 Pod

| Pod | 作用/确认边界 |
| --- | --- |
| 两个 `ssf-guard-*` | 同一准入保护服务的两个副本 |
| `ssf-postgres-*` | 按名称为账本数据库；保存元数据，不保存 guest 文件内容 |
| `ssf-migrate-* Completed` | 按名称推断为数据库迁移任务；具体 command/owner 需查询确认 |

`agentenv-storage`、`ssf-node` 在当前 AgentENV overlay 中作为原节点 Pod 内的常驻 sidecar。它们不会各自出现在独立 Pod 名称列表中。

## 5. 公共控制协调、账本和 Guard 的职责

| 组件 | 作用 | 当前运行位置 |
| --- | --- | --- |
| Consumption Coordinator | 解析授权、联合放置/绑定、确保准备、获取使用权、Attach、签名计划、Commit/Release | 原 Scheduler 内的 Go 库 |
| Preparation Coordinator | 准备状态、独占、节点调用、恢复、准备与运行交接、删除排空 | 原 Scheduler 内；后台周期 List/Reconcile |
| `SSF/ledger` | 事务、锁、条件更新；writer、Binding、owner/run/epoch、hold、attachment、引用与操作步骤 | 主要在 Scheduler 内运行，也被 Guard/Gateway 按职责使用 |
| PostgreSQL | 持久保存共享账本 | 独立/外部数据库 |
| Guard | Kubernetes 准入默认化与校验，防争用、防身份篡改、防违规回收 | 独立服务 |

账本不只是数据库组件，也包含公共代码逻辑。多个 Scheduler 副本运行相同代码、访问同一个数据库，由事务保证使用权一致。Node 本地 bbolt WAL 与共享账本分工不同。

Guard 不执行格式化或挂载，也不在 webhook 中等待排空。受 finalizer 保护的 DELETE 可以进入 Terminating，后台等待准备、运行、暂停和快照引用解除后再推进删除，不为删 PVC 擅自停止沙箱。

## 6. 将协调器与账本逻辑独立成组件的讨论

### 6.1 用户提出的问题

Consumption Coordinator、Preparation Coordinator 和 ledger 代码能否从项目控制面抽出，作为单独组件？

### 6.2 可行性结论

**可以，但当前没有交付该独立公共服务。** 当前是进程内库调用，需要补认证 RPC、操作身份、查询/重试/取消和调度协作协议，不能只更换 Deployment。

讨论中的候选结构：

```text
原项目 Scheduler + 项目 ControlPlugin
    ↓ 认证 RPC
SSF Coordinator Service
    ├── Consumption
    ├── Preparation
    ├── Checkpoints / 恢复与排空
    └── ledger 事务逻辑
           ↓
       PostgreSQL
```

原节点发现、计算调度、资源预留、模板语义、VMM 管理仍保留在项目侧。

### 6.3 尚未批准实施的事项

- 是否正式采用独立服务，还是继续默认嵌入式。
- 是否允许 Guard/CSI Gateway 保留受限数据库访问，还是只允许服务访问数据库。
- PlacementProvider 如何远程化，WFFC 选点和计算预留如何交接。
- 服务多副本、超时未知结果、故障恢复、认证和版本兼容策略。

此前建议是将协调与 ledger 逻辑放在同一个服务内部，保留模块边界，不额外把 ledger 拆成另一微服务；这是建议，不是用户已确认的最终设计。

## 7. 原生 OverlayBD 接入的可行性与待决策事项

### 7.1 已得到的分析结论

可以保留公共框架总体架构，复用原 VolumeManager、repository、OverlayBD/ublk 和原 VMM 能力，但需要实质性新增或重构存储接入。不是仅修改 StorageClass、profile 或格式化命令。

### 7.2 两种讨论过的实现方向

| 方向 | 思路 | 与用户目标关系 |
| --- | --- | --- |
| A：CSI 发布 OverlayBD 块设备 | Driver 使用 OverlayBD/ublk，输出标准 Block Endpoint，再走外部设备接入 | 复用公共框架较多，但不充分复用原 ExtraDrive 接入链 |
| B：CSI 管理逻辑卷、adapter 衔接原生描述 | Kubernetes 管理卷，运行时通过原 OverlayBD/ublk 路径创建和挂载设备 | 更贴近用户原生复用诉求；需明确 CSI Node 发布资源与设备所有权 |

两种方向均尚未形成批准实施的详细设计。不能把“逻辑卷描述/配置文件”假装成标准 CSI Block 发布结果。

### 7.3 必须先明确的问题

1. **身份映射：** PVC/PV UID、CSI handle、原生 volume ID、repository 记录如何稳定关联。
2. **唯一管理权：** Kubernetes 管理卷生命周期与原生 VolumeManager/repository 操作如何衔接，避免两套独立删除权威。
3. **设备所有者：** CSI Node 与原 AgentENV 谁创建、持有、销毁 ublk；禁止重复创建设备或双方都负责释放。
4. **使用权衔接：** 原生 reserve 与 SSF writer/hold 如何形成单一有效授权关系。
5. **交付契约：** Node Endpoint、原生层描述、image config 和本地缓存路径如何安全传递与恢复。
6. **发布与迁移条件：** latest upper、Uploading/Ready、远端持久化、节点故障和换节点恢复如何判定。
7. **完整生命周期：** freeze/seal/restack、联合快照、Fork 独立 upper、模板引用、删除和 GC 如何保持一致。
8. **失败恢复：** 供卷中断、未知设备创建、层上传失败、回执丢失的幂等与引用保留。
9. **既有路径关系：** 通用 CSI Block/ext4 是否作为另一种能力保留，如何与原生 OverlayBD profile 区分；不直接假定废弃或迁移现有卷。

## 8. 测试操作中的疑问与文档改进点

| 用户问题 | 已澄清内容 | 后续文档应落实 |
| --- | --- | --- |
| `AENV_IMAGE` 是什么镜像 | 原 `/sandboxes-cold` 的沙箱工作负载镜像，不是 Gateway/SSF/CSI 镜像 | 使用环境中已验证的镜像示例，说明 envd 和测试工具前提 |
| `aenv exec` 为什么 command not found | 操作机缺少 CLI 或 PATH 配置，不代表卷挂载失败 | 增加 CLI 获取/构建、安装与连接配置步骤 |
| `aenv` 在哪里 | 项目安装脚本默认 `/usr/local/bin/aenv`；构建通常在 `AgentENV/target/{debug,release}/aenv`；cargo install 通常在 `~/.cargo/bin` | 按实际安装方式说明，不假定部署服务端就有本机 CLI |
| 是否只需 PVC + 创建接口 | 是用户主线；准备应由内部自动完成 | 避免把内部检查步骤写成额外业务接口要求 |
| 为什么有这么多测试步骤 | 主场景只需验证挂载、写入、删除沙箱后持久化 | 保持最简主线，完整生命周期作为进阶验收 |

项目 `scripts/install.sh` 同时涉及服务端安装/配置，不能不加说明就作为“只装 CLI”的操作推荐。

## 9. 文档整理要求及现有索引

用户要求分析 `design` 中可删除文档，**只分析、不删除**。此前建议：09/10 归档、15 合并后删除、11 统一覆盖状态，04/07/17 精简历史内容；尚未据此执行清理。

| 文档 | 用途 |
| --- | --- |
| [03 公共架构](03-framework-design.md) | 总体设计依据，包含历史/目标内容；不能逐句当当前实现事实 |
| [20 手动 PVC 验证](20-kubernetes-pvc-agentenv-mount-validation.md) | 当前通用 CSI PVC 的简单手动场景 |
| [21 准备与两部分 adapter 设计](21-ssf-volume-provisioning-and-runtime-adapter-design.md) | 此前准备/PVC 增量设计；尚未完整表达本次原生 OverlayBD 诉求 |
| [22 实现与验证记录](22-volume-preparation-implementation.md) | 本机检查和未执行真机项目，不是用户集群通过证明 |
| [23 当前实现架构](23-current-implementation-architecture.md) | 逻辑/部署/调用时序，以及 CSI 与 legacy OverlayBD 路径区别 |
| [24 公共框架扩展点](24-ssf-framework-architecture-and-extension-points.md) | 实际接口、装配方式及未提供的通用能力 |
| 本文 25 | 最新需求、问题、实现偏差和待决策事项汇总 |

## 10. 后续工作的约束

- 当前请求仅为需求与问题汇总，不执行 OverlayBD 改造、协调器服务化或文档删除。
- 不把可行性结论写成已交付能力，不把建议方向写成用户已批准方案。
- 原生 OverlayBD 接入与 Coordinator 服务化是两个独立决策，不能默认捆绑为同一次改造。
- 后续若获准实施，需先确定资源和设备所有权，再确定协议与部署，最后实施和验证完整生命周期。
- 继续保留原生架构、用户数据和已有工作区改动；未授权不提交、推送或部署外部集群。
