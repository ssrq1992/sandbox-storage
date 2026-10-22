# 当前实现架构：独立 SSF 协调服务与 AgentENV 薄接入

更新：2026-10-21。本页描述当前源码和部署材料；真实 Linux/KVM/Kubernetes/厂商 CSI 验证不在本机通过记录内。

## 1. 职责归属

Kubernetes 管理卷资源和供卷，SSF 管理准备、消费、引用和恢复，AgentENV 原 Scheduler 选择计算节点、Orchestrator 管理 VMM。原 API、SDK、Gateway 代理、VolumeManager、SnapshotManager、OverlayBD、P2P、custom extension、builder 和 recorder 保留。

| 模块 | 源码 | 运行位置 |
| --- | --- | --- |
| Consumption / Preparation / Resolver / Ledger | `sandbox-storage/{consume,preparation,kubernetes,ledger}` | 独立 `ssf-coordinator` |
| 运行时注册与 mTLS 服务 | `sandbox-storage/coordinator`、`cmd/ssf-coordinator` | 同上 |
| AgentENV 控制映射与原生计算预留 | `sandbox-storage/adapter/agentenvadapter/control_service.go`、`native_*.go` | 同上，专用表保留在 adapter |
| 原计算策略 / 发现 / 心跳 | `AgentENV/services/scheduler` | 原 Scheduler |
| 轻量 Go 客户端 | `sandbox-storage/sdk/go/control` | 原 Scheduler 内；仅标准库，无 PG/CSI/SSF 核心依赖 |
| 运行时协议、outbox、回收扫描、guest 策略 | `sandbox-storage/adapter/agentenvadapter/runtime` | 链入原 Node 的 Rust crate |
| 设备/工件边界及生命周期调用点 | `AgentENV/src/sandbox/storage/mod.rs` 和原生生命周期 | 原 Node/Orchestrator |
| NodePlugin / SSF Node / WAL | `sandbox-storage/adapter/agentenvadapter`、`node` | 原 Node Pod 的两个常驻 sidecar |
| Guard / 数据库 | `sandbox-storage/guard`、PostgreSQL | 独立 Guard Pod / 独立数据库 |

本轮继续采用 CSI Block/ext4 外部数据盘。`agentenv-block` 是 StorageClass 名，`provisioner` 决定后端 Driver，profile 决定准备规格。legacy OverlayBD 保留；实现原生 OverlayBD CSI Driver 属于另一个范围。

## 2. 逻辑架构

```mermaid
flowchart TB
  subgraph A["AgentENV 原有进程"]
    GW["Gateway：鉴权 / 原 HTTP 和 WS 代理"]
    SCH["Scheduler：发现 / 心跳 / 健康过滤 / 原策略"]
    HOST["Node + Orchestrator：VMM / 冻结屏障 / 真实停止"]
    BR["通用宿主 bridge：设备边界 / 工件删除"]
  end
  subgraph C["sandbox-storage：ssf-coordinator 进程"]
    REG["coordinator.Runtime 注册接口"]
    AD["agentenvadapter：请求 / 模板 / 生命周期映射"]
    CONS["Consumption / Preparation / Resolver / WFFC"]
    LED["Ledger：事务 / 使用权 / 恢复"]
  end
  PG[("PostgreSQL：唯一共享账本")]
  G["独立 Guard"]
  SDK["框架仓库 Rust adapter crate：协议 / outbox / guest 策略"]
  PL["agentenv-storage 节点插件"]
  N["SSF Node：CSI Stage/Publish / WAL"]
  K["Kubernetes：SC / PVC / PV / Snapshot"]
  GW --> SCH
  SCH -->|"mTLS ssf.control.v1"| REG
  REG --> AD --> CONS --> LED --> PG
  AD -->|"只读 ObserveNodes"| SCH
  G --> PG
  K --> G
  CONS --> K
  GW -->|"原创建请求 + 签名计划"| HOST
  HOST --> BR --> SDK -->|"Pod loopback + token"| PL
  PL -->|"mTLS 生命周期回执"| REG
  PL --> N
  N -->|"内容准备回调"| PL
  classDef common fill:#203e58,stroke:#72a6c9,color:#fff;
  classDef custom fill:#594331,stroke:#c99c72,color:#fff;
  classDef native fill:#353e4a,stroke:#9aaabd,color:#fff;
  classDef data fill:#254d40,stroke:#7cbaa0,color:#fff;
  class REG,CONS,LED,G,N common;
  class AD,SDK,PL custom;
  class GW,SCH,HOST,BR,K native;
  class PG data;
```

## 3. 部署架构

```mermaid
flowchart TB
  subgraph NS["agentenv-system namespace"]
    GW["原 Gateway Deployment"]
    SCH["原 Scheduler Deployment：原 gRPC + 9447 只读观测"]
    CO["新增 ssf-coordinator Deployment：2 副本 / 9446 mTLS / 9096 探针"]
    subgraph NODE["原 AgentENV Node DaemonSet Pod：共享 PID namespace"]
      HOST["原 Node / Orchestrator / Firecracker"]
      AD["agentenv-storage 常驻 sidecar：9444 / loopback 9445"]
      SN["ssf-node 常驻 sidecar：9443 / Node WAL"]
    end
    HEAD["原 headless Service / 心跳 / P2P"]
  end
  GUARD["SSF Guard Deployment：webhook / 受限数据库角色"]
  PG[("独立 PostgreSQL")]
  CSI["厂商 CSI Controller / 可选 Gateway"]
  DN["厂商 CSI Node DaemonSet"]
  BACK[("后端存储")]
  GW --> SCH --> CO
  CO -->|"ObserveNodes"| SCH
  CO --> PG
  GUARD --> PG
  CO --> CSI
  GW --> HOST
  HEAD -.-> HOST
  HOST --> AD --> SN -->|"UDS Node RPC"| DN
  AD --> CO
  CO --> SN
  HOST ==>|"virtio Block 数据 I/O"| BACK
  CSI --> BACK
  classDef common fill:#203e58,stroke:#72a6c9,color:#fff;
  classDef custom fill:#594331,stroke:#c99c72,color:#fff;
  classDef native fill:#353e4a,stroke:#9aaabd,color:#fff;
  classDef data fill:#254d40,stroke:#7cbaa0,color:#fff;
  class CO,SN,GUARD common;
  class AD,CSI,DN custom;
  class GW,SCH,HOST,HEAD native;
  class PG,BACK data;
```

协调服务是唯一消费执行方。NodePlugin 不再回调 Scheduler 的 StorageControl，也不在 standalone 内嵌数据库和协调器。standalone 增加本机 `ssf-coordinator.service`，保留原 AgentENV 服务和固定本机 placement。

## 4. 创建与生命周期

```mermaid
sequenceDiagram
  participant U as 用户
  participant K as Kubernetes/CSI provisioner
  participant G as 原 Gateway
  participant S as 原 Scheduler
  participant C as SSF coordinator
  participant N as 原 Node/Orchestrator
  participant A as 节点 adapter + SSF Node
  U->>K: 创建 PVC
  C->>K: 周期观察 / profile / UID 固定
  Note over C,A: Immediate 自动准备后释放临时使用；WFFC 等原 Scheduler 选点
  U->>G: 原创建 API，volumeMounts 引用 PVC
  G->>S: 签名原请求
  S->>C: ResolveLaunch：固定模板/PVC，返回可行候选
  C->>S: mTLS ObserveNodes：原生发现/健康状态
  Note over S: 原策略选择节点
  S->>C: PrepareLaunch：只提交选中的节点
  C->>C: 唯一计算预留 / WFFC / 准备 / Acquire / Attach
  C-->>S: 完整签名 dispatch
  S-->>G: 节点 + dispatch
  G->>N: 原请求 body + 内部签名计划
  N->>A: Prepare / 原生启动 VMM / guest 验证
  A->>C: Ready + 独立可信回执
  C->>C: Commit
  N-->>G: 原生 201
  G-->>U: 原生 201 + native ID
  Note over N,C: 暂停、恢复、Fork、快照及删除仍由原 Orchestrator 发起
```

`ResolveLaunch` 持久固定请求、模板和卷引用；`PrepareLaunch` 只处理原策略选中的节点。已取得预留但结果未知时，`InspectOperation` 返回原节点或原 dispatch；只有明确尚无预留的容量拒绝才能由原策略重选。后台恢复不重放公开创建 API。

暂停仍在原冻结屏障中保留逻辑使用权，停止后释放当前 run；恢复、Fork、builder、recorder 使用相同计算预留。外部卷通过公共准备及 Endpoint 接入，native memory/rootfs、legacy 和 CSI 卷联合捕获。recorder 使用独立 owner/run 和 checkpoint 克隆，不复用源 writer。

Rust crate 外移的是代码归属，outbox 仍在原进程的副作用边界同步落盘。宿主负责原生设备路径/major/minor、受限 guest 执行、真实进程停止、最后工件删除。服务端不接收任意宿主删除路径。

## 5. 运维与验收

部署 overlay、联合 Dockerfile、配置和凭据工具在 `sandbox-storage/adapter/agentenvadapter/deploy`；引用原 AgentENV base。Scheduler 无 SSF 数据库 Secret、无消费 RBAC、无 SSF modfile/tag。可关闭的 StorageProvider 和无卷路径保留。

[实施和协议](26-independent-coordinator-implementation.md) · [部署](13-agentenvadapter-installation-and-deployment.md) · [测试矩阵](14-agentenvadapter-real-environment-test-plan.md) · [PVC 手动主场景](20-kubernetes-pvc-agentenv-mount-validation.md)。验证结果以本轮报告为准，旧 18/22 中的测试不能替代本轮测试。
