# 独立协调服务的真实环境验收

更新：2026-10-21。以下项目需在部署后的 Linux/KVM/Kubernetes/CSI 环境手动或显式运行工具验证。本机 Go/PG/Rust 检查不填充这些项目的“通过”状态。

## 1. 最小主线

使用 [20 手动测试](20-kubernetes-pvc-agentenv-mount-validation.md)：Kubernetes 创建 profile 管理的 PVC → 调原 AgentENV 创建 A，挂 `/data` → 写入唯一测试内容并 sync → 删 A → 原 API 创建 B 引用同一 PVC → 读回。先 Immediate，再 WFFC；动态及静态供给分别记录。无需初始化 Pod、手动格式化或预先 `/volumes import`。

同时观察：Gateway/Scheduler/Node 仍是原工作负载；新增 ssf-coordinator；Scheduler 没有数据库连接，NodePlugin 命令直达协调服务；无逐沙箱消费者 Pod。删除沙箱后 PVC/数据保留。

## 2. 原生回归矩阵

每行分别执行“StorageProvider 关闭”“启用且无受管卷”“启用且混合 legacy/CSI”。未适用写明原因，不把未测列记作通过。

| 功能 | 操作与预期 | 初始状态 |
| --- | --- | --- |
| 冷启动/模板/v2 | 原请求/原 native ID/201；list/get/delete 正常 | 未执行 |
| HTTP/WS/流式/显式 proxy | 原鉴权、路径与协议；不重复拼 `/proxy` | 未执行 |
| custom extension/P2P | 原回调、P2P 查找与传输不被存储服务替代 | 未执行 |
| builder | 原模板构建；SSF 开启时进入同一容量准入 | 未执行 |
| startup recorder | 开启原 recorder 设置，发布含 CSI 卷的快照；观察新的 run/owner、独立克隆、recording trace 和清理；源沙箱继续写不冲突 | 未执行 |
| pause/resume/autoResume | 写入、暂停、自动或显式恢复；原 ID，run/epoch 更新；Pausing 期间恢复等待 | 未执行 |
| 联合快照 | legacy/CSI 和 native memory/rootfs 在同一屏障；失败解除冻结，引用保留 | 未执行 |
| Fork | 原父节点放置与部分成功；子可写卷独立，互相写不影响，RO 按原语义共享 | 未执行 |
| 模板继承/[]/替换 | 省略继承；空数组清空公开挂载；替换旧路径仍保护隐藏克隆、盘位和旧 FD | 未执行 |
| 离线扩容 | 有 writer/hold 时拒绝；离线扩后 bytes/UUID/内容匹配 | 未执行 |
| 卷删除保护 | 活动/暂停/快照引用期间不回收；普通删沙箱不删用户 PVC | 未执行 |

## 3. 服务边界和故障

1. 正常运行 A，停止一个 coordinator Pod；A 的已有数据 I/O 应继续。恢复服务后读取同一 PVC；记录 DB run/attachment、Node WAL 和进程事实。
2. 同时发出多个创建请求超过一台节点容量；只有原 Scheduler 能选节点，持久预留总量加原生已分配不能超限。使用同一 PVC 并发可写时只允许一个 writer。
3. 在 PrepareLaunch 形成预留/Attach 后切断响应；恢复同一 launch，检查 InspectOperation 固定原节点/dispatch。不能因超时启动第二个 VMM。不得用重新发一个新 request 的成功代替同一次未知操作恢复验证。
4. 分别在 spawn、Ready/Commit、暂停、freeze、联合发布、stop/release 阶段中断服务或节点插件；恢复后必须有真实停止证据才释放，旧 epoch 不能恢复 writer。检查冻结解除、原生句柄和引用没有丢失。
5. 将一个组件改为旧 `scheduler_endpoint`/旧协议或错误 mTLS 身份，应明确拒绝；不能绕过卷准备或以空卷回退。
6. 正常创建前后读取原 API、数据内容及 ledger/node 证据。仅看到 HTTP 200、Pod Running 或 PVC Bound 不算挂载就绪。

推荐工具均保留显式环境参数、记录/执行开关和独立结果目录：`native_preflight.py`、`native_evidence.py`、`native_lifecycle_e2e.py`、`native_compatibility_e2e.py`、`native_pvc_e2e.py`、`native_fault_e2e.py`、`native_invariants.py`、`native_cleanup.py`。先使用 `--help` 查看参数；故障脚本需要明确场景命令且不隐式操作集群。fixture 注入与真实厂商 CSI 结果分开保存。

## 4. 本地检查入口与报告

在 `sandbox-storage` 下：

```bash
python3 scripts/check_control_boundary.py
python3 scripts/validate_native_foundation.py --output validation/control-service/foundation
python3 scripts/native_database_test.py --postgres-bin /path/to/postgresql/bin --output validation/control-service/database
python3 scripts/validate_native_deployment.py --kustomize /path/to/kustomize --output validation/control-service/deployment
cargo test --locked --manifest-path adapter/agentenvadapter/runtime/Cargo.toml
bash scripts/check_agentenv_linux.sh
```

数据库脚本只启动临时本机 PostgreSQL 并清理；不连接现有数据库。Linux 脚本执行原生 Rust 可执行安全测试；macOS 交叉类型检查单独记录。完整矩阵按“通过 / 失败 / 未执行”填写，保留执行环境、Driver 类型、时间、版本/源码哈希、数据内容和实际日志。报告不得包含 Secret 或 API Key。
