# 可复现验证

`scripts/integration.sh` 创建独立 PostgreSQL 17.4 容器，运行事务测试，然后构建特权 Linux 测试镜像，分别执行文件 bind mount 与 loop 块设备测试。测试驱动不属于生产 CSI Driver。失败时保留测试路径，避免递归删除仍有挂载的目录；根据日志先确认卸载与 loop 设备释放。

`scripts/kind-fixture.sh` 创建 `ssf-test` 集群（kindest/node:v1.35.0），安装固定版本的 provisioner、registrar、resizer，提供 Immediate 和 WFFC 两类 StorageClass。fixture ServiceAccount 使用 cluster-admin 仅为隔离测试简化配置，不复制到生产。

运行环境要求：Linux 或可运行特权 Linux 容器的 Docker、Go 1.26、kind、kubectl。块卷测试要求宿主内核可用 loop 设备。所有脚本显式操作独立测试集群，不使用默认生产 context。

集群测试从 `SSF_TEST_KUBECONFIG` 读取专用 kubeconfig，选择 `ssf.dev/enabled=true` 节点，测试真实动态供卷和 WFFC；未设置时显示 SKIP。静态选卷、拓扑、UID、防删除等还有 client-go fake/AdmissionReview 单元测试，不能视为集群 E2E。

快照集成需要另外安装 external-snapshotter v8.2.0 的 v1 CRD、snapshot-controller、csi-snapshotter sidecar 和对应 VolumeSnapshotClass。安装包来源固定为 `kubernetes-csi/external-snapshotter` 的 v8.2.0 tag。fixture 已实现 Create/DeleteSnapshot，但普通 VM 的一致性快照还需要真实 Runtime Freeze/Thaw；公共 verification adapter 不声明支持。

真实集群 Guard 证书、API-server webhook 调用、原生扩容/快照/回收与故障矩阵仍需在测试环境执行并记录，不能根据 Go 单测宣称通过。生产启用前需要验证厂商 Driver 的真实 node_id、宿主目录可见性、Secret、拓扑和 attach 行为。
