# Standalone 进程单元

这些 systemd 单元定义独立 daemon、原生宿主、CSI Node、repository worker 和 Controller
的进程边界，未在 Linux 实机执行。原 AgentENV、storage provider、SSF Node、Coordinator
服务继续使用原入口。无 Kubernetes 的 legacy 模式无需启用这些服务；PVC 模式仍需要
Kubernetes、CSI sidecar、Snapshot controller、Guard 和可访问的 PostgreSQL。

配置放在 `/etc/ssf-overlaybd`，结构参见 Driver 根目录 JSON 示例：

| 文件 | 使用者 |
| --- | --- |
| `native.json` | 原生宿主，必须使用与 legacy 隔离的 repository/state/socket |
| `node.json` | CSI Node，真实 node UID、boot 与公共 Node 的根目录一致 |
| `worker.json` | 可选 mTLS repository worker，仅允许 Controller 身份 |
| `controller.json` | CSI Controller，standalone 必须设置 `kubeconfig` |
| `overlaybd.json` | 原生 OverlayBD 后端配置及读取凭据 |
| `resize.json` | daemon 的隔离 resize 配置；扩容需显式启用下面的配置 |

证书、密钥、数据库 URL 文件必须单独提供，单元中没有默认凭据。受管 daemon 的端口
9106 与原生 daemon 分开；UDS 目录 0700。`/var/lib/ssf/overlaybd` 必须位于持久磁盘。
本地 repository 必须绑定唯一真实节点；共享 POSIX/OSS 配置才能使用空拓扑。

启动顺序为 daemon → host → CSI Node → 公共 SSF Node → provider → AgentENV；停止时
systemd 对同时停止的单元反向排序。Controller/worker 独立于设备 I/O，不能通过停止它们
推断 VMM 已停止。只停止某一个单元不等于完成联合 drain。

单元禁止超时后 SIGKILL 存储进程：受管会话尚未释放时保留进程和状态，由操作员调查
VMM 停止证据、未发布 capture 和挂载引用，不能删除 WAL 或强杀 daemon“修复”。自动
停止整套服务前仍须使用原生 drain 流程确认 VM 退出及最终发布。完整联合 drain 的自动
验收尚未交付，因此这些单元不代表部署已经通过完整验收。

不建议直接批量 enable。先在隔离 Linux 测试机准备所有配置、执行 `systemd-analyze verify`
检查单元和二进制，再逐项启动并查看日志。macOS 当前无法执行 systemd/ublk 验证，状态
为未执行；本轮没有安装或启动这些服务。

## 可选离线扩容

仅对新建且明确启用的 OverlayBD profile/StorageClass 使用：Controller JSON 设置
`"offlineExpansion": true`，profile 设置 `controllerOfflineExpansion`，StorageClass
设置 `allowVolumeExpansion: true`；Kubernetes 侧仍需 external-resizer。不要将旧 PVC
的不可变 profile 原地改成新规格。

提供 `ssf-overlaybd-daemon.service.d/offline-expansion.conf.example` 作为可选 drop-in。
在 `/etc/ssf-overlaybd/daemon.toml` 配置 `deps_path = "/opt/aenv-deps"`，复用当前原生
AgentENV 已验证的 `/opt/aenv-deps/overlaybd/bin/overlaybd-resize`；安装发行版提供的
`e2fsprogs`。示例检查 `/usr/sbin/e2fsck` 和 `/usr/sbin/resize2fs`，按实际发行版路径
调整后执行 `systemd-analyze verify`。不启用 drop-in 时保留原单元。

能力从 daemon、宿主、worker 到 Controller 显式协商；缺少 resize 工具时拒绝开放扩容。
活动 VM/暂停 hold 阻止设备扩容；PVC 只提交容量增长意图。恢复继续使用原 operation
和维护占用，不能删除 upper/WAL 后重试。当前已交付主链路及部分中断恢复，全部设备
故障矩阵和 systemd 真机验证仍未执行。
