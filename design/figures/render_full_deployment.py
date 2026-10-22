#!/usr/bin/env python3
"""Build the fixed-layout SVG and equivalent editable Mermaid deployment tree."""
from pathlib import Path
from html import escape
import unicodedata

ROOT = Path(__file__).resolve().parent
WIDTH, HEIGHT = 2800, 6050
COLORS = {
    'K': ('#24364a', '#617d9b', '#c3d0dd'),
    'N': ('#244351', '#739bad', '#c8dee6'),
    'P': ('#44392c', '#aa9066', '#dfcfb2'),
    'D': ('#3d3328', '#937b56', '#d8c6a9'),
    'I': ('#2b323e', '#7b8798', '#c6ced9'),
    'B': ('#253a33', '#668b7d', '#b4cfc3'),
}
SVG, NODES, RELATIONS = [], [], []


def wrapped(value, width, size):
    limit = max(12, (width - 24) / size)
    lines = []
    for paragraph in value.split('\n'):
        line, used = '', 0
        for char in paragraph:
            units = 1 if unicodedata.east_asian_width(char) in 'WF' else .56
            if used + units > limit and line:
                lines.append(line)
                line, used = '', 0
            line += char
            used += units
        lines.append(line)
    return lines


def label(x, y, value, width, size=20, color='#c4d0dc', center=False):
    lines = wrapped(value, width, size)
    anchor = 'middle' if center else 'start'
    tx = x + width / 2 if center else x + 12
    spans = ''.join(f'<tspan x="{tx}" dy="{0 if i == 0 else size * 1.35}">{escape(line)}</tspan>'
                    for i, line in enumerate(lines))
    SVG.append(f'<text x="{tx}" y="{y}" font-size="{size}" fill="{color}" text-anchor="{anchor}">{spans}</text>')
    return len(lines) * size * 1.35


def box(key, x, y, w, h, title, origin='I', parent=None, frame=False, optional=False, size=20):
    assert key not in {node['id'] for node in NODES}, key
    fill, stroke, fg = COLORS[origin]
    if frame:
        fill, stroke = '#111c27', '#64798d'
    dash = ' stroke-dasharray="9 5"' if optional else ''
    SVG.append(f'<g id="{key}" data-parent="{parent or ""}" data-origin="{origin}">')
    SVG.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" stroke="{stroke}" stroke-width="{2 if frame else 1.5}"{dash}/>')
    label(x, y + size + 8, title, w, size, fg if not frame else '#d2dce6')
    SVG.append('</g>')
    node = dict(id=key, parent=parent, title=title, origin=origin, frame=frame,
                optional=optional, x=x, y=y, w=w, h=h)
    if parent:
        p = next(n for n in NODES if n['id'] == parent)
        assert x >= p['x'] and y >= p['y'] and x+w <= p['x']+p['w'] and y+h <= p['y']+p['h'], key
    NODES.append(node)
    return key


def leaves(parent, items, x, y, w, h, cols=2, gap=14):
    rows = (len(items) + cols - 1) // cols
    cw, ch = (w-gap*(cols-1))/cols, (h-gap*(rows-1))/rows
    for i, item in enumerate(items):
        key, title, origin, *opt = item
        box(key, x+(i % cols)*(cw+gap), y+(i//cols)*(ch+gap), cw, ch,
            f'[{origin}] {title}', origin, parent, optional=bool(opt and opt[0]), size=19)


def workload(key, x, y, w, h, title, container, items, origin='K', optional=False, cols=1, parent=None):
    box(key, x, y, w, h, title, origin, parent, frame=True, optional=optional, size=19)
    box(key+'_pod', x+12, y+62, w-24, h-74, 'Pod：'+key.replace('_','-'), origin, key, frame=True, size=18)
    box(key+'_proc', x+24, y+103, w-48, h-127, container, origin, key+'_pod', frame=True, size=18)
    leaves(key+'_proc', items, x+36, y+152, w-72, h-189, cols)


def line(points, caption, x, y, width, kind='control'):
    color = '#afc1d3' if kind != 'io' else '#9fbeae'
    dash = ' stroke-dasharray="7 5"' if kind == 'state' else ''
    stroke_width = 4 if kind == 'io' else 2
    pts = ' '.join(f'{a},{b}' for a,b in points)
    SVG.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{stroke_width}"{dash} marker-end="url(#arrow)"/>')
    SVG.append(f'<rect x="{x}" y="{y-22}" width="{width}" height="32" fill="#0f1822"/>')
    label(x, y, caption, width, 18, color)


SVG.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="diagram-title diagram-desc">')
SVG.append('<title id="diagram-title">SSF 存储完整部署图</title><desc id="diagram-desc">展开 K8S 原生组件、CSI Pod、SSF 新增组件、项目定制、宿主进程及各自内部模块。</desc>')
SVG.append('<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#afc1d3"/></marker></defs>')
SVG.append('<style>text{font-family:"PingFang SC","Microsoft YaHei",Arial,sans-serif}text,tspan{dominant-baseline:auto}</style>')
SVG.append(f'<rect width="{WIDTH}" height="{HEIGHT}" fill="#0f1822"/>')
label(36, 48, 'SSF 存储完整部署图：工作负载 → Pod → 容器 / 进程 → 内部模块', 2728, 30)
label(36, 85, '默认部署；K8S 管理正式卷；公共消费模块嵌入项目控制面，默认无 ssf-controller Pod。每类 Pod 画一个实例。', 2728, 20)
for i,(tag,text) in enumerate([('K','原生 K8S / 官方 CSI'),('N','SSF 新增公共实现'),('P','项目定制 / 改造'),('D','存储厂商 Driver'),('I','复用基础软件'),('B','实际存储后端')]):
    fill,stroke,fg=COLORS[tag]
    x=40+i*452
    SVG.append(f'<rect x="{x}" y="111" width="430" height="38" rx="4" fill="{fill}" stroke="{stroke}"/>')
    label(x,137,f'[{tag}] {text}',430,19,fg)
label(36,178,'灰色虚线边框＝可选 / 条件部署；来源仍按 K/N/P/D/I 标签识别。框的包含关系表示部署归属，不表示串行调用。',2728,18)

# K8S control plane: four separate static Pods, never one combined Pod.
box('k8s',32,204,2736,450,'① 原生 K8S 基础设施：kubeadm 静态 Pod 示例；托管控制面可替换为等价服务',frame=True)
workload('kube_apiserver',52,260,650,285,'[K] 独立静态 Pod','容器 / 进程：kube-apiserver',[
    ('api_auth','认证 / RBAC / admission webhook','K'),
    ('api_storage','存储资源 REST / watch / UID 校验','K')],parent='k8s')
workload('kube_controller_manager',722,260,650,285,'[K] 独立静态 Pod','容器 / 进程：kube-controller-manager',[
    ('pv_controller','PV Controller\n绑定 / 回收','K'),
    ('ad_controller','Attach/Detach\n原生 Pod 协调','K'),
    ('protection','PVC/PV\nprotection / GC','K')],cols=3,parent='k8s')
workload('kube_scheduler',1392,260,650,285,'[K] 独立静态 Pod','容器 / 进程：kube-scheduler',[
    ('volume_binding','VolumeBinding 与卷相关过滤','K'),
    ('platform_schedule','调度平台 Pod；不调度独立 VM','K')],parent='k8s')
workload('etcd',2062,260,686,285,'[K] 独立静态 Pod / 可外部部署','容器 / 进程：etcd [I]',[
    ('etcd_kv','K8S API 对象持久化；不同于 SSF 账本','I')],origin='I',parent='k8s')
box('api_objects',52,562,2696,74,'[K] API 对象（不是 Pod）：SC / PVC / PV / CSIDriver / CSINode / VolumeAttachment / Snapshot 对象 / Secret / Service / RBAC\n[N] SSF 登记配置及准入配置：ValidatingWebhookConfiguration → SSF Guard；绑定与保护字段由指定组件维护','K','k8s',size=18)

# Native CSI plus separate guard, ledger and snapshot-controller Pods.
box('cluster_services',32,700,2736,800,'② 原生 CSI 管理与独立保护/账本：集群级职责，不要求位于控制平面宿主',frame=True)
box('csi_controller',52,758,1288,720,'CSI Controller Deployment → ReplicaSet',parent='cluster_services',frame=True)
box('csi_controller_pod',66,814,1260,650,'Pod：csi-controller-xxx；下列为独立容器','I','csi_controller',frame=True)
leaves('csi_controller_pod',[
    ('provisioner','容器 external-provisioner\nPVC/PV → Create/DeleteVolume','K'),
    ('attacher','容器 external-attacher〔条件〕\n原生 Pod / VA Bridge；SSF Direct 卷禁并管','K',True),
    ('snapshotter','容器 external-snapshotter〔可选〕\nContent → Create/DeleteSnapshot','K',True),
    ('resizer','容器 external-resizer〔可选〕\nPVC → ControllerExpandVolume','K',True),
],82,862,1228,246,2)
box('controller_driver',82,1124,605,218,'[D] 厂商 Driver 容器 / 进程','D','csi_controller_pod',frame=True)
leaves('controller_driver',[
    ('c_identity','Identity：Info / Capabilities / Probe','D'),
    ('c_service','Controller：卷、attach、snapshot 等\n后端管理适配 / SDK（实现相关）','D')],96,1175,577,151,1)
box('controller_gateway',705,1124,605,218,'[N] 容器：CSI Controller Gateway〔条件〕','N','csi_controller_pod',frame=True,optional=True)
leaves('controller_gateway',[
    ('gateway_policy','mTLS / 探测 / 能力查询\n副作用仅 Attach-Detach','N'),
    ('gateway_uds','拒绝 Create/Delete/Snapshot/Expand\n消费 RPC → 本地 CSI UDS','N')],719,1175,577,151,1)
box('c_liveness',82,1356,1228,84,'[K] 独立容器 livenessprobe〔推荐〕：调用 Identity Probe\n共享 emptyDir：CSI UDS；所有 sidecar 各自调用 Driver，不串行互调','K','csi_controller_pod',size=18)

# No default ssf-controller Pod. Guard, ledger and snapshot controller are real separate Pods.
workload('ssf_guard',1370,758,440,720,'[N] Guard Deployment → ReplicaSet','容器 / 进程：ssf-guard',[
    ('guard','Admission Guard\n校验删除 / 迁移许可\n拒绝冲突消费者','N'),
    ('guard_ledger','读取共享使用账本\n不在准入内执行副作用','N'),
    ('guard_config','Service / TLS / RBAC\nfailurePolicy Fail\nsideEffects None','N')],origin='N',parent='cluster_services')
workload('ssf_metadata',1828,758,440,720,'[I] 使用账本〔可外部/复用DB〕','容器 / 进程：postgres〔自建〕',[
    ('ssf_sql','事务 / CAS / operation\nwriter、Binding、node pin\n不是PV/PVC管理权威','I'),
    ('ssf_db_disk','独立原生 PVC / 外部存储\n可复用满足条件的项目DB\n不经沙箱卷路径引导','I')],origin='I',optional=True,parent='cluster_services')
workload('snapshot_controller',2286,758,462,720,'[K] Snapshot Deployment〔可选〕','容器：snapshot-controller',[
    ('snapshot_reconcile','原生 Snapshot / Content\n通过 API 与 snapshotter 协作','K'),
    ('snapshot_status','原生快照状态 / 绑定\n生命周期状态协调','K')],optional=True,parent='cluster_services')

# Existing project control Pods contain the new public consumption modules.
box('support_services',32,1546,2736,512,'③ 项目控制面：公共消费模块嵌入现有进程，属于新增代码，不是新增 Controller Pod',frame=True)
workload('cube_master',52,1604,1310,430,'[P] CubeMaster Deployment → ReplicaSet','容器 / 进程：cube-master；P 项目入口 + N 公共消费库',[
    ('cube_volume_api','原API → 原生PVC facade\nlegacy ID / PVC UID 映射','P'),
    ('cube_placement','项目计算调度 / lease\n原Create RPC携带卷计划','P'),
    ('cube_resolver','PVC/PV Resolver\n身份 / 能力 / 保护','N'),
    ('cube_delayed','DelayedBinding\nWFFC动态 / 静态桥','N'),
    ('cube_use','Acquire / Drain / 账本\nwriter、Binding、node pin','N'),
    ('cube_attach','Attachment Coordinator\n直接Attach / BuildNodePlan','N'),
    ('cube_sessions','Controller SessionManager\n身份 / 能力 / 长连接','N'),
    ('cube_steps','Step Runner / Recovery\n请求直接执行、未知结果','N'),
    ('cube_native','Native Lifecycle facade\n原生快照/克隆/回收','N')],origin='P',cols=3,parent='support_services')
box('agentenv_control',1390,1604,1358,430,'[P] AgentENV：既有 gateway 与 scheduler 两个 Deployment','P','support_services',frame=True)
box('agentenv_gateway_pod',1406,1658,1326,90,'Pod → gateway 容器 / 进程 [P]：API 路由 / 卷引用与租户身份传递','P','agentenv_control',frame=True,size=18)
box('agentenv_gateway_module',1420,1699,1298,36,'[P] 项目 Gateway，与 CSI Controller Gateway 不同；原有 API 不直接调用 CSI 管理 RPC','P','agentenv_gateway_pod',size=16)
box('agentenv_scheduler_pod',1406,1760,1326,256,'Pod → scheduler 容器 / 进程 [P]；新增公共消费模块 [N]','P','agentenv_control',frame=True,size=18)
leaves('agentenv_scheduler_pod',[
    ('agentenv_scheduler_module','项目选点 / PlacementLease\n权威创建编排入口适配','P'),
    ('agentenv_resolver','PVC/PV Resolver / facade\n原生资源及身份映射','N'),
    ('agentenv_delayed','DelayedBinding\nWFFC动态 / 静态桥','N'),
    ('agentenv_use','Acquire / Drain / 共享账本\nwriter、Binding、node pin','N'),
    ('agentenv_attach','Attach / BuildNodePlan\nSessionManager / 长连接','N'),
    ('agentenv_steps','Step Runner / Recovery\n创建RPC前完成Attach','N')],1422,1811,1294,189,3)

# Worker node, including processes outside Pods and inside project containers.
box('worker',32,2106,2736,2330,'④ Worker Node：下列 DaemonSet 在每个适用节点各有 Pod；独立 VM 不创建逐沙箱消费者 Pod',frame=True)
box('host_services',52,2168,1310,330,'宿主服务进程：不是 Pod','I','worker',frame=True)
box('kubelet_process',68,2222,1278,165,'[K] kubelet 进程内部模块；原生 Pod 卷流程继续保留','K','host_services',frame=True)
leaves('kubelet_process',[
    ('plugin_manager','Plugin Manager\nCSI 注册 / CSINode 发现','K'),
    ('volume_manager','Volume Manager\n原生 Pod 卷状态协调','K'),
    ('operation_executor','Operation Executor\n仅原生 Pod Node RPC','K')],82,2276,1250,95,3)
box('cri_process',68,2403,1278,73,'[I] CRI runtime：containerd / CRI-O + shim / runc；负责平台 Pod 容器\nCube 的运行时 containerd 是否独立，按项目 profile 配置','I','host_services',size=18)
box('host_resources',1390,2168,1358,330,'宿主内核、持久路径与挂载可见域：不是 Pod','I','worker',frame=True)
leaves('host_resources',[
    ('node_state','/var/lib/ssf/node-state\nWAL / 本地 operation / Binding','N'),
    ('stage_targets','SSF staging / targets\n挂载身份、holder / pin','N'),
    ('kernel_devices','内核 mount / block / ublk / KVM\n设备与文件系统真实数据路径','I'),
    ('visibility','受控 hostPath / mount propagation\nUDS、设备权限、jailer 路径','N')],1408,2230,1322,246,2)

box('node_agent',52,2538,1310,620,'[N] SSF Node Agent DaemonSet','N','worker',frame=True)
box('node_agent_pod',66,2594,1282,548,'Pod：ssf-node-agent-xxx','N','node_agent',frame=True)
box('node_agent_process',80,2641,1254,485,'容器 / 进程：ssf-node-agent；以下均为新增公共内部模块','N','node_agent_pod',frame=True)
leaves('node_agent_process',[
    ('node_api','mTLS gRPC / 身份与票据校验','N'),
    ('node_sessions','DriverRegistry / SessionManager\nNodeGetInfo / 能力 / revision','N'),
    ('prepare_executor','Prepare / Release Executor\n可重入步骤与批量准备','N'),
    ('stage_manager','Stage / Publish Manager\n每卷操作与多卷有界并行','N'),
    ('local_wal','StateStore / WAL\nintent / outcome / pin','N'),
    ('runtime_bindings','RuntimeBinding Registry\n设备身份、Ready / 停止回执','N'),
    ('node_recovery','Recovery / GC\n观测、重试、安全释放','N'),
    ('node_telemetry','节点能力注册 / telemetry\n项目 Coordinator 协同与诊断','N')],96,2698,1222,407,2)

box('csi_node',1390,2538,1358,620,'CSI Node DaemonSet','D','worker',frame=True)
box('csi_node_pod',1404,2594,1330,548,'Pod：csi-node-xxx；Driver 与 registrar 为不同容器','D','csi_node',frame=True)
box('node_driver',1420,2641,818,420,'[D] 厂商 Driver 容器 / 进程','D','csi_node_pod',frame=True)
leaves('node_driver',[
    ('n_identity','Identity：Info / Probe','D'),
    ('n_service','Node Service：GetInfo / Capabilities\nStage / Publish / 逆操作 / 按需 Expand','D'),
    ('n_backend','后端 / mount / device 适配\n存储实现相关模块','D'),
    ('checkpoint_extension','CheckpointExtension〔可选定制〕\n分层引擎 seal / restack 协同','D',True)],1436,2698,786,347,1)
box('registrar',2256,2641,460,193,'[K] 独立容器\nnode-driver-registrar\nGetPluginInfo → Driver\n注册 socket → kubelet','K','csi_node_pod',size=19)
box('n_liveness',2256,2852,460,126,'[K] 独立容器 livenessprobe\nIdentity Probe〔推荐〕','K','csi_node_pod',size=19)
box('node_sockets',1420,3077,1296,47,'[I] CSI UDS 与注册 UDS 分开；Driver 挂载传播到宿主，Node Agent / Runtime 使用受控可见路径','I','csi_node_pod',size=17)

box('cube_runtime_profile',52,3210,1310,936,'CubeSandbox 接入分支：既有部署 + 项目定制','P','worker',frame=True)
box('cube_node_pod',68,3266,1278,430,'DaemonSet cube-node → Pod','P','cube_runtime_profile',frame=True)
box('cubelet_container',84,3317,844,360,'容器 / 进程：cubelet [P]','P','cube_node_pod',frame=True)
leaves('cubelet_container',[
    ('cube_node_bridge','VolumePlugin → SSF Prepare/Release\n现有 storage / legacy 映射','P'),
    ('cube_runtime_adapter','Runtime Adapter / 实例观测\n文件路径、readonly、就绪与清理','P'),
    ('cube_restore','snapshot / restore 集成\n外部卷 Reference/Share','P')],100,3371,812,287,1)
box('cube_s3lvol',946,3317,382,204,'[P] 可选独立容器\ncube-s3lvol\n既有卷/根盘后端\n按迁移权威接入','P','cube_node_pod',optional=True,size=19)
box('cube_init',946,3539,382,138,'[P] initContainer\nwait-node-prep\n启动门槛','P','cube_node_pod',size=19)
box('cube_vmm_processes',68,3714,1278,217,'运行时子进程 / 宿主可见进程：不是新增 Pod','I','cube_runtime_profile',frame=True)
leaves('cube_vmm_processes',[
    ('cube_shim','Cube runtime containerd / CubeShim\nCubeRuntime：按既有 ns/profile 运行','P'),
    ('cube_vmm','VMM / 文件共享进程\nCloud Hypervisor 等 / virtiofsd','I')],84,3770,1246,143,2)
box('cube_guest',68,3949,1278,177,'独立沙箱 VM：不是 Pod，也不是上方容器进程','P','cube_runtime_profile',frame=True)
box('cube_guest_mount',84,4001,1246,105,'[P] guest agent / 文件系统 / 业务进程或容器\n消费共享目录或新增块卷 adapter；确认 GuestReady','P','cube_guest',size=20)

box('agentenv_runtime_profile',1390,3210,1358,936,'AgentENV 接入分支：现有 node Pod 承载 runtime','P','worker',frame=True)
box('agentenv_node_pod',1406,3266,1326,665,'DaemonSet agentenv-node → Pod','P','agentenv_runtime_profile',frame=True)
box('agentenv_container',1422,3317,1294,596,'容器：agentenv；服务与下列子进程都属于此容器执行域','P','agentenv_node_pod',frame=True)
box('agentenv_server',1438,3370,1262,292,'进程：AgentENV server [P]；下列为进程内部模块','P','agentenv_container',frame=True)
leaves('agentenv_server',[
    ('agentenv_volume','VolumeManager facade / 委托\nexclusive / readonly / 发布状态','P'),
    ('agentenv_adapter','Runtime Adapter / ManagedBlock\nSSF SDK、设备槽位与 Ready 回执','P'),
    ('agentenv_checkpoint','checkpoint / fork / restore\nCapture / Clone / 层版本 pin','P'),
    ('agentenv_orchestrator','既有 orchestrator / 节点 API\n计算占用与失败清理协同','P')],1454,3424,1230,220,2)
leaves('agentenv_container',[
    ('ublk_process','子进程 ublk-daemon / OverlayBD\n兼容阶段保留；交接后由 Driver 持有设备','P'),
    ('firecracker_process','子进程 jailer / Firecracker [I]\n配置真实设备、只读属性与恢复槽位','I')],1438,3680,1262,212,2)
box('agentenv_guest',1406,3949,1326,177,'独立沙箱 VM：guest 位于上方 Firecracker 管理的虚机中','P','agentenv_runtime_profile',frame=True)
box('agentenv_guest_mount',1422,4001,1294,105,'[P] guest agent / envd / 文件系统 / 用户任务\n设备识别、挂载和 GuestReady；保留块卷语义','P','agentenv_guest',size=20)

box('plain_vm',52,4184,2696,228,'普通 VM 接入替代分支：独立宿主服务 / 进程，不要求 guest 内再套容器','P','worker',frame=True,optional=True)
leaves('plain_vm',[
    ('plain_adapter','项目管理进程 + Runtime Adapter [P]\nPrepare → VMM 配置 → GuestReady / 停止确认','P'),
    ('plain_vmm','libvirtd / QEMU 或其他 VMM [I]\n文件 transport 或 virtio-blk / virtio-scsi','I'),
    ('plain_guest','独立 guest 内核与文件系统 [P]\n普通业务进程使用目录或磁盘','P')],70,4244,2660,148,3)

# Conditional compatibility and existing project support Pods.
box('conditional',32,4480,2736,990,'⑤ 条件组件与原项目配套 Pod：按功能启用，不属于每次沙箱创建路径',frame=True,optional=True)
workload('cube_bootstrap',52,4538,876,390,'[P] cube-node-bootstrap DaemonSet','容器组；下方每框为独立 init / run 容器',[
    ('cube_boot_invalidate','invalidate-stale-node-prep\ninit：清理过期准备标记','P'),
    ('cube_boot_wait','wait-pvm-host\ninit：PVM 条件门槛','P'),
    ('cube_boot_init','cube-node-init\ninit：宿主目录 / 设备检查','P'),
    ('cube_boot_hold','write-node-prep-ready\nrun：准备状态维护','P')],origin='P',optional=True,cols=2,parent='conditional')
workload('cube_installer',948,4538,876,390,'[P] cube-node-installer DaemonSet','容器组；下方每框为一个独立 install 容器',[
    ('cube_install_shim','cube-shim-install\nshim / runtime 制品','P'),
    ('cube_install_kernel','cube-kernel-install\n内核制品','P'),
    ('cube_install_guest','cube-guest-install\nguest 制品','P'),
    ('cube_install_agent','cube-agent-install\nguest agent 制品','P')],origin='P',optional=True,cols=2,parent='conditional')
workload('cube_pvm',1844,4538,904,390,'[P] cube-node-pvm DaemonSet〔PVM 时〕','容器组；下方每框为独立 init / run 容器',[
    ('cube_pvm_bootstrap','pvm-host-bootstrap\ninit：内核与重启准备','P'),
    ('cube_pvm_hold','hold-pvm-ready\nrun：PVM 准备门槛；不执行 CSI RPC','P')],origin='P',optional=True,parent='conditional')
workload('cube_mysql',52,4950,650,490,'[I/P] 项目数据库 StatefulSet〔可外部〕','容器 / 进程：mysql（或外部项目 DB）',[
    ('cube_db_modules','Cube 卷映射与项目元数据\n与 SSF 账本职责分开','I'),
    ('cube_db_pvc','原生 PVC / 独立存储\n不由 SSF 自管卷引导','I')],origin='I',optional=True,parent='conditional')
workload('cube_redis',722,4950,650,490,'[I/P] 项目 Redis StatefulSet〔可外部〕','容器 / 进程：redis',[
    ('cube_redis_modules','原项目缓存 / 状态用途\n不是 SSF writer 权威账本','I')],origin='I',optional=True,parent='conditional')
workload('cube_minio',1392,4950,650,490,'[I/P] 项目 MinIO StatefulSet〔可外部〕','容器 / 进程：minio',[
    ('cube_minio_modules','S3 制品 / snapshot / 层对象\n沿用项目原有 repository','I'),
    ('cube_minio_pvc','原生 PVC 或独立后端\n不经 SSF 控制面逐 I/O 转发','I')],origin='I',optional=True,parent='conditional')
box('alternative_profiles',2062,4950,686,490,'替代 / 兼容部署：均非默认创建路径','I','conditional',frame=True,optional=True,size=18)
box('shared_coordinator_pod',2076,5005,658,186,'[N] 替代：共享 Coordinator Deployment → Pod','N','alternative_profiles',frame=True,optional=True,size=18)
box('shared_coordinator_process',2090,5055,630,116,'[N] 容器/进程：公共消费协调服务\n替代项目内嵌模块，使用同一账本\n不新增卷生命周期管理权威','N','shared_coordinator_pod',size=18)
box('native_consumer_pod',2076,5206,658,218,'[K/P] 原生消费者 / 显式 Holder Pod〔兼容〕','K','alternative_profiles',frame=True,optional=True,size=18)
box('native_volume',2090,5260,630,148,'[K] 原生消费者容器：kubelet 管理 volume\n[N] Holder Bridge 必须独立认证与交接\n同一卷不能同时由 SSF Direct 管理','K','native_consumer_pod',size=18)

box('backends',32,5510,2736,280,'⑥ 实际存储服务 / 设备：可为外部托管或已有存储集群；其内部拓扑由后端管理',frame=True,origin='B')
leaves('backends',[
    ('data_backend','卷数据：NFS / Ceph / 云盘 / 本地盘\n文件系统或虚拟设备 → 实际数据 I/O','B'),
    ('artifact_backend','项目数据：S3 / COS / snapshot repository\nOverlayBD 层、rootfs / 内存制品及其原有缓存','B')],52,5580,2696,184,2)
label(40,5850,'部署与调用边界：Controller Service 不调用 Node Service；SSF / CSI 管理 RPC 不转发业务数据。',2720,22)
label(40,5892,'Independent VM：卷声明/绑定 → 嵌入式消费授权 / Attach → Node Agent / CSI Node → Runtime Adapter → guest。',2720,20)
label(40,5930,'平台 Pod 的原生卷仍由 kubelet 管理；SSF / 项目数据库等基础设施不能依赖尚未启动的 SSF 自管卷。',2720,20)
label(40,5970,'项目分支按实际部署选用；服务式 Coordinator 仅替代嵌入模式；Cube runtime namespace 按既有 profile。非存储容器省略。',2720,19)

# Only a few relations are drawn; full call paths remain in ssf-module-calls.
line([(215,654),(215,700)],'① API / watch',250,686,290,'state')
line([(2120,1546),(2120,1500)],'② 使用账本 / 准入保护协作',1600,1533,500)
line([(1362,2820),(1390,2820)],'③ Node CSI UDS',1170,2518,360)
line([(68,4075),(14,4075),(14,5550),(32,5550)],'④ VM 实际 I/O 经文件共享或虚拟磁盘到后端',60,5499,900,'io')
line([(2732,4075),(2786,4075),(2786,5550),(2768,5550)],'⑤ 实际业务 I/O（非管理 RPC）',2040,5499,650,'io')
SVG.append('</svg>')
(ROOT/'ssf-full-deployment.svg').write_text('\n'.join(SVG)+'\n')

# Generate the same deployment containment as editable Mermaid.
children={}
for node in NODES: children.setdefault(node['parent'],[]).append(node)
out=['%%{init: {"theme":"base","themeVariables":{"fontFamily":"PingFang SC, Microsoft YaHei, sans-serif","fontSize":"18px","darkMode":true,"background":"#0f1822","textColor":"#c4d0dc","lineColor":"#8999aa"},"flowchart":{"htmlLabels":false,"curve":"linear"}}}%%','flowchart TB']
def emit(node,depth=1):
    pad='    '*depth
    text=node['title'].replace('"',"'").replace('\n','<br/>')
    if children.get(node['id']):
        out.append(f'{pad}subgraph {node["id"]}["{text}"]')
        for child in children[node['id']]: emit(child,depth+1)
        out.append(pad+'end')
    else:
        out.append(f'{pad}{node["id"]}["{text}"]')
for node in children[None]: emit(node)
out += [
    '    api_storage -.->|原生状态 watch| cube_resolver',
    '    api_storage -.->|原生状态 watch| agentenv_resolver',
    '    api_auth -.->|admission| guard',
    '    guard_ledger -.->|校验使用/排空许可| ssf_sql',
    '    provisioner -->|CSI UDS Create / Delete| c_service',
    '    attacher -->|native / VA Bridge only| c_service',
    '    snapshot_reconcile -.->|Snapshot API| snapshotter',
    '    snapshotter -->|CSI UDS Snapshot| c_service',
    '    resizer -->|CSI UDS Expand| c_service',
    '    cube_attach -->|mTLS Attach/Detach| gateway_policy',
    '    agentenv_attach -->|mTLS Attach/Detach| gateway_policy',
    '    gateway_uds -->|CSI UDS Publish/Unpublish| c_service',
    '    cube_use -->|transaction / operation| ssf_sql',
    '    agentenv_use -->|transaction / operation| ssf_sql',
    '    cube_delayed -.->|selected-node / static bind| api_storage',
    '    agentenv_delayed -.->|selected-node / static bind| api_storage',
    '    cube_placement -->|创建RPC携带NodeVolumePlan| cube_node_bridge',
    '    agentenv_scheduler_module -->|创建RPC携带NodeVolumePlan| agentenv_adapter',
    '    cube_node_bridge -->|Prepare / Release mTLS| node_api',
    '    agentenv_adapter -->|Prepare / Release mTLS| node_api',
    '    stage_manager -->|CSI UDS| n_service',
    '    registrar -->|GetPluginInfo| n_identity',
    '    registrar -->|registration UDS| plugin_manager',
    '    operation_executor -->|native Pod only| n_service',
    '    n_service --> stage_targets',
    '    cube_guest_mount ==>|volume I/O| data_backend',
    '    agentenv_guest_mount ==>|volume I/O| data_backend',
]
for tag,(fill,stroke,fg) in COLORS.items():
    out.append(f'    classDef origin{tag} fill:{fill},stroke:{stroke},color:{fg},stroke-width:1.5px;')
    leaves_for_tag=[n['id'] for n in NODES if n['origin']==tag and not children.get(n['id'])]
    if leaves_for_tag: out.append('    class '+','.join(leaves_for_tag)+f' origin{tag};')
for node in NODES:
    if children.get(node['id']):
        out.append(f'    style {node["id"]} fill:#111c27,stroke:#64798d,color:#c4d0dc')
    if node['optional']:
        out.append(f'    style {node["id"]} stroke:#8793a3,stroke-dasharray:8 5')
(ROOT/'ssf-full-deployment.mmd').write_text('\n'.join(out)+'\n')
print(f'Generated SVG + Mermaid: {len(NODES)} deployment elements, {WIDTH} × {HEIGHT}')
