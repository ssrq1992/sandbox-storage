#!/usr/bin/env python3
"""Render current native architecture SVG and editable Mermaid without dependencies."""
from pathlib import Path
from html import escape
root=Path(__file__).resolve().parents[4]/'design/figures'
root.mkdir(exist_ok=True)
colors={'common':'#24364a','adapter':'#44392c','backend':'#253a33'}
figures={
 'agentenvadapter-modules': ('AgentENV × SSF · 原生模块与权威边界',[
 ('原生请求与生命周期（保留）', [('common','Gateway：鉴权 / 路由 / HTTP・WS・流代理'),('common','Scheduler：发现 / 健康 / 策略 / 计算准入'),('common','AgentENV：SnapshotManager / Orchestrator / VMM')]),
 ('存储插件（定制）与公共消费能力', [('adapter','ControlPlugin：模板 pin / VolumeProviderMux / 生命周期命令'),('common','SSF Coordinator：拓扑 / WFFC / owner・run・epoch / Attach'),('adapter','NodePlugin：签名计划 / loopback hook / guest 与停止证据')]),
 ('原生卷管理与直接节点消费', [('common','Kubernetes：PVC / PV / Snapshot・Content / 原生 sidecars'),('common','SSF Node / WAL / Guard / PostgreSQL：授权・保护・恢复'),('adapter','厂商 CSI Controller・Node：管理 RPC / NodeStage・Publish')]),
 ('实际数据路径与持久性', [('common','Firecracker guest → virtio-block → host block（原 VMM 拥有）'),('backend','后端持久存储；业务 I/O 不经过控制插件'),('backend','暂停保留 UseHold；真实 stop → Unpublish → Unstage → Detach')])]),
 'agentenvadapter-deployment': ('AgentENV × SSF · 原生部署增量',[
 ('保留原工作负载', [('common','Gateway Deployment / 原路由与代理 Service'),('common','Scheduler Deployment + 内嵌 ControlPlugin'),('common','原 node DaemonSet / headless / 心跳 / P2P / 探针')]),
 ('原 node Pod 的增量 sidecar', [('common','原 AgentENV 容器与 Firecracker / guest'),('adapter','NodePlugin：loopback :9445 token；mTLS :9444'),('common','SSF Node：mTLS :9443 / WAL；先启动后退出')]),
 ('独立公共基础设施与原生 CSI', [('common','SSF Guard / PostgreSQL / Kubernetes 管理面'),('adapter','厂商 Controller + 官方 sidecars / 本机 Node UDS'),('backend','真实后端存储；无逐沙箱 HolderPod')]),
 ('共享宿主资源与 standalone', [('backend','host PID / dev / WAL；CSI・SSF 双向挂载传播'),('common','standalone：保留原 service，新增插件与 SSF Node'),('adapter','同一消费库 + 固定本机 placement，无替代沙箱控制面')])]),
 'agentenvadapter-lifecycle': ('AgentENV × SSF · 生命周期与数据屏障',[
 ('创建 / 提交', [('common','原 Gateway → 原 Scheduler：计算预留 + 卷解析 / 绑定'),('common','共享 Coordinator Attach → 完整签名计划 → Prepare'),('adapter','原 Orchestrator spawn → guest 设备验证 → 可信 Ready / Commit')]),
 ('暂停 / 恢复', [('common','冻结与原生暂停；UseHold 保留逻辑使用权'),('adapter','真实 stop → 释放本 run 资源；新 run 原子接续 epoch'),('common','保留原 ID、自动暂停 / connect 恢复与驱逐策略')]),
 ('快照 / Fork / 模板', [('adapter','native memory・rootfs + legacy + CSI 同屏障捕获'),('common','CheckpointSet / Snapshot・Content UID / 仓库 pin 联合发布'),('backend','RW 独立克隆；RO 按语义共享；替换隐藏旧盘并保留 FD')]),
 ('删除 / 恢复 / 回收', [('adapter','持久清理 outbox → 原 VMM 停止重试与真实证据'),('common','Unpublish → 最后引用 Unstage → Detach；未知结果不强删'),('backend','用户卷保留；内部克隆 / capture 仅在所有引用消失后回收')])])}
for name,(title,groups) in figures.items():
 out=['<svg xmlns="http://www.w3.org/2000/svg" width="1480" height="1180" viewBox="0 0 1480 1180">','<rect width="1480" height="1180" fill="#0f1822"/>','<g font-family="PingFang SC,Microsoft YaHei,sans-serif" fill="#c4d0dc">',f'<text x="32" y="48" font-size="28">{escape(title)}</text>','<text x="32" y="83" font-size="18">蓝：原生模块 / 公共框架　棕：运行时定制与厂商 Driver　绿：持久存储 / 数据边界</text>']
 mermaid=['flowchart TB','classDef common fill:#24364a,stroke:#617d9b,color:#c4d0dc','classDef adapter fill:#44392c,stroke:#aa9066,color:#c4d0dc','classDef backend fill:#253a33,stroke:#668b7d,color:#c4d0dc']
 previous=None
 for row,(label,boxes) in enumerate(groups):
  y=112+row*260
  out += [f'<rect x="24" y="{y}" width="1432" height="240" rx="8" fill="#111c27" stroke="#64798d"/>',f'<text x="44" y="{y+34}" font-size="23">{escape(label)}</text>']
  mermaid.append(f'subgraph G{row}["{label}"]')
  for col,(kind,text) in enumerate(boxes):
   yy=y+52+col*56
   out += [f'<rect x="44" y="{yy}" width="1392" height="46" rx="6" fill="{colors[kind]}" stroke="#64798d"/>',f'<text x="60" y="{yy+30}" font-size="20">{escape(text)}</text>']
   node=f'N{row}{col}';mermaid.append(f'{node}["{text}"]:::{kind}')
  mermaid.append('end')
  if previous and name.endswith('lifecycle'): mermaid.append(f'{previous} --> N{row}0')
  if name.endswith('lifecycle'): mermaid += [f'N{row}0 --> N{row}1 --> N{row}2']
  previous=f'N{row}2'
 out += ['</g></svg>']
 (root/(name+'.svg')).write_text('\n'.join(out)+'\n')
 (root/(name+'.mmd')).write_text('\n'.join(mermaid)+'\n')
