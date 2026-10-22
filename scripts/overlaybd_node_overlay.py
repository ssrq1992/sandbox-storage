"""Add OverlayBD processes to the original native-node overlay, without applying it."""
import copy
import json
from pathlib import Path
import re

DRIVER = 'overlaybd.csi.ssf.dev'
CONFIG = '/run/ssf-overlaybd-config/private'
SOCKET = '/var/lib/kubelet/plugins/' + DRIVER + '/csi.sock'
PROFILE = dict(name='agentenv-overlaybd-ext4-v1', runtime='agentenv',
               version='volume-preparation-v1', volumeMode='Block', filesystem='ext4',
               initializeBlank=False)


def build_inventory(source):
    if not isinstance(source.get('offline_expansion',False), bool):
        raise ValueError('offline_expansion must be a JSON boolean')
    nodes = {}
    repositories = {}
    definitions = {}
    for entry in source['nodes']:
        name, repository = entry['node'], entry['repository']
        if not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', name) or len(name) > 253:
            raise ValueError('invalid native node name')
        if name in nodes or not re.fullmatch(r'[A-Za-z0-9_-]+', repository):
            raise ValueError('duplicate node or invalid native repository identity')
        accessible = entry['accessible_nodes']
        if accessible not in ([], [name]):
            raise ValueError('local repository must name exactly its real native node')
        endpoint = entry['worker_endpoint']
        if not re.fullmatch(r'[A-Za-z0-9.-]+:9455', endpoint):
            raise ValueError('worker endpoint must be its certificate DNS/IP name:9455')
        backend = entry.get('repository_backend', 'posix_fs')
        if backend not in ('posix_fs', 'oss'):
            raise ValueError('unsupported native repository backend')
        native = dict(repository_id=repository, repository_backend=backend,
                      state_root='/var/lib/ssf/overlaybd/host', socket='/run/ssf-overlaybd/private/backend.sock',
                      node_id=name, daemon_socket='/run/ssf-overlaybd/private/daemon.sock',
                      overlaybd_config='/etc/ssf-overlaybd/overlaybd.json', accessible_nodes=accessible,
                      max_size_mb=entry.get('max_size_mb', 1048576), max_mounts=24,
                      request_timeout_seconds=300)
        if backend == 'oss':
            if accessible:
                raise ValueError('OSS inventory must describe a shared repository')
            native['oss'] = entry['oss']
        else:
            native['repository_root'] = '/var/lib/ssf-overlaybd-repository'
        tls = dict(certificate='/etc/ssf-overlaybd-tls/tls.crt', key='/etc/ssf-overlaybd-tls/tls.key',
                   ca='/etc/ssf-overlaybd-tls/ca.crt')
        node = dict(role='node', socket=SOCKET, backendSocket=native['socket'],
                    wal='/var/lib/ssf/overlaybd/node/sessions.db', node=name, csiNode=name,
                    root='/var/lib/ssf', planPublicKey='/etc/agentenv-storage/plan.pub',
                    evidenceURL='https://127.0.0.1:9444/v1/evidence',
                    captureEvidenceURL='https://127.0.0.1:9444/v1/capture/evidence', **tls)
        worker = dict(role='worker', listen='0.0.0.0:9455', workerRepository=repository,
                      backendSocket=native['socket'], peers=['spiffe://ssf/overlaybd-controller'], **tls)
        nodes[name] = dict(native=native, node=node, worker=worker)
        definition = {key: native.get(key) for key in ('repository_backend', 'repository_root', 'oss', 'accessible_nodes')}
        if repository in definitions and definitions[repository] != definition:
            raise ValueError('one repository ID cannot describe different backend storage')
        definitions[repository] = definition
        prior = repositories.get(repository)
        if prior and (prior['nodes'] or accessible):
            raise ValueError('local repositories cannot be shared by several inventory nodes')
        # Any shared worker can supply the repository; the first explicit
        # endpoint remains fixed, not a load-balanced local-directory alias.
        if not prior:
            repositories[repository] = dict(id=repository, endpoint=endpoint, nodes=accessible)
    if not nodes:
        raise ValueError('explicit worker inventory required')
    return dict(nodes=nodes), list(repositories.values())


def resources(source, namespace, image):
    inventory, repositories = build_inventory(source)
    source_volume = source.get('repository_volume')
    if any(n['native']['repository_backend'] == 'posix_fs' for n in inventory['nodes'].values()):
        if not isinstance(source_volume, dict) or len(source_volume) != 1 or not set(source_volume) <= {'hostPath', 'nfs', 'persistentVolumeClaim'}:
            raise ValueError('explicit original-compatible POSIX repository volume required')
        if 'hostPath' in source_volume and any(not n['native']['accessible_nodes'] for n in inventory['nodes'].values() if n['native']['repository_backend']=='posix_fs'):
            raise ValueError('shared POSIX needs a verified shared volume, not per-node hostPath')
    config = {'inventory.json': json.dumps(inventory, indent=2),
              'overlaybd.json': json.dumps(source['overlaybd_config'], indent=2),
              'resize.json': json.dumps(source['resize_config'], indent=2)}
    offline = source.get('offline_expansion',False)
    tools_volume = source.get('overlaybd_tools_volume')
    if offline:
        if not isinstance(tools_volume,dict) or len(tools_volume)!=1 or not set(tools_volume)<= {'hostPath','nfs','persistentVolumeClaim'}:
            raise ValueError('offline expansion requires the original verified OverlayBD tools volume')
        config['daemon.toml']='deps_path = "/opt/aenv-deps"\n'
    metadata = dict(name='ssf-overlaybd-node-config', namespace=namespace)
    objects = [dict(apiVersion='v1', kind='ConfigMap', metadata=metadata, data=config),
               dict(apiVersion='storage.k8s.io/v1', kind='CSIDriver', metadata=dict(name=DRIVER),
                    spec=dict(attachRequired=True, podInfoOnMount=False, fsGroupPolicy='None',
                              volumeLifecycleModes=['Persistent']))]
    mounts = [dict(name='overlaybd-config', mountPath='/etc/ssf-overlaybd', readOnly=True),
              dict(name='overlaybd-generated', mountPath='/run/ssf-overlaybd-config', readOnly=True),
              dict(name='overlaybd-sockets', mountPath='/run/ssf-overlaybd'),
              dict(name='ssf-state', mountPath='/var/lib/ssf', mountPropagation='Bidirectional'),
              dict(name='host-dev', mountPath='/dev'),
              dict(name='overlaybd-tls', mountPath='/etc/ssf-overlaybd-tls', readOnly=True),
              dict(name='overlaybd-plan-public', mountPath='/etc/agentenv-storage', readOnly=True)]
    native_mounts = copy.deepcopy([m for m in mounts if m['name'] not in ('overlaybd-tls', 'overlaybd-plan-public')])
    if source_volume:
        native_mounts.append(dict(name='overlaybd-repository', mountPath='/var/lib/ssf-overlaybd-repository'))
    if any(n['native']['repository_backend']=='oss' for n in inventory['nodes'].values()):
        native_mounts.append(dict(name='overlaybd-oss', mountPath='/etc/ssf-overlaybd/oss', readOnly=True))
    def sidecar(name, command, args, volume_mounts=None):
        return dict(name=name, image=image, restartPolicy='Always', command=[command], args=args,
                    securityContext=dict(privileged=True, runAsUser=0),
                    volumeMounts=copy.deepcopy(volume_mounts or mounts))
    bootstrap = dict(name='overlaybd-config', image=image, command=['overlaybd-config'],
                     args=['-inventory', '/etc/ssf-overlaybd/inventory.json'],
                     env=[dict(name='SSF_NODE_NAME', valueFrom=dict(fieldRef=dict(fieldPath='spec.nodeName')))],
                     securityContext=dict(runAsUser=0, allowPrivilegeEscalation=False),
                     volumeMounts=[dict(name='overlaybd-config', mountPath='/etc/ssf-overlaybd', readOnly=True),
                                   dict(name='overlaybd-generated', mountPath='/run/ssf-overlaybd-config')])
    daemon = sidecar('overlaybd-daemon', 'uvm-ublk-daemon', [
        '--socket-path', '/run/ssf-overlaybd/private/daemon.sock', '--global-config', '/etc/ssf-overlaybd/overlaybd.json',
        '--resize-global-config', '/etc/ssf-overlaybd/resize.json', '--managed-state-dir', '/var/lib/ssf/overlaybd/daemon',
        '--metrics-listen-addr', '0.0.0.0:9106'], native_mounts)
    if offline:
        daemon['args'] += ['--config','/etc/ssf-overlaybd/daemon.toml']
        daemon['volumeMounts'].append(dict(name='overlaybd-tools',mountPath='/opt/aenv-deps/overlaybd',readOnly=True))
    daemon['startupProbe'] = dict(exec=dict(command=['test', '-S', '/run/ssf-overlaybd/private/daemon.sock']), periodSeconds=2, failureThreshold=90)
    if offline:
        daemon['startupProbe']['exec']['command']=['/bin/sh','-c','test -S /run/ssf-overlaybd/private/daemon.sock && test -x /opt/aenv-deps/overlaybd/bin/overlaybd-resize && command -v e2fsck >/dev/null && command -v resize2fs >/dev/null']
    host = sidecar('overlaybd-native', 'ssf-overlaybd-native', [CONFIG+'/native.json'], native_mounts)
    host['startupProbe'] = dict(exec=dict(command=['overlaybd-csi', '-config', CONFIG+'/node.json', '-probe-backend']), periodSeconds=2, timeoutSeconds=6, failureThreshold=90)
    worker = sidecar('overlaybd-worker', 'overlaybd-csi', ['-config', CONFIG+'/worker.json'], [m for m in mounts if m['name'] in ('overlaybd-config', 'overlaybd-generated', 'overlaybd-sockets', 'overlaybd-tls')])
    worker['securityContext'] = dict(runAsUser=0, allowPrivilegeEscalation=False)
    worker['ports'] = [dict(name='overlaybd-worker', containerPort=9455, hostPort=9455)]
    worker['startupProbe'] = dict(tcpSocket=dict(port=9455), periodSeconds=2, failureThreshold=90)
    worker['readinessProbe'] = dict(tcpSocket=dict(port=9455), periodSeconds=10)
    driver = sidecar('overlaybd-csi', 'overlaybd-csi', ['-config', CONFIG+'/node.json'])
    driver['volumeMounts'].append(dict(name='csi-plugins', mountPath='/var/lib/kubelet/plugins'))
    probe = dict(exec=dict(command=['overlaybd-csi', '-config', CONFIG+'/node.json', '-probe']), periodSeconds=5, timeoutSeconds=6, failureThreshold=36)
    driver['startupProbe'], driver['readinessProbe'] = copy.deepcopy(probe), copy.deepcopy(probe)
    registrar = dict(name='overlaybd-registrar', image='registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0',
                     restartPolicy='Always', args=['--csi-address='+SOCKET, '--kubelet-registration-path='+SOCKET],
                     securityContext=dict(runAsUser=0, allowPrivilegeEscalation=False),
                     volumeMounts=[dict(name='csi-plugins', mountPath='/var/lib/kubelet/plugins'),
                                   dict(name='overlaybd-registration', mountPath='/registration')])
    volumes = [dict(name='overlaybd-config', configMap=dict(name=metadata['name'])),
               dict(name='overlaybd-generated', emptyDir={}), dict(name='overlaybd-sockets', emptyDir={}),
               dict(name='overlaybd-tls', secret=dict(secretName='ssf-overlaybd-node-tls', defaultMode=256)),
               dict(name='overlaybd-plan-public', secret=dict(secretName='agentenv-storage-node', defaultMode=256, items=[dict(key='plan.pub', path='plan.pub')])),
               dict(name='overlaybd-registration', hostPath=dict(path='/var/lib/kubelet/plugins_registry', type='Directory'))]
    if offline:
        volumes.append(dict(name='overlaybd-tools', **tools_volume))
    if source_volume:
        volumes.append(dict(name='overlaybd-repository', **source_volume))
    if any(n['native']['repository_backend']=='oss' for n in inventory['nodes'].values()):
        volumes.append(dict(name='overlaybd-oss', secret=dict(secretName='ssf-overlaybd-oss', defaultMode=256)))
    patches = []
    # JSON patch prepend preserves every original initContainer and workload.
    for container in reversed([bootstrap, daemon, host, worker, driver, registrar]):
        patches.append(dict(op='add', path='/spec/template/spec/initContainers/0', value=container))
    for volume in volumes:
        patches.append(dict(op='add', path='/spec/template/spec/volumes/-', value=volume))
    return objects, patches, repositories


def install(overlay, source, namespace, image):
    objects, patches, repositories = resources(source, namespace, image)
    (overlay/'overlaybd-resources.json').write_text(json.dumps(dict(apiVersion='v1', kind='List', items=objects), indent=2)+'\n')
    (overlay/'overlaybd-node-patch.json').write_text(json.dumps(patches, indent=2)+'\n')
    kustomization = overlay/'kustomization.yaml'
    text = kustomization.read_text().replace('resources:\n', 'resources:\n  - overlaybd-resources.json\n', 1)
    text = text.replace('patches:\n', 'patches:\n  - path: overlaybd-node-patch.json\n    target:\n      group: apps\n      version: v1\n      kind: DaemonSet\n      name: agentenv-node\n', 1)
    # Apply after the SSF patch creates initContainers and volumes.
    block = '  - path: overlaybd-node-patch.json\n    target:\n      group: apps\n      version: v1\n      kind: DaemonSet\n      name: agentenv-node\n'
    text = text.replace(block, '').replace('  - path: node.yaml\n', '  - path: node.yaml\n'+block)
    kustomization.write_text(text)
    path=overlay/'config/node.json'
    node=json.loads(path.read_text())
    if 'spiffe://ssf/overlaybd-node' not in node['evidence_peers']:
        node['evidence_peers'].append('spiffe://ssf/overlaybd-node')
    path.write_text(json.dumps(node, indent=2)+'\n')
    path=overlay/'config/control.json'
    control=json.loads(path.read_text())
    control['volume_profiles'].append(dict(PROFILE,controllerOfflineExpansion=True) if source.get('offline_expansion',False) else PROFILE)
    path.write_text(json.dumps(control, indent=2)+'\n')
    path=overlay/'config/coordinator.json'
    coordinator=json.loads(path.read_text())
    coordinator['storage_peers']=['spiffe://ssf/overlaybd-controller']
    path.write_text(json.dumps(coordinator, indent=2)+'\n')
    return repositories
