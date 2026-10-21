#!/usr/bin/env python3
"""Render Guard/profile additions without contacting Kubernetes.

The supplied StorageClass JSON is copied under a new name. Vendor parameters
are preserved; the existing class is never patched.
"""
import argparse
import base64
import copy
import json
from pathlib import Path
import re


def render(source, name, mode, namespace, ssf_namespace, ca):
    if source.get('kind') != 'StorageClass' or not source.get('provisioner'):
        raise ValueError('input must be a native StorageClass JSON object')
    if source.get('metadata', {}).get('name') == name:
        raise ValueError('use a new dedicated StorageClass name')
    for value in (name, namespace, ssf_namespace):
        if len(value) > 63 or not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value):
            raise ValueError('invalid resource name/namespace')
    sc = copy.deepcopy(source)
    sc.pop('status', None)
    annotations = dict(sc['metadata'].get('annotations', {}))
    for key in ('storageclass.kubernetes.io/is-default-class',
                'storageclass.beta.kubernetes.io/is-default-class',
                'kubectl.kubernetes.io/last-applied-configuration'):
        annotations.pop(key, None)
    annotations['ssf.dev/volume-profile'] = 'agentenv-block-ext4-v1'
    sc['metadata'] = {'name': name, 'annotations': annotations}
    sc.update(apiVersion='storage.k8s.io/v1', volumeBindingMode=mode, reclaimPolicy='Retain')
    profile = dict(name='agentenv-block-ext4-v1', runtime='agentenv',
                   version='volume-preparation-v1', volumeMode='Block',
                   filesystem='ext4', initializeBlank=True)
    config = dict(Listen=':9443', Cert='/etc/ssf-tls/tls.crt', Key='/etc/ssf-tls/tls.key',
                  CA='/etc/ssf-tls/ca.crt', VolumeProfiles=[profile],
                  ManagedNamespaces=[namespace],
                  CoordinatorUsers=['system:serviceaccount:' + namespace + ':agentenv-scheduler'])
    webhook = dict(apiVersion='admissionregistration.k8s.io/v1',
                   kind='MutatingWebhookConfiguration', metadata={'name': 'ssf-volume-profile'},
                   webhooks=[dict(name='volume-profile.ssf.dev', admissionReviewVersions=['v1'],
                                  sideEffects='None', failurePolicy='Fail', matchPolicy='Equivalent',
                                  reinvocationPolicy='Never', timeoutSeconds=5,
                                  namespaceSelector={'matchLabels': {'kubernetes.io/metadata.name': namespace}},
                                  clientConfig={'service': {'namespace': ssf_namespace, 'name': 'ssf-guard',
                                                           'path': '/mutate', 'port': 443},
                                                'caBundle': base64.b64encode(ca).decode()},
                                  rules=[dict(operations=['CREATE'], apiGroups=[''], apiVersions=['v1'],
                                              resources=['persistentvolumeclaims'], scope='Namespaced')])])
    return {'apiVersion': 'v1', 'kind': 'List', 'items': [
        {'apiVersion': 'v1', 'kind': 'ConfigMap',
         'metadata': {'name': 'ssf-guard-config', 'namespace': ssf_namespace},
         'data': {'config.json': json.dumps(config, indent=2)}}, webhook, sc]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--storage-class-json', required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--binding-mode', choices=['Immediate', 'WaitForFirstConsumer'], required=True)
    p.add_argument('--namespace', default='agentenv-system')
    p.add_argument('--ssf-namespace', default='ssf-system')
    p.add_argument('--ca-file', required=True)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    ca = Path(args.ca_file).read_bytes()
    if b'-----BEGIN CERTIFICATE-----' not in ca:
        p.error('ca-file must contain the Guard CA certificate')
    manifest = render(json.loads(Path(args.storage_class_json).read_text()), args.name,
                      args.binding_mode, args.namespace, args.ssf_namespace, ca)
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2) + '\n')
    print(target)


if __name__ == '__main__':
    main()
