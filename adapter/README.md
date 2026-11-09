# Sandbox adapters

Project integrations live below this directory. They may import the public SSF packages; public SSF packages must not import an adapter. Each adapter owns its API, project client, placement, runtime evidence, deployment, and acceptance tests. Storage drivers remain separate from runtime adapters.

`sandboxschedadapter` integrates SandboxSched (containerd + runc sandboxes).