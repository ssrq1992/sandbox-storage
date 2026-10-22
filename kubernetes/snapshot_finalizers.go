package kubernetes

// Native external-snapshotter v8.2.0 protection finalizers. These retain object
// identity while a driver operation has an unknown result.
const SnapshotBoundFinalizer = "snapshot.storage.kubernetes.io/volumesnapshot-bound-protection"
const SnapshotContentBoundFinalizer = "snapshot.storage.kubernetes.io/volumesnapshotcontent-bound-protection"
