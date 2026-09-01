# KIMERA Phase 2 architecture

## Reconciliation and identity

The `DevelopmentEnvironment` controller remains the only domain controller. It validates the
specification, checks referenced ConfigMaps and Secrets, then converges PVC, ServiceAccount,
NetworkPolicy, Service, Deployment, and optional Ingress before observing readiness. Kubernetes
remains the state machine; transient API errors are returned, invalid input is terminal, and
normal provisioning is represented in status.

Identity is instance-scoped:

```text
DevelopmentEnvironment UID
  +-- DevelopmentEnvironmentIdentity -> workload selector labels
  +-- StorageIdentity                 -> workspace PVC name
```

The CR name is presentation and child-object naming, not workload or storage identity. A deleted
and recreated `demo` has a new UID, selector, and PVC. A retained old PVC is never inferred or
adopted by the new CR.

## Suspend and persistence

`spec.suspended=false` owns `Deployment.spec.replicas=1`; `true` owns replicas zero. Suspension
does not delete or recreate any child. Status reports `Phase=Suspended`, `Suspended=True`,
`Ready=False`, `Progressing=False`, and `WorkloadReady=Unknown` with reason
`EnvironmentSuspended`. On resume, the existing Deployment returns to one replica and mounts the
same PVC. Manual replica edits are drift and are corrected in either state.

Retention remains independent of suspension. `Retain` removes the PVC controller reference so
the claim survives CR deletion. `Delete` controller-owns and removes the PVC during finalization.
PVC shrink, StorageClass changes, incompatible access modes, and unsupported expansion retain the
Phase 1 terminal/degraded behavior. Only a supported expansion patches storage size.

## Git initialization

An optional `spec.source.git` defines creation-time, one-time workspace initialization and is
immutable for the lifetime of the `DevelopmentEnvironment`. It cannot be added, removed, or
changed after creation. It adds a pinned `alpine/git` init container. Phase 2 accepts only a
public, absolute HTTPS URL without credentials, query parameters, or fragments. Revision and
subpath are passed as environment variables to a static shell program; user input is never
interpolated into shell source or logged.

The init container mounts the same PVC as the IDE. It clones into an EmptyDir, resolves the
requested branch/tag/commit, then copies into an empty workspace and writes
`.kimera-initialized`. If the marker exists, or if pre-existing data is found, it leaves the
workspace unchanged. It never runs `git pull`. Clone failure prevents the IDE container from
starting; Kubernetes retries the init container and `SourceReady=False` reports
`SourceInitializationFailed`. A CR created suspended initializes only after its first resume.
Changing `spec.source` is rejected by CRD transition validation; destructive reinitialization and
source update/pull semantics are not part of Phase 2.

Git credentials are intentionally deferred. Putting credentials in a URL is rejected; no Secret
is mounted into either container for bootstrap authentication.

## Security boundary

Every IDE Pod names its own managed ServiceAccount. Token automount is false on both the
ServiceAccount and Pod, and KIMERA creates no Role or RoleBinding for it. The operator's identity
and permissions are never delegated to an IDE.

The Pod and containers run as non-root UID/GID 1000 with fsGroup 1000, RuntimeDefault seccomp,
privilege escalation disabled, and all Linux capabilities dropped. The Git init container also
uses a read-only root filesystem and a writable `/tmp` EmptyDir. The IDE root filesystem remains
writable because code-server writes runtime/configuration state below `/home/coder`; forcing it
read-only would make the supported image fail. The persistent writable workspace is mounted at
`/home/coder/project`.

The requested IDE image is user-controlled. UID 1000 and the code-server arguments are part of
the current workload contract; incompatible images fail rather than causing the controller to
weaken security. Image trust/admission is future platform policy.

Most importantly, code-server currently starts with `--auth none`. An Ingress therefore exposes
an unauthenticated code execution environment. Networking defaults off and must only be enabled
for trusted local demonstrations until an authentication gateway is implemented.

## NetworkPolicy and namespace model

The managed NetworkPolicy selects the exact UID-scoped Pod labels and applies ingress and egress
isolation. TCP 8080 ingress is accepted from Pods in the environment namespace and
`kube-system`, which supports same-trust-domain access and the usual k3d/K3s Traefik placement
without relying on fragile controller Pod labels. DNS TCP/UDP 53 is allowed to `kube-system` and
outbound TCP 443 is allowed for public Git and normal HTTPS access. Kubernetes NetworkPolicy
cannot allow egress by DNS name, so this is intentionally a protocol/port boundary, not a domain
allowlist. HTTPS egress can therefore also reach in-cluster endpoints on TCP 443; it does not
permit arbitrary egress on other ports. Environments requiring mutual distrust must be placed in
separate namespaces and combined with cluster-level network policy appropriate to that CNI.

NetworkPolicy enforcement is a CNI feature. Envtest proves object shape and ownership only, not
packet filtering. Verify the k3d/K3s CNI supports and enforces NetworkPolicy before treating this
as an isolation control.

The Kubernetes namespace is the Phase 2 tenancy boundary. Environments in the same namespace are
in one trust domain; KIMERA does not yet create namespaces or Project/Group resources. This avoids
unsafe cross-namespace owner references and premature namespace lifecycle policy.

ResourceQuota and LimitRange are namespace-scoped and cannot select a single environment by
label. Creating one named after every environment in a shared namespace would cause every quota
to apply to the namespace's aggregate usage, not to its apparent owner. Phase 2 therefore manages
neither. Explicit container requests/limits remain enforced, while namespace quota/default policy
belongs to the cluster administrator until a later Project/Group namespace controller defines
unambiguous ownership.

## Status, Events, metrics, and watches

User-facing conditions are `StorageReady`, `WorkloadReady`, `NetworkReady`, `SourceReady`,
`Suspended`, `Ready`, and `Progressing`. `ObservedGeneration` records the generation from which
status was computed. Kubernetes Events are emitted only for transitions such as PVC readiness,
suspend/resume, source success/failure, readiness, and terminal configuration/storage errors.

The metrics endpoint adds three aggregate, label-free gauges:

- `kimera_development_environments`
- `kimera_development_environments_ready`
- `kimera_development_environments_suspended`

Controller-runtime already supplies reconcile/error metrics, so KIMERA does not duplicate them.

Owned children and UID-selected Pods enqueue reconciliation. Referenced ConfigMaps and Secrets
are checked during reconciliation but are not indexed/watched in Phase 2; changing only a
reference object may require touching the CR or waiting for another reconciliation trigger.

## Non-goals

Phase 2 does not implement Git credentials or webhooks, IDE authentication, image admission,
Project/Group CRDs, namespace provisioning, ResourceQuota/LimitRange ownership, restore/adoption,
jobs or execution planes, a resource gateway, Vault, GPU abstractions, or multi-cluster placement.
