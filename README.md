# KIMERA Operator

KIMERA Phase 2 manages a persistent browser IDE as a Kubernetes
`DevelopmentEnvironment`. Each resource receives a UID-scoped Deployment and Pod selector, a
UID-derived workspace PVC, a ClusterIP Service, an optional Ingress, an unprivileged
ServiceAccount, and a NetworkPolicy. It can initialize a new workspace from a public HTTPS Git
repository and suspend compute without replacing storage or network objects.

This is a production-minded learning milestone, not a complete public multi-tenant platform.
The IDE still runs code-server with `--auth none`; enable Ingress only on a trusted local/demo
network until an authentication gateway exists. The CR also accepts an arbitrary image, so a
real platform needs an image admission policy. See [Phase 2 architecture](docs/phase2-architecture.md)
for the security and namespace boundaries.

## Requirements

Go, Docker (or another container runtime), `kubectl`, Kubebuilder, and a local
Kubernetes cluster such as kind or k3d are required.

## Development workflow

```sh
make generate
make manifests
make install
make run
kubectl apply -f config/samples/platform_v1alpha1_developmentenvironment.yaml
```

Inspect the environment with:

```sh
kubectl get developmentenvironments
kubectl get deployment,pods,pvc,svc,ingress,serviceaccount,networkpolicy
kubectl get developmentenvironment demo -o yaml
```

## Phase 2 behavior

- `spec.suspended: false` means one IDE replica. `true` patches that same Deployment to zero;
  the PVC, Service, Ingress, ServiceAccount, selectors, and CR identity remain unchanged.
- `spec.source.git` performs one non-destructive clone on the first Pod start. The persistent
  `.kimera-initialized` marker prevents cloning or pulling on restarts and resume. Existing
  workspace contents are never overwritten. Source is immutable after CR creation.
- The IDE uses a dedicated ServiceAccount with token automount disabled and receives no RBAC.
- The NetworkPolicy selects the exact CR UID. It permits IDE HTTP ingress from the environment
  namespace and `kube-system`, DNS to `kube-system`, and outbound HTTPS. Enforcement requires a
  NetworkPolicy-capable CNI.
- Namespace is the current security/tenancy boundary. Multiple environments in one namespace
  are in the same trust domain. ResourceQuota and LimitRange are deliberately not synthesized
  per environment because Kubernetes applies both to an entire namespace.

The complete persistence, suspend/resume, self-healing, security-object, UID-isolation, and
retention demonstration is in [the Phase 2 k3d demo](docs/phase2-demo.md).

Storage expansion is passed to Kubernetes only for a Bound PVC whose actual StorageClass
explicitly enables expansion. Unsupported expansion is reported as `Degraded` with
`StorageReady=False`; shrinking and changing an existing PVC's StorageClass are reported as
terminal `Failed` states without repeatedly sending an API write that Kubernetes will reject.
Restoring the requested size to the PVC's current size lets reconciliation return to `Ready`.

The controller reads StorageClasses directly for expansion capability checks but does not watch
them cluster-wide. If an administrator later enables expansion, update the
DevelopmentEnvironment spec to trigger re-evaluation.

## Workspace identity and retention

Workspace PVC names include the immutable `DevelopmentEnvironment` UID. Reconciliation of the
same CR instance therefore uses one stable claim, while deleting and recreating a CR with the
same name produces a different claim. `Retain` preserves the old claim and its provenance; it
does not authorize a future same-name CR to mount that data.

Inspect retained storage with:

```sh
kubectl get pvc -l platform.kimera.dev/managed-by=kimera --show-labels
```

Explicit restoration is future control-plane work. A restore flow must intentionally reference
the retained storage identity and verify KIMERA provenance, compatibility, attachment state, and
the authenticated caller's authorization. Phase 2 does not infer business ownership from a
Kubernetes resource name and never adopts retained storage implicitly.
