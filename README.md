# KIMERA Operator

KIMERA Operator Phase 1 manages a `DevelopmentEnvironment`: one persistent
workspace PVC, one IDE Deployment, a ClusterIP Service, and an optional Ingress.
It intentionally does not manage source control, authentication, or shared tenancy.
The sample uses `codercom/code-server:latest` with code-server authentication disabled for a
local learning demo. Production deployments should pin an explicit image version or digest and
use an appropriate authentication approach.

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
kubectl get deployment,pods,pvc,svc,ingress
kubectl get developmentenvironment demo -o yaml
```

## Demo

1. Start a local cluster with a default StorageClass, then run the workflow above. If your
   cluster has no default StorageClass, specify one under `spec.storage.storageClassName`.
2. Wait until `kubectl get developmentenvironment demo` reports `Ready`, then inspect the
   generated Deployment, Service, PVC, and Ingress.
3. Point `demo.kimera.local` at the local ingress address (for many local clusters this is
   `127.0.0.1`; add `127.0.0.1 demo.kimera.local` to your hosts file) and open
   `http://demo.kimera.local`.
4. Delete `kubectl delete deployment demo`; the owned-resource watch makes the operator recreate it.
5. Edit the sample CR's CPU request or image and apply it again; observe the Deployment roll out.
6. Delete the CR with `retentionPolicy: Retain` and verify `demo-workspace` remains.
7. Reapply it with `retentionPolicy: Delete`, then delete the CR and verify the PVC is removed.

Storage expansion is passed to Kubernetes only when requested size increases. PVC
shrinking is never attempted and is reported in the resource status as a failure.
