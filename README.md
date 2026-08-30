# KIMERA Operator

KIMERA Operator Phase 1 manages a `DevelopmentEnvironment`: one persistent
workspace PVC, one IDE Deployment, a ClusterIP Service, and an optional Ingress.
It intentionally does not manage source control, authentication, or shared tenancy.

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
```

## Demo

1. Apply the sample and wait for its PVC and Deployment to become ready.
2. Inspect the generated Deployment, Service, PVC, and Ingress.
3. Delete the generated Deployment; the operator recreates it through its child watch.
4. Change the CR's CPU request or image and observe the Deployment rolling update.
5. Delete a CR with `retentionPolicy: Retain`; the workspace PVC remains.
6. Repeat with `retentionPolicy: Delete`; the operator deletes the workspace PVC.

Storage expansion is passed to Kubernetes only when requested size increases. PVC
shrinking is never attempted and is reported in the resource status as a failure.
