# Phase 2 k3d demo

These commands assume k3d exposes Traefik HTTP on host port 8080 and the operator repository is
the current directory. The sample IDE has no authentication; use a disposable local cluster.

## Install and create an environment

```sh
k3d cluster create kimera --agents 1 -p '8080:80@loadbalancer'
make install
make run
```

Leave `make run` active. In another terminal:

```sh
kubectl apply -f config/samples/platform_v1alpha1_developmentenvironment.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Ready developmentenvironment/demo --timeout=5m
kubectl get developmentenvironment demo -o wide
kubectl get deployment,pod,pvc,service,ingress,serviceaccount,networkpolicy -l platform.kimera.dev/development-environment=demo -o wide
curl -H 'Host: demo.kimera.local' http://127.0.0.1:8080/healthz
```

If no default StorageClass exists, set `spec.storage.storageClassName` first. NetworkPolicy packet
enforcement requires a capable CNI; the existence of the policy alone does not prove enforcement.

## Prove workspace persistence through suspend, Pod replacement, and rollout

```sh
POD="$(kubectl get pod -l platform.kimera.dev/development-environment=demo -o jsonpath='{.items[0].metadata.name}')"
kubectl exec "$POD" -- sh -c 'printf "%s\n" kimera-phase-2 > /home/coder/project/hashem'
kubectl exec "$POD" -- cat /home/coder/project/hashem

DEPLOYMENT_UID="$(kubectl get deployment demo -o jsonpath='{.metadata.uid}')"
PVC="$(kubectl get deployment demo -o jsonpath='{.spec.template.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')"
PVC_UID="$(kubectl get pvc "$PVC" -o jsonpath='{.metadata.uid}')"

kubectl patch developmentenvironment demo --type=merge -p '{"spec":{"suspended":true}}'
kubectl wait --for=jsonpath='{.status.phase}'=Suspended developmentenvironment/demo --timeout=2m
kubectl get deployment demo -o jsonpath='replicas={.spec.replicas}{"\n"}'
kubectl get pvc "$PVC"
test "$(kubectl get deployment demo -o jsonpath='{.metadata.uid}')" = "$DEPLOYMENT_UID"
test "$(kubectl get pvc "$PVC" -o jsonpath='{.metadata.uid}')" = "$PVC_UID"

kubectl scale deployment demo --replicas=1
sleep 3
kubectl get deployment demo -o jsonpath='operator-restored-replicas={.spec.replicas}{"\n"}'

kubectl patch developmentenvironment demo --type=merge -p '{"spec":{"suspended":false}}'
kubectl wait --for=jsonpath='{.status.phase}'=Ready developmentenvironment/demo --timeout=5m
POD="$(kubectl get pod -l platform.kimera.dev/development-environment=demo -o jsonpath='{.items[0].metadata.name}')"
kubectl exec "$POD" -- cat /home/coder/project/hashem

kubectl delete pod "$POD"
kubectl rollout status deployment/demo --timeout=5m
POD="$(kubectl get pod -l platform.kimera.dev/development-environment=demo -o jsonpath='{.items[0].metadata.name}')"
kubectl exec "$POD" -- cat /home/coder/project/hashem

kubectl patch developmentenvironment demo --type=merge -p '{"spec":{"resources":{"cpuRequest":"300m","cpuLimit":"1","memoryRequest":"512Mi","memoryLimit":"1Gi"}}}'
kubectl rollout status deployment/demo --timeout=5m
POD="$(kubectl get pod -l platform.kimera.dev/development-environment=demo -o jsonpath='{.items[0].metadata.name}')"
kubectl exec "$POD" -- cat /home/coder/project/hashem
```

The Git init container sees `.kimera-initialized` after every replacement and leaves the user file
unchanged.

## Inspect workload identity and isolation objects

```sh
ENV_UID="$(kubectl get developmentenvironment demo -o jsonpath='{.metadata.uid}')"
kubectl get deployment demo -o jsonpath='{.spec.selector.matchLabels}{"\n"}{.spec.template.spec.serviceAccountName}{"\n"}{.spec.template.spec.automountServiceAccountToken}{"\n"}'
kubectl get serviceaccount demo -o yaml
kubectl get rolebinding -o yaml | grep -F "$ENV_UID" && echo unexpected || echo 'no IDE RoleBinding'
kubectl get networkpolicy demo -o yaml
kubectl get pod -l "platform.kimera.dev/environment-uid=$ENV_UID" --show-labels
kubectl get pod -l 'platform.kimera.dev/environment-uid=definitely-not-this-uid' --no-headers
```

The last query must return no Pods. To test packet enforcement, first confirm the cluster CNI
implements NetworkPolicy, then attempt TCP 8080 and non-HTTPS egress from Pods inside and outside
the allowed namespace boundary; envtest cannot perform this proof.

## Prove Retain and new-UID isolation

```sh
OLD_ENV_UID="$(kubectl get developmentenvironment demo -o jsonpath='{.metadata.uid}')"
OLD_PVC="$PVC"
OLD_PVC_UID="$(kubectl get pvc "$OLD_PVC" -o jsonpath='{.metadata.uid}')"
kubectl delete developmentenvironment demo --wait=true
kubectl get pvc "$OLD_PVC"

kubectl apply -f config/samples/platform_v1alpha1_developmentenvironment.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Ready developmentenvironment/demo --timeout=5m
NEW_ENV_UID="$(kubectl get developmentenvironment demo -o jsonpath='{.metadata.uid}')"
NEW_PVC="$(kubectl get deployment demo -o jsonpath='{.spec.template.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')"
NEW_PVC_UID="$(kubectl get pvc "$NEW_PVC" -o jsonpath='{.metadata.uid}')"

test "$NEW_ENV_UID" != "$OLD_ENV_UID"
test "$NEW_PVC" != "$OLD_PVC"
test "$NEW_PVC_UID" != "$OLD_PVC_UID"
kubectl get pvc "$OLD_PVC" "$NEW_PVC" -o wide
```

The retained PVC is intentionally orphaned and is not mounted by the recreated CR.

To exercise `Delete` retention on the new instance:

```sh
kubectl patch developmentenvironment demo --type=merge -p '{"spec":{"storage":{"retentionPolicy":"Delete"}}}'
NEW_PVC="$(kubectl get deployment demo -o jsonpath='{.spec.template.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')"
kubectl delete developmentenvironment demo --wait=true
kubectl wait --for=delete "pvc/$NEW_PVC" --timeout=2m
kubectl get pvc "$OLD_PVC"
```

Finally stop `make run` and remove the disposable cluster:

```sh
k3d cluster delete kimera
```
