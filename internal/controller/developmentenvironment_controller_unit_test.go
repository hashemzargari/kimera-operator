package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	"github.com/hashemzargari/kimera-operator/internal/resources"
	statusutil "github.com/hashemzargari/kimera-operator/internal/status"
)

const (
	unitNamespace     = "default"
	unitRetainPolicy  = "Retain"
	assignedClusterIP = "10.0.0.42"
)

func TestReconcileMissingParentIsTerminal(t *testing.T) {
	reconciler, _ := unitReconciler(t)
	result, err := reconciler.Reconcile(context.Background(), request("does-not-exist"))
	if err != nil || result.RequeueAfter > 0 {
		t.Fatalf("missing parent should be terminal: result=%+v err=%v", result, err)
	}
}

func TestReconcileDeploymentConflictIsReturnedAndRetrySucceeds(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("conflict")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	} // finalizer
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	} // children

	current := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Image = "codercom/code-server:changed"
	if err := base.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}

	reconciler.Client = &conflictOnceClient{Client: base, conflictDeploymentPatch: true}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); !apierrors.IsConflict(err) {
		t.Fatalf("expected retryable Deployment conflict, got %v", err)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase == platformv1alpha1.PhaseFailed {
		t.Fatal("transient conflict persisted PhaseFailed")
	}
	reconciler.Client = base
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatalf("retry after conflict failed: %v", err)
	}
	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), deployment); err != nil {
		t.Fatal(err)
	}
	if got := deployment.Spec.Template.Spec.Containers[0].Image; got != "codercom/code-server:changed" {
		t.Fatalf("image after retry = %q", got)
	}
}

func TestReconcileNotReadyChildrenRemainProvisioning(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("provisioning")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	actual := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), actual); err != nil {
		t.Fatal(err)
	}
	if actual.Status.Phase != platformv1alpha1.PhaseProvisioning {
		t.Fatalf("phase = %q, want Provisioning", actual.Status.Phase)
	}
	if actual.Status.ObservedGeneration != actual.Generation {
		t.Fatalf("observed generation = %d, generation = %d", actual.Status.ObservedGeneration, actual.Generation)
	}
}

func TestDeletionIgnoresAlreadyDeletedChildren(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("deleting")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), env); err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.reconcileDelete(context.Background(), env)
	if err != nil || result.RequeueAfter > 0 {
		t.Fatalf("cleanup with absent children should succeed: result=%+v err=%v", result, err)
	}
}

func TestNoOpReconcileDoesNotPatchManagedChildren(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("no-op")
	env.Spec.Network = platformv1alpha1.NetworkSpec{Enabled: true, Host: "no-op.example.com"}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if tracker.deploymentPatches != 0 || tracker.servicePatches != 0 || tracker.ingressPatches != 0 || tracker.pvcPatches != 0 {
		t.Fatalf("no-op reconciliation patched children: Deployment=%d Service=%d Ingress=%d PVC=%d", tracker.deploymentPatches, tracker.servicePatches, tracker.ingressPatches, tracker.pvcPatches)
	}
}

func TestOwnedChildDriftIsCorrectedOnceThenConverges(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("drift")
	env.Spec.Network = platformv1alpha1.NetworkSpec{Enabled: true, Host: "drift.example.com"}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(env)
	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), key, deployment); err != nil {
		t.Fatal(err)
	}
	zero := int32(0)
	deployment.Spec.Replicas = &zero
	if err := base.Update(context.Background(), deployment); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{}
	if err := base.Get(context.Background(), key, service); err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].Port = 81
	service.Spec.Selector[naming.EnvironmentUIDLabel] = "wrong-uid"
	if err := base.Update(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	ingress := &networkingv1.Ingress{}
	if err := base.Get(context.Background(), key, ingress); err != nil {
		t.Fatal(err)
	}
	ingress.Spec.Rules[0].Host = "drifted.example.com"
	ingressClassName := "externally-configured-class"
	ingress.Spec.IngressClassName = &ingressClassName
	ingress.Annotations = map[string]string{"external.example/kept": "true"}
	if err := base.Update(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}

	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if tracker.deploymentPatches != 1 || tracker.servicePatches != 1 || tracker.ingressPatches != 1 {
		t.Fatalf("corrective patches: Deployment=%d Service=%d Ingress=%d, want one each", tracker.deploymentPatches, tracker.servicePatches, tracker.ingressPatches)
	}
	if err := base.Get(context.Background(), key, deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want 1", deployment.Spec.Replicas)
	}
	if err := base.Get(context.Background(), key, service); err != nil {
		t.Fatal(err)
	}
	if service.Spec.Ports[0].Port != 80 {
		t.Fatalf("Service port = %d, want 80", service.Spec.Ports[0].Port)
	}
	if !reflect.DeepEqual(service.Spec.Selector, naming.SelectorLabels(env)) {
		t.Fatalf("Service selector = %#v, want %#v", service.Spec.Selector, naming.SelectorLabels(env))
	}
	if err := base.Get(context.Background(), key, ingress); err != nil {
		t.Fatal(err)
	}
	if ingress.Spec.Rules[0].Host != env.Spec.Network.Host {
		t.Fatalf("Ingress host = %q, want %q", ingress.Spec.Rules[0].Host, env.Spec.Network.Host)
	}
	assertIngressPreservedAndRouted(t, ingress, env, ingressClassName)
	tracker.deploymentPatches, tracker.servicePatches, tracker.ingressPatches = 0, 0, 0
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if tracker.deploymentPatches != 0 || tracker.servicePatches != 0 || tracker.ingressPatches != 0 {
		t.Fatalf("post-correction reconcile patched children: Deployment=%d Service=%d Ingress=%d", tracker.deploymentPatches, tracker.servicePatches, tracker.ingressPatches)
	}
}

func TestDeploymentDeletionRecreatesAndReadinessRecovers(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("self-heal")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	markChildrenReady(t, base, env.Name)
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseReady)

	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), deployment); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(context.Background(), deployment); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), deployment); err != nil {
		t.Fatalf("Deployment was not recreated: %v", err)
	}
	if !reflect.DeepEqual(deployment.Spec.Selector.MatchLabels, naming.SelectorLabels(env)) {
		t.Fatalf("recreated Deployment selector = %#v, want %#v", deployment.Spec.Selector.MatchLabels, naming.SelectorLabels(env))
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseDegraded)
	deployment.Status.AvailableReplicas = 1
	if err := base.Status().Update(context.Background(), deployment); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseReady)
}

func TestControlledDeploymentWithDifferentSelectorIsRecreatedAndPreservesPVC(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("selector-replacement")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	existingPVC := resources.DesiredPVC(env)
	existingPVC.UID = types.UID("existing-workspace-pvc")
	if err := base.Create(context.Background(), existingPVC); err != nil {
		t.Fatal(err)
	}
	differentSelector := map[string]string{"external.example/workload": "previous-state"}
	deploymentWithDifferentSelector := resources.DesiredDeployment(env)
	deploymentWithDifferentSelector.Spec.Selector = &metav1.LabelSelector{MatchLabels: differentSelector}
	deploymentWithDifferentSelector.Spec.Template.Labels = differentSelector
	if err := controllerutil.SetControllerReference(env, deploymentWithDifferentSelector, reconciler.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(context.Background(), deploymentWithDifferentSelector); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), deployment); err != nil {
		t.Fatal(err)
	}
	wantSelector := &metav1.LabelSelector{MatchLabels: naming.SelectorLabels(env)}
	if !equality.Semantic.DeepEqual(deployment.Spec.Selector, wantSelector) {
		t.Fatalf("recreated selector = %#v, want %#v", deployment.Spec.Selector, wantSelector)
	}
	if !metav1.IsControlledBy(deployment, env) {
		t.Fatal("recreated Deployment is not controlled by the current environment")
	}
	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := client.ObjectKey{Namespace: env.Namespace, Name: naming.PVC(env)}
	if err := base.Get(context.Background(), pvcKey, pvc); err != nil {
		t.Fatalf("UID-derived PVC was not preserved: %v", err)
	}
	if pvc.UID != existingPVC.UID {
		t.Fatalf("PVC UID after Deployment replacement = %q, want original %q", pvc.UID, existingPVC.UID)
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := base.List(context.Background(), claims, client.InNamespace(env.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(claims.Items) != 1 || claims.Items[0].Name != pvcKey.Name {
		t.Fatalf("PVCs after Deployment replacement = %#v, want only %q", claims.Items, pvcKey.Name)
	}
}

func TestUnrelatedSameNameDeploymentIsNotAdoptedOrReplaced(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("deployment-collision")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	unrelated := resources.DesiredDeployment(env)
	unrelated.Labels = map[string]string{"external.example/owner": "someone-else"}
	unrelatedSelector := map[string]string{"external.example/workload": "unrelated"}
	unrelated.Spec.Selector = &metav1.LabelSelector{MatchLabels: unrelatedSelector}
	unrelated.Spec.Template.Labels = unrelatedSelector
	if err := base.Create(context.Background(), unrelated); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err == nil || !strings.Contains(err.Error(), "refusing adoption or replacement") {
		t.Fatalf("expected safe Deployment collision error, got %v", err)
	}
	after := &appsv1.Deployment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), after); err != nil {
		t.Fatal(err)
	}
	if len(after.OwnerReferences) != 0 || after.Labels["external.example/owner"] != "someone-else" || !reflect.DeepEqual(after.Spec.Selector, unrelated.Spec.Selector) {
		t.Fatal("unrelated same-name Deployment was mutated or adopted")
	}
}

func TestMissingEnvironmentUIDFailsBeforeCreatingChildren(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("missing-uid")
	env.UID = ""
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
		t.Fatalf("missing UID should be a terminal lifecycle error: result=%+v err=%v", result, err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
	for _, object := range []client.Object{&appsv1.Deployment{}, &corev1.Service{}} {
		list := object.DeepCopyObject().(client.Object)
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), list); !apierrors.IsNotFound(err) {
			t.Fatalf("%T exists despite missing environment UID: %v", object, err)
		}
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := base.List(context.Background(), claims, client.InNamespace(env.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(claims.Items) != 0 {
		t.Fatalf("PVCs exist despite missing environment UID: %#v", claims.Items)
	}
}

func TestParentDisappearingDuringStatusPatchIsTerminal(t *testing.T) {
	reconciler, _ := unitReconciler(t)
	env := unitEnvironment("status-race")
	before := env.DeepCopy()
	env.Status.Phase = platformv1alpha1.PhaseProvisioning
	reconciler.Client = &parentNotFoundClient{Client: reconciler.Client, failStatusPatch: true}
	if err := reconciler.writeStatus(context.Background(), before, env); err != nil {
		t.Fatalf("parent NotFound from status Patch should be terminal: %v", err)
	}
}

func TestParentDisappearingDuringFinalizerPatchIsTerminal(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("finalizer-race")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	reconciler.Client = &parentNotFoundClient{Client: base, failParentPatch: true}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatalf("parent NotFound from finalizer Patch should be terminal: %v", err)
	}
}

func TestParentDisappearingDuringFinalizerRemovalIsTerminal(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("finalizer-removal-race")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), env); err != nil {
		t.Fatal(err)
	}
	reconciler.Client = &parentNotFoundClient{Client: base, failParentPatch: true}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatalf("parent NotFound from finalizer removal should be terminal: %v", err)
	}
}

func TestUnchangedStatusDoesNotPatch(t *testing.T) {
	reconciler, base := unitReconciler(t)
	tracker := &statusPatchCountingClient{Client: base}
	reconciler.Client = tracker
	env := unitEnvironment("unchanged-status")
	if err := reconciler.writeStatus(context.Background(), env.DeepCopy(), env); err != nil {
		t.Fatal(err)
	}
	if tracker.statusPatches != 0 {
		t.Fatalf("unchanged status patched %d times", tracker.statusPatches)
	}
}

func TestRetentionPoliciesDuringDeletion(t *testing.T) {
	for _, test := range []struct {
		name, policy string
		pvcRemains   bool
	}{{"retain-delete", unitRetainPolicy, true}, {"delete-delete", "Delete", false}} {
		t.Run(test.policy, func(t *testing.T) {
			reconciler, base := unitReconciler(t)
			env := unitEnvironment(test.name)
			env.Spec.Storage.RetentionPolicy = test.policy
			if err := base.Create(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
				t.Fatal(err)
			}
			if err := base.Delete(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
				t.Fatal(err)
			}
			pvc := &corev1.PersistentVolumeClaim{}
			err := base.Get(context.Background(), client.ObjectKey{Name: naming.PVC(env), Namespace: unitNamespace}, pvc)
			if test.pvcRemains && err != nil {
				t.Fatalf("retained PVC missing: %v", err)
			}
			if !test.pvcRemains && !apierrors.IsNotFound(err) {
				t.Fatalf("Delete-policy PVC still exists: %v", err)
			}
			if test.pvcRemains && metav1.IsControlledBy(pvc, env) {
				t.Fatal("retained PVC still has parent controller reference")
			}
			if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
				t.Fatalf("stale post-deletion event was not terminal: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestSameEnvironmentIdentityUsesOneStablePVC(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("stable-storage")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
			t.Fatal(err)
		}
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := base.List(context.Background(), claims, client.InNamespace(env.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(claims.Items) != 1 {
		t.Fatalf("PVC count = %d, want 1", len(claims.Items))
	}
	if claims.Items[0].Name != naming.PVC(env) {
		t.Fatalf("PVC = %q, want %q", claims.Items[0].Name, naming.PVC(env))
	}
}

func TestRecreatedSameNameEnvironmentDoesNotAdoptRetainedPVC(t *testing.T) {
	reconciler, base := unitReconciler(t)
	first := unitEnvironment("demo")
	first.UID = types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	if err := base.Create(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(first.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(first.Name)); err != nil {
		t.Fatal(err)
	}
	firstPVC := naming.PVC(first)
	if err := base.Delete(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(first.Name)); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), client.ObjectKey{Name: firstPVC, Namespace: first.Namespace}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("retained PVC A missing: %v", err)
	}

	second := unitEnvironment("demo")
	second.UID = types.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	if err := base.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(second.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(second.Name)); err != nil {
		t.Fatal(err)
	}
	secondPVC := naming.PVC(second)
	if firstPVC == secondPVC {
		t.Fatalf("different CR UIDs resolved to the same PVC %q", firstPVC)
	}
	if err := base.Get(context.Background(), client.ObjectKey{Name: secondPVC, Namespace: second.Namespace}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("PVC B missing: %v", err)
	}
	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(second), deployment); err != nil {
		t.Fatal(err)
	}
	if got := deployment.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != secondPVC {
		t.Fatalf("new environment mounted %q, want %q", got, secondPVC)
	}
}

func TestPVCWithCollidingGeneratedNameButWrongProvenanceIsRejected(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("provenance-collision")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	orphan := resources.DesiredPVC(env)
	orphan.Labels = map[string]string{}
	orphan.Annotations = map[string]string{}
	if err := base.Create(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Deployment created despite unsafe PVC collision: %v", err)
	}
}

func TestCurrentIdentityPVCWithIncompatibleStorageClassIsRejected(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("incompatible-pvc")
	env.Finalizers = []string{finalizer}
	env.Spec.Storage.StorageClassName = "requested-class"
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	claim := resources.DesiredPVC(env)
	actualClass := "other-class"
	claim.Spec.StorageClassName = &actualClass
	if err := base.Create(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
}

func TestCurrentIdentityPVCWithIncompatibleAccessModeIsRejected(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("incompatible-access-mode")
	env.Finalizers = []string{finalizer}
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	claim := resources.DesiredPVC(env)
	claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	if err := base.Create(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
}

func TestSupportedPVCExpansionPatchesOnce(t *testing.T) {
	reconciler, base, env := readyEnvironmentWithStorageClass(t, "supported-expansion", true)
	updateStorageSize(t, base, env.Name, "3Gi")
	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if tracker.pvcPatches != 1 {
		t.Fatalf("PVC patches = %d, want 1", tracker.pvcPatches)
	}
	pvc := getPVC(t, base, env.Name)
	requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if requested.Cmp(resource.MustParse("3Gi")) != 0 {
		t.Fatalf("PVC request = %s, want 3Gi", requested.String())
	}
}

func TestUnsupportedPVCExpansionIsTerminalAndRecoversAfterSpecRestore(t *testing.T) {
	reconciler, base, env := readyEnvironmentWithStorageClass(t, "unsupported-expansion", false)
	updateStorageSize(t, base, env.Name, "3Gi")
	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
		t.Fatalf("unsupported expansion should be terminal: result=%+v err=%v", result, err)
	}
	if tracker.pvcPatches != 0 {
		t.Fatalf("unsupported expansion patched PVC %d times", tracker.pvcPatches)
	}
	if tracker.statusPatches != 1 {
		t.Fatalf("initial unsupported expansion status patches = %d, want 1", tracker.statusPatches)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseDegraded)
	assertCondition(t, base, env.Name, statusutil.StorageReady, metav1.ConditionFalse, "VolumeExpansionUnsupported")
	assertCondition(t, base, env.Name, statusutil.Ready, metav1.ConditionFalse, "VolumeExpansionUnsupported")
	assertCondition(t, base, env.Name, statusutil.Progressing, metav1.ConditionFalse, "VolumeExpansionUnsupported")
	current := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ObservedGeneration != current.Generation {
		t.Fatalf("observedGeneration = %d, generation = %d", current.Status.ObservedGeneration, current.Generation)
	}
	if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
		t.Fatalf("repeated unsupported expansion should remain terminal: result=%+v err=%v", result, err)
	}
	if tracker.pvcPatches != 0 {
		t.Fatalf("repeated unsupported expansion patched PVC %d times", tracker.pvcPatches)
	}
	if tracker.statusPatches != 1 {
		t.Fatalf("repeated unsupported expansion wrote status again: patches=%d", tracker.statusPatches)
	}

	updateStorageSize(t, base, env.Name, "2Gi")
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseReady)
	assertCondition(t, base, env.Name, statusutil.StorageReady, metav1.ConditionTrue, "PVCObserved")
}

func TestPVCShrinkIsTerminalWithoutPatch(t *testing.T) {
	reconciler, base, env := readyEnvironmentWithStorageClass(t, "shrink", true)
	pvc := getPVC(t, base, env.Name)
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("3Gi")
	if err := base.Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
		t.Fatalf("shrink should be terminal: result=%+v err=%v", result, err)
	}
	if tracker.pvcPatches != 0 {
		t.Fatalf("shrink attempted %d PVC patches", tracker.pvcPatches)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
	assertCondition(t, base, env.Name, statusutil.StorageReady, metav1.ConditionFalse, "VolumeShrinkUnsupported")
}

func TestPVCStorageClassChangeIsTerminalWithoutPatch(t *testing.T) {
	reconciler, base, env := readyEnvironmentWithStorageClass(t, "storage-class-change", true)
	current := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Storage.StorageClassName = "another-class"
	current.Generation++
	if err := base.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	tracker := &childPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.RequeueAfter > 0 {
		t.Fatalf("StorageClass change should be terminal: result=%+v err=%v", result, err)
	}
	if tracker.pvcPatches != 0 {
		t.Fatalf("StorageClass change attempted %d PVC patches", tracker.pvcPatches)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseFailed)
	assertCondition(t, base, env.Name, statusutil.StorageReady, metav1.ConditionFalse, "StorageClassImmutable")
}

func TestPVCExpansionConflictIsReturnedForRetry(t *testing.T) {
	reconciler, base, env := readyEnvironmentWithStorageClass(t, "expansion-conflict", true)
	updateStorageSize(t, base, env.Name, "3Gi")
	reconciler.Client = &conflictOnceClient{Client: base, conflictPVCPatch: true}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); !apierrors.IsConflict(err) {
		t.Fatalf("expected retryable PVC conflict, got %v", err)
	}
	current := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ObservedGeneration == current.Generation {
		t.Fatal("transient PVC conflict was incorrectly recorded as an observed terminal state")
	}
}

func TestServiceAssignedFieldsAndDeploymentSelectorArePreserved(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("preserve-fields")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{}
	key := client.ObjectKeyFromObject(env)
	if err := base.Get(context.Background(), key, service); err != nil {
		t.Fatal(err)
	}
	policy := corev1.IPFamilyPolicySingleStack
	service.Spec.ClusterIP = assignedClusterIP
	service.Spec.ClusterIPs = []string{assignedClusterIP}
	service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	service.Spec.IPFamilyPolicy = &policy
	if err := base.Update(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := base.Get(context.Background(), key, deployment); err != nil {
		t.Fatal(err)
	}
	selector := deployment.Spec.Selector.DeepCopy()
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), key, service); err != nil {
		t.Fatal(err)
	}
	if service.Spec.ClusterIP != assignedClusterIP || len(service.Spec.ClusterIPs) != 1 || service.Spec.IPFamilyPolicy == nil || *service.Spec.IPFamilyPolicy != policy {
		t.Fatal("Service API-assigned fields were overwritten")
	}
	if err := base.Get(context.Background(), key, deployment); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(selector, deployment.Spec.Selector) {
		t.Fatal("immutable Deployment selector changed")
	}
}

type conflictOnceClient struct {
	client.Client
	conflictDeploymentPatch bool
	conflictPVCPatch        bool
}

func (c *conflictOnceClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if c.conflictDeploymentPatch {
		if _, ok := object.(*appsv1.Deployment); ok {
			c.conflictDeploymentPatch = false
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("simulated conflict"))
		}
	}
	if c.conflictPVCPatch {
		if _, ok := object.(*corev1.PersistentVolumeClaim); ok {
			c.conflictPVCPatch = false
			return apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, object.GetName(), errors.New("simulated conflict"))
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

type childPatchCountingClient struct {
	client.Client
	deploymentPatches int
	servicePatches    int
	ingressPatches    int
	pvcPatches        int
	statusPatches     int
}

func (c *childPatchCountingClient) Status() client.SubResourceWriter {
	return &statusPatchCountingWriter{SubResourceWriter: c.Client.Status(), count: &c.statusPatches}
}

func (c *childPatchCountingClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	switch object.(type) {
	case *appsv1.Deployment:
		c.deploymentPatches++
	case *corev1.Service:
		c.servicePatches++
	case *networkingv1.Ingress:
		c.ingressPatches++
	case *corev1.PersistentVolumeClaim:
		c.pvcPatches++
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

type parentNotFoundClient struct {
	client.Client
	failParentPatch, failStatusPatch bool
}

func (c *parentNotFoundClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if c.failParentPatch {
		if _, ok := object.(*platformv1alpha1.DevelopmentEnvironment); ok {
			return apierrors.NewNotFound(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "developmentenvironments"}, object.GetName())
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}
func (c *parentNotFoundClient) Status() client.SubResourceWriter {
	if c.failStatusPatch {
		return &notFoundStatusWriter{SubResourceWriter: c.Client.Status()}
	}
	return c.Client.Status()
}

type notFoundStatusWriter struct{ client.SubResourceWriter }

func (w *notFoundStatusWriter) Patch(_ context.Context, object client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "developmentenvironments"}, object.GetName())
}

type statusPatchCountingClient struct {
	client.Client
	statusPatches int
}

func (c *statusPatchCountingClient) Status() client.SubResourceWriter {
	return &statusPatchCountingWriter{SubResourceWriter: c.Client.Status(), count: &c.statusPatches}
}

type statusPatchCountingWriter struct {
	client.SubResourceWriter
	count *int
}

func (w *statusPatchCountingWriter) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
	*w.count++
	return w.SubResourceWriter.Patch(ctx, object, patch, options...)
}

func unitReconciler(t *testing.T) (*DevelopmentEnvironmentReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&platformv1alpha1.DevelopmentEnvironment{}, &appsv1.Deployment{}, &corev1.PersistentVolumeClaim{}).Build()
	return &DevelopmentEnvironmentReconciler{Client: base, APIReader: base, Scheme: scheme}, base
}

func unitEnvironment(name string) *platformv1alpha1.DevelopmentEnvironment {
	return &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: unitNamespace, UID: types.UID("uid-" + name), Generation: 1}, Spec: platformv1alpha1.DevelopmentEnvironmentSpec{Image: "codercom/code-server:latest", Resources: platformv1alpha1.ResourceSpec{CPURequest: resource.MustParse("250m"), CPULimit: resource.MustParse("1"), MemoryRequest: resource.MustParse("512Mi"), MemoryLimit: resource.MustParse("1Gi")}, Storage: platformv1alpha1.StorageSpec{Size: resource.MustParse("2Gi"), RetentionPolicy: unitRetainPolicy}}}
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: unitNamespace}}
}

func markChildrenReady(t *testing.T, base client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	key := client.ObjectKey{Name: name, Namespace: unitNamespace}
	pvc := &corev1.PersistentVolumeClaim{}
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(ctx, key, env); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKey{Name: naming.PVC(env), Namespace: unitNamespace}, pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: pvc.Spec.Resources.Requests[corev1.ResourceStorage]}
	if err := base.Status().Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := base.Get(ctx, key, deployment); err != nil {
		t.Fatal(err)
	}
	deployment.Status.AvailableReplicas = 1
	if err := base.Status().Update(ctx, deployment); err != nil {
		t.Fatal(err)
	}
}

func readyEnvironmentWithStorageClass(t *testing.T, name string, allowExpansion bool) (*DevelopmentEnvironmentReconciler, client.Client, *platformv1alpha1.DevelopmentEnvironment) {
	t.Helper()
	reconciler, base := unitReconciler(t)
	storageClassName := name + "-class"
	storageClass := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, Provisioner: "example.com/test", AllowVolumeExpansion: &allowExpansion}
	if err := base.Create(context.Background(), storageClass); err != nil {
		t.Fatal(err)
	}
	env := unitEnvironment(name)
	env.Spec.Storage.StorageClassName = storageClassName
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	markChildrenReady(t, base, env.Name)
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, base, env.Name, platformv1alpha1.PhaseReady)
	return reconciler, base, env
}

func updateStorageSize(t *testing.T, base client.Client, name, size string) {
	t.Helper()
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: name, Namespace: unitNamespace}, env); err != nil {
		t.Fatal(err)
	}
	env.Spec.Storage.Size = resource.MustParse(size)
	env.Generation++
	if err := base.Update(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func getPVC(t *testing.T, base client.Client, environmentName string) *corev1.PersistentVolumeClaim {
	t.Helper()
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: environmentName, Namespace: unitNamespace}, env); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: naming.PVC(env), Namespace: env.Namespace}, pvc); err != nil {
		t.Fatal(err)
	}
	return pvc
}

func assertCondition(t *testing.T, base client.Client, name, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: name, Namespace: unitNamespace}, env); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(env.Status.Conditions, conditionType)
	if condition == nil || condition.Status != status || condition.Reason != reason {
		t.Fatalf("condition %s = %#v, want status=%s reason=%s", conditionType, condition, status, reason)
	}
}

func assertPhase(t *testing.T, base client.Client, name string, want platformv1alpha1.DevelopmentEnvironmentPhase) {
	t.Helper()
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: name, Namespace: unitNamespace}, env); err != nil {
		t.Fatal(err)
	}
	if env.Status.Phase != want {
		t.Fatalf("phase = %q, want %q", env.Status.Phase, want)
	}
}

func assertIngressPreservedAndRouted(t *testing.T, ingress *networkingv1.Ingress, env *platformv1alpha1.DevelopmentEnvironment, ingressClassName string) {
	t.Helper()
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != ingressClassName || ingress.Annotations["external.example/kept"] != "true" {
		t.Fatal("Ingress reconciliation did not preserve unrelated fields")
	}
	backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if backend == nil || backend.Name != naming.Service(env) {
		t.Fatalf("Ingress backend = %#v, want named Service %q", backend, naming.Service(env))
	}
}
