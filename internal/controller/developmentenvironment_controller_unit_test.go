package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	"github.com/hashemzargari/kimera-operator/internal/resources"
)

func TestReconcileMissingParentIsTerminal(t *testing.T) {
	reconciler, _ := unitReconciler(t)
	result, err := reconciler.Reconcile(context.Background(), request("does-not-exist"))
	if err != nil || result.Requeue {
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
	if err != nil || result.Requeue {
		t.Fatalf("cleanup with absent children should succeed: result=%+v err=%v", result, err)
	}
}

func TestNoOpReconcileDoesNotPatchDeployment(t *testing.T) {
	reconciler, base := unitReconciler(t)
	env := unitEnvironment("no-op")
	if err := base.Create(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	tracker := &deploymentPatchCountingClient{Client: base}
	reconciler.Client = tracker
	if _, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil {
		t.Fatal(err)
	}
	if tracker.deploymentPatches != 0 {
		t.Fatalf("no-op reconciliation patched Deployment %d times", tracker.deploymentPatches)
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
	}{{"retain-delete", "Retain", true}, {"delete-delete", "Delete", false}} {
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
			err := base.Get(context.Background(), client.ObjectKey{Name: naming.PVC(env), Namespace: "default"}, pvc)
			if test.pvcRemains && err != nil {
				t.Fatalf("retained PVC missing: %v", err)
			}
			if !test.pvcRemains && !apierrors.IsNotFound(err) {
				t.Fatalf("Delete-policy PVC still exists: %v", err)
			}
			if test.pvcRemains && metav1.IsControlledBy(pvc, env) {
				t.Fatal("retained PVC still has parent controller reference")
			}
			if result, err := reconciler.Reconcile(context.Background(), request(env.Name)); err != nil || result.Requeue {
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
	service.Spec.ClusterIP = "10.0.0.42"
	service.Spec.ClusterIPs = []string{"10.0.0.42"}
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
	if service.Spec.ClusterIP != "10.0.0.42" || len(service.Spec.ClusterIPs) != 1 || service.Spec.IPFamilyPolicy == nil || *service.Spec.IPFamilyPolicy != policy {
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
}

func (c *conflictOnceClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if c.conflictDeploymentPatch {
		if _, ok := object.(*appsv1.Deployment); ok {
			c.conflictDeploymentPatch = false
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("simulated conflict"))
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

type deploymentPatchCountingClient struct {
	client.Client
	deploymentPatches int
}

func (c *deploymentPatchCountingClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if _, ok := object.(*appsv1.Deployment); ok {
		c.deploymentPatches++
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
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&platformv1alpha1.DevelopmentEnvironment{}, &appsv1.Deployment{}, &corev1.PersistentVolumeClaim{}).Build()
	return &DevelopmentEnvironmentReconciler{Client: base, Scheme: scheme}, base
}

func unitEnvironment(name string) *platformv1alpha1.DevelopmentEnvironment {
	return &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)}, Spec: platformv1alpha1.DevelopmentEnvironmentSpec{Image: "codercom/code-server:latest", Resources: platformv1alpha1.ResourceSpec{CPURequest: resource.MustParse("250m"), CPULimit: resource.MustParse("1"), MemoryRequest: resource.MustParse("512Mi"), MemoryLimit: resource.MustParse("1Gi")}, Storage: platformv1alpha1.StorageSpec{Size: resource.MustParse("2Gi"), RetentionPolicy: "Retain"}}}
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: "default"}}
}

func markChildrenReady(t *testing.T, base client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	key := client.ObjectKey{Name: name, Namespace: "default"}
	pvc := &corev1.PersistentVolumeClaim{}
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(ctx, key, env); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKey{Name: naming.PVC(env), Namespace: "default"}, pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Status.Phase = corev1.ClaimBound
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

func assertPhase(t *testing.T, base client.Client, name string, want platformv1alpha1.DevelopmentEnvironmentPhase) {
	t.Helper()
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := base.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "default"}, env); err != nil {
		t.Fatal(err)
	}
	if env.Status.Phase != want {
		t.Fatalf("phase = %q, want %q", env.Status.Phase, want)
	}
}
