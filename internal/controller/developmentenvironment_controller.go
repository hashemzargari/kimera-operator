/* Copyright 2026. Licensed under the Apache License, Version 2.0. */
package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	"github.com/hashemzargari/kimera-operator/internal/resources"
	statusutil "github.com/hashemzargari/kimera-operator/internal/status"
)

const finalizer = "platform.kimera.dev/finalizer"

// DevelopmentEnvironmentReconciler reconciles DevelopmentEnvironment resources and their children.
type DevelopmentEnvironmentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

// Reconcile makes the deterministic PVC, Deployment, Service, and optional Ingress match the spec.
func (r *DevelopmentEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := r.Get(ctx, req.NamespacedName, env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !env.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, env)
	}
	if !controllerutil.ContainsFinalizer(env, finalizer) {
		controllerutil.AddFinalizer(env, finalizer)
		return ctrl.Result{}, r.Update(ctx, env)
	}
	before := env.DeepCopy()
	env.Status.Phase = platformv1alpha1.PhaseProvisioning
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionTrue, "Reconciling", "Reconciling desired resources")
	if err := validateQuantities(env); err != nil {
		return r.failStatus(ctx, env, before, err)
	}
	refsOK, refMessage, err := r.referencesExist(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePVC(ctx, env); err != nil {
		return r.failStatus(ctx, env, before, err)
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredDeployment(env)); err != nil {
		return r.failStatus(ctx, env, before, err)
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredService(env)); err != nil {
		return r.failStatus(ctx, env, before, err)
	}
	if env.Spec.Network.Enabled {
		if err := r.reconcileOwned(ctx, env, resources.DesiredIngress(env)); err != nil {
			return r.failStatus(ctx, env, before, err)
		}
	} else if err := r.Delete(ctx, resources.DesiredIngress(env)); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, fmt.Errorf("delete disabled ingress: %w", err)
	}
	r.setReadiness(ctx, env, refsOK, refMessage)
	if !equality.Semantic.DeepEqual(before.Status, env.Status) {
		if err := r.Status().Patch(ctx, env, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

func validateQuantities(env *platformv1alpha1.DevelopmentEnvironment) error {
	for name, value := range map[string]resource.Quantity{"cpuRequest": env.Spec.Resources.CPURequest, "cpuLimit": env.Spec.Resources.CPULimit, "memoryRequest": env.Spec.Resources.MemoryRequest, "memoryLimit": env.Spec.Resources.MemoryLimit, "storage.size": env.Spec.Storage.Size} {
		if value.Sign() <= 0 {
			return fmt.Errorf("spec.%s must be positive", name)
		}
	}
	return nil
}
func (r *DevelopmentEnvironmentReconciler) referencesExist(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) (bool, string, error) {
	if env.Spec.ConfigMapRef != nil {
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.ConfigMapRef.Name}, &corev1.ConfigMap{}); err != nil {
			if apierrors.IsNotFound(err) {
				return false, "Referenced ConfigMap is missing", nil
			}
			return false, "", fmt.Errorf("get referenced ConfigMap: %w", err)
		}
	}
	for _, ref := range env.Spec.SecretRefs {
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: ref.Name}, &corev1.Secret{}); err != nil {
			if apierrors.IsNotFound(err) {
				return false, "Referenced Secret is missing", nil
			}
			return false, "", fmt.Errorf("get referenced Secret: %w", err)
		}
	}
	return true, "", nil
}
func (r *DevelopmentEnvironmentReconciler) reconcilePVC(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) error {
	desired := resources.DesiredPVC(env)
	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		if env.Spec.Storage.RetentionPolicy == "Delete" {
			if err := controllerutil.SetControllerReference(env, desired, r.Scheme); err != nil {
				return err
			}
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return fmt.Errorf("get PVC: %w", err)
	}
	if env.Spec.Storage.RetentionPolicy == "Retain" && metav1.IsControlledBy(existing, env) {
		// A policy change must remove an old controller reference before deletion can
		// preserve the claim. This is safe because this controller is the only owner it adds.
		if err := controllerutil.RemoveControllerReference(env, existing, r.Scheme); err != nil {
			return fmt.Errorf("remove PVC controller reference: %w", err)
		}
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("detach retained PVC: %w", err)
		}
	} else if !metav1.IsControlledBy(existing, env) {
		if err := controllerutil.SetControllerReference(env, existing, r.Scheme); err != nil {
			return fmt.Errorf("set PVC controller reference: %w", err)
		}
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("update PVC controller reference: %w", err)
		}
	}
	current := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if env.Spec.Storage.Size.Cmp(current) < 0 {
		return fmt.Errorf("requested storage %s is smaller than existing PVC size %s; shrinking is unsupported", env.Spec.Storage.Size.String(), current.String())
	}
	if env.Spec.Storage.Size.Cmp(current) > 0 {
		existing.Spec.Resources.Requests[corev1.ResourceStorage] = env.Spec.Storage.Size
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("expand PVC: %w", err)
		}
	}
	return nil
}
func (r *DevelopmentEnvironmentReconciler) reconcileOwned(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, desired client.Object) error {
	expected := desired.DeepCopyObject().(client.Object)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, desired, func() error {
		desired.SetLabels(expected.GetLabels())
		switch actual := desired.(type) {
		case *appsv1.Deployment:
			actual.Spec = expected.(*appsv1.Deployment).Spec
		case *corev1.Service:
			// ClusterIP is allocated by Kubernetes and must be preserved on update.
			clusterIP := actual.Spec.ClusterIP
			actual.Spec = expected.(*corev1.Service).Spec
			actual.Spec.ClusterIP = clusterIP
		case *networkingv1.Ingress:
			actual.Spec = expected.(*networkingv1.Ingress).Spec
		}
		return controllerutil.SetControllerReference(env, desired, r.Scheme)
	})
	return err
}
func (r *DevelopmentEnvironmentReconciler) setReadiness(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, refsOK bool, refMessage string) {
	pvc := &corev1.PersistentVolumeClaim{}
	_ = r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.PVC(env)}, pvc)
	storageReady := pvc.Status.Phase == corev1.ClaimBound
	deployment := &appsv1.Deployment{}
	_ = r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Deployment(env)}, deployment)
	workloadReady := deployment.Status.AvailableReplicas >= 1
	networkReady := !env.Spec.Network.Enabled
	if env.Spec.Network.Enabled {
		ingress := &networkingv1.Ingress{}
		networkReady = r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Ingress(env)}, ingress) == nil
		env.Status.EnvironmentURL = "https://" + env.Spec.Network.Host
	} else {
		env.Status.EnvironmentURL = ""
	}
	statusutil.Set(env, statusutil.StorageReady, condition(storageReady), "PVCObserved", "Workspace PVC is observed")
	statusutil.Set(env, statusutil.WorkloadReady, condition(workloadReady), "DeploymentObserved", "IDE Deployment is observed")
	statusutil.Set(env, statusutil.NetworkReady, condition(networkReady), "IngressObserved", "Network configuration is observed")
	env.Status.ObservedGeneration = env.Generation
	if storageReady && workloadReady && networkReady && refsOK {
		env.Status.Phase = platformv1alpha1.PhaseReady
		statusutil.Set(env, statusutil.Ready, metav1.ConditionTrue, "ResourcesReady", "DevelopmentEnvironment is ready")
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Reconciled", "Desired resources are ready")
	} else {
		env.Status.Phase = platformv1alpha1.PhaseDegraded
		msg := "Waiting for child resources"
		if !refsOK {
			msg = refMessage
		}
		statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "NotReady", msg)
	}
}
func condition(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
func (r *DevelopmentEnvironmentReconciler) failStatus(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, reconcileErr error) (ctrl.Result, error) {
	env.Status.Phase = platformv1alpha1.PhaseFailed
	env.Status.ObservedGeneration = env.Generation
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "InvalidSpecification", reconcileErr.Error())
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Failed", reconcileErr.Error())
	if err := r.Status().Patch(ctx, env, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("update failed status: %w", err)
	}
	return ctrl.Result{}, nil
}
func (r *DevelopmentEnvironmentReconciler) reconcileDelete(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) (ctrl.Result, error) {
	for _, obj := range []client.Object{resources.DesiredDeployment(env), resources.DesiredService(env), resources.DesiredIngress(env)} {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("delete managed resource: %w", err)
		}
	}
	if env.Spec.Storage.RetentionPolicy == "Delete" {
		if err := r.Delete(ctx, resources.DesiredPVC(env)); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("delete workspace PVC: %w", err)
		}
	}
	controllerutil.RemoveFinalizer(env, finalizer)
	return ctrl.Result{}, r.Update(ctx, env)
}

// SetupWithManager watches owned children so manual deployment deletion is repaired.
func (r *DevelopmentEnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&platformv1alpha1.DevelopmentEnvironment{}).Owns(&appsv1.Deployment{}).Owns(&corev1.Service{}).Owns(&corev1.PersistentVolumeClaim{}).Owns(&networkingv1.Ingress{}).Named("developmentenvironment").Complete(r)
}
