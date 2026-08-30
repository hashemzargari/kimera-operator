/* Copyright 2026. Licensed under the Apache License, Version 2.0. */
package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	"github.com/hashemzargari/kimera-operator/internal/resources"
	statusutil "github.com/hashemzargari/kimera-operator/internal/status"
)

const (
	finalizer   = "platform.kimera.dev/finalizer"
	logFieldPVC = "pvc"
)

// DevelopmentEnvironmentReconciler reconciles DevelopmentEnvironment resources and their children.
type DevelopmentEnvironmentReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get

// Reconcile converges the deterministic Phase 1 child resources. API failures are returned so
// controller-runtime retries; only invalid, unfulfillable user input is recorded as Failed.
func (r *DevelopmentEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	env := &platformv1alpha1.DevelopmentEnvironment{}
	if err := r.Get(ctx, req.NamespacedName, env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	logger := ctrl.LoggerFrom(ctx).WithValues(
		"developmentEnvironment", env.Name,
		"namespace", env.Namespace,
		"generation", env.Generation,
	)
	ctx = crlog.IntoContext(ctx, logger)
	logger.V(2).Info("Reconciling DevelopmentEnvironment")
	if !env.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, env)
	}
	if !controllerutil.ContainsFinalizer(env, finalizer) {
		before := env.DeepCopy()
		controllerutil.AddFinalizer(env, finalizer)
		if err := r.patchParent(ctx, env, before, "add finalizer"); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Added DevelopmentEnvironment finalizer")
		return ctrl.Result{}, nil
	}

	before := env.DeepCopy()
	if err := validateQuantities(env); err != nil {
		return r.recordInvalidSpec(ctx, env, before, err)
	}
	refsReady, refsMessage, err := r.referencesExist(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePVC(ctx, env); err != nil {
		var storageIssue *terminalStorageError
		if errors.As(err, &storageIssue) {
			return r.recordStorageFailure(ctx, env, before, storageIssue)
		}
		if _, invalid := err.(*specError); invalid {
			return r.recordInvalidSpec(ctx, env, before, err)
		}
		return ctrl.Result{}, err
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredDeployment(env)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile Deployment: %w", err)
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredService(env)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile Service: %w", err)
	}
	if env.Spec.Network.Enabled {
		if err := r.reconcileOwned(ctx, env, resources.DesiredIngress(env)); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile Ingress: %w", err)
		}
	} else if err := r.Delete(ctx, resources.DesiredIngress(env)); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, fmt.Errorf("delete disabled Ingress: %w", err)
	}

	if err := r.setReadiness(ctx, env, before.Status.Phase, refsReady, refsMessage); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.writeStatus(ctx, before, env)
}

type specError struct{ message string }

func (e *specError) Error() string { return e.message }

type terminalStorageError struct {
	phase      platformv1alpha1.DevelopmentEnvironmentPhase
	reason     string
	message    string
	logMessage string
	logFields  []any
}

func (e *terminalStorageError) Error() string { return e.message }

func validateQuantities(env *platformv1alpha1.DevelopmentEnvironment) error {
	for name, value := range map[string]resource.Quantity{"cpuRequest": env.Spec.Resources.CPURequest, "cpuLimit": env.Spec.Resources.CPULimit, "memoryRequest": env.Spec.Resources.MemoryRequest, "memoryLimit": env.Spec.Resources.MemoryLimit, "storage.size": env.Spec.Storage.Size} {
		if value.Sign() <= 0 {
			return &specError{fmt.Sprintf("spec.%s must be positive", name)}
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

func deletePVC(env *platformv1alpha1.DevelopmentEnvironment) bool {
	return env.Spec.Storage.RetentionPolicy == "Delete"
}

// reconcilePVC changes only the storage request. PVCs cannot be shrunk, and StorageClass is
// intentionally immutable after creation because changing it would require a replacement claim.
func (r *DevelopmentEnvironmentReconciler) reconcilePVC(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) error {
	logger := ctrl.LoggerFrom(ctx)
	if env.UID == "" {
		return &specError{"metadata.uid is required to derive a safe workspace storage identity"}
	}
	desired := resources.DesiredPVC(env)
	logger = logger.WithValues("pvc", desired.Name)
	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		if deletePVC(env) {
			if err := controllerutil.SetControllerReference(env, desired, r.Scheme); err != nil {
				return err
			}
		}
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("create workspace PVC: %w", err)
		}
		logger.Info("Created workspace PVC",
			"retentionPolicy", env.Spec.Storage.RetentionPolicy,
			"requestedSize", env.Spec.Storage.Size.String(),
			"storageClass", env.Spec.Storage.StorageClassName,
			"storageIdentity", naming.StorageIdentity(env),
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get PVC: %w", err)
	}
	if err := validatePVCIdentityAndCompatibility(existing, desired, env); err != nil {
		return err
	}
	current := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if env.Spec.Storage.Size.Cmp(current) < 0 {
		err := storageFailure(platformv1alpha1.PhaseFailed, "VolumeShrinkUnsupported",
			fmt.Sprintf("Requested workspace storage %s is smaller than current PVC request %s; shrinking is unsupported", env.Spec.Storage.Size.String(), current.String()),
			"Workspace PVC shrink is unsupported", existing, current, env.Spec.Storage.Size)
		return err
	}
	before := existing.DeepCopy()
	ownershipChanged := false
	if !deletePVC(env) && metav1.IsControlledBy(existing, env) {
		if err := controllerutil.RemoveControllerReference(env, existing, r.Scheme); err != nil {
			return fmt.Errorf("remove PVC controller reference: %w", err)
		}
		ownershipChanged = true
	}
	if deletePVC(env) && !metav1.IsControlledBy(existing, env) {
		if err := controllerutil.SetControllerReference(env, existing, r.Scheme); err != nil {
			return fmt.Errorf("set PVC controller reference: %w", err)
		}
		ownershipChanged = true
	}
	expanded := env.Spec.Storage.Size.Cmp(current) > 0
	if expanded {
		if err := r.validatePVCExpansion(ctx, existing, current, env.Spec.Storage.Size); err != nil {
			return err
		}
		existing.Spec.Resources.Requests[corev1.ResourceStorage] = env.Spec.Storage.Size
	}
	if ownershipChanged || expanded {
		if err := r.Patch(ctx, existing, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("patch PVC: %w", err)
		}
		if ownershipChanged {
			logger.Info("Updated workspace PVC controller ownership", "retentionPolicy", env.Spec.Storage.RetentionPolicy, "storageIdentity", naming.StorageIdentity(env))
		}
		if expanded {
			logger.Info("Expanded workspace PVC", "previousSize", current.String(), "requestedSize", env.Spec.Storage.Size.String(), "storageIdentity", naming.StorageIdentity(env))
		}
	} else {
		logger.V(2).Info("Workspace PVC for current storage identity is already reconciled", "storageIdentity", naming.StorageIdentity(env))
	}
	return nil
}

func validatePVCIdentityAndCompatibility(existing, desired *corev1.PersistentVolumeClaim, env *platformv1alpha1.DevelopmentEnvironment) error {
	identity := naming.StorageIdentity(env)
	if existing.Labels[naming.ManagedByLabel] != naming.ManagedByValue ||
		existing.Labels[naming.EnvironmentUIDLabel] != identity ||
		existing.Annotations[naming.OriginalEnvironmentUIDAnnotation] != identity ||
		existing.Annotations[naming.StorageIdentityAnnotation] != identity ||
		existing.Annotations[naming.OriginalEnvironmentNameAnnotation] != env.Name {
		return &terminalStorageError{phase: platformv1alpha1.PhaseFailed, reason: "StorageIdentityMismatch", message: fmt.Sprintf("PVC %q does not have provenance for DevelopmentEnvironment UID %q; refusing automatic adoption", existing.Name, identity), logMessage: "Workspace PVC identity is incompatible", logFields: []any{logFieldPVC, existing.Name, "storageIdentity", identity}}
	}
	if env.Spec.Storage.StorageClassName != "" && (existing.Spec.StorageClassName == nil || *existing.Spec.StorageClassName != env.Spec.Storage.StorageClassName) {
		actual := ""
		if existing.Spec.StorageClassName != nil {
			actual = *existing.Spec.StorageClassName
		}
		return &terminalStorageError{phase: platformv1alpha1.PhaseFailed, reason: "StorageClassImmutable", message: fmt.Sprintf("PVC %q uses StorageClass %q, requested %q; storageClassName is immutable", existing.Name, actual, env.Spec.Storage.StorageClassName), logMessage: "Workspace PVC StorageClass is immutable", logFields: []any{logFieldPVC, existing.Name, "currentStorageClass", actual, "requestedStorageClass", env.Spec.Storage.StorageClassName}}
	}
	if !equality.Semantic.DeepEqual(existing.Spec.AccessModes, desired.Spec.AccessModes) {
		return &terminalStorageError{phase: platformv1alpha1.PhaseFailed, reason: "AccessModesIncompatible", message: fmt.Sprintf("PVC %q access modes %v do not match required access modes %v", existing.Name, existing.Spec.AccessModes, desired.Spec.AccessModes), logMessage: "Workspace PVC access modes are incompatible", logFields: []any{logFieldPVC, existing.Name}}
	}
	return nil
}

func (r *DevelopmentEnvironmentReconciler) validatePVCExpansion(ctx context.Context, pvc *corev1.PersistentVolumeClaim, current, requested resource.Quantity) error {
	if pvc.Status.Phase != corev1.ClaimBound {
		return storageFailure(platformv1alpha1.PhaseDegraded, "VolumeExpansionUnsupported",
			fmt.Sprintf("Requested workspace storage %s exceeds current %s, but PVC %q is %s; only Bound PVCs can be expanded", requested.String(), current.String(), pvc.Name, pvc.Status.Phase),
			"Workspace PVC expansion is unsupported", pvc, current, requested)
	}
	storageClassName := ""
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}
	if storageClassName == "" {
		return storageFailure(platformv1alpha1.PhaseDegraded, "VolumeExpansionUnsupported",
			fmt.Sprintf("Requested workspace storage %s exceeds current %s, but PVC %q has no StorageClass and is not eligible for expansion", requested.String(), current.String(), pvc.Name),
			"Workspace PVC expansion is unsupported", pvc, current, requested)
	}
	storageClass := &storagev1.StorageClass{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, client.ObjectKey{Name: storageClassName}, storageClass); apierrors.IsNotFound(err) {
		return storageFailure(platformv1alpha1.PhaseDegraded, "VolumeExpansionUnsupported",
			fmt.Sprintf("Requested workspace storage %s exceeds current %s, but StorageClass %q does not exist", requested.String(), current.String(), storageClassName),
			"Workspace PVC expansion is unsupported", pvc, current, requested)
	} else if err != nil {
		return fmt.Errorf("get StorageClass %q for PVC expansion: %w", storageClassName, err)
	}
	if storageClass.Provisioner == "kubernetes.io/no-provisioner" || storageClass.AllowVolumeExpansion == nil || !*storageClass.AllowVolumeExpansion {
		return storageFailure(platformv1alpha1.PhaseDegraded, "VolumeExpansionUnsupported",
			fmt.Sprintf("Requested workspace storage %s exceeds current %s, but StorageClass %q does not support volume expansion", requested.String(), current.String(), storageClassName),
			"Workspace PVC expansion is unsupported", pvc, current, requested)
	}
	return nil
}

func storageFailure(phase platformv1alpha1.DevelopmentEnvironmentPhase, reason, message, logMessage string, pvc *corev1.PersistentVolumeClaim, current, requested resource.Quantity) *terminalStorageError {
	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}
	return &terminalStorageError{phase: phase, reason: reason, message: message, logMessage: logMessage, logFields: []any{logFieldPVC, pvc.Name, "currentSize", current.String(), "requestedSize", requested.String(), "storageClass", storageClass}}
}

func (r *DevelopmentEnvironmentReconciler) reconcileOwned(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, desired client.Object) error {
	expected := desired.DeepCopyObject().(client.Object)
	// Apply the same built-in Kubernetes defaults the API server applies. Without this,
	// each reconcile can clear defaulted PodTemplate fields that the API server restores.
	r.Scheme.Default(expected)
	operation, err := controllerutil.CreateOrPatch(ctx, r.Client, desired, func() error {
		labels := desired.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, expected.GetLabels())
		desired.SetLabels(labels)
		switch actual := desired.(type) {
		case *appsv1.Deployment:
			wanted := expected.(*appsv1.Deployment)
			if !actual.CreationTimestamp.IsZero() && !equality.Semantic.DeepEqual(actual.Spec.Selector, wanted.Spec.Selector) {
				return fmt.Errorf("managed Deployment selector differs from the immutable desired selector")
			}
			if actual.CreationTimestamp.IsZero() {
				actual.Spec.Selector = wanted.Spec.Selector
			}
			if !equality.Semantic.DeepEqual(actual.Spec.Replicas, wanted.Spec.Replicas) {
				actual.Spec.Replicas = wanted.Spec.Replicas
			}
			templateLabels := actual.Spec.Template.Labels
			if templateLabels == nil {
				templateLabels = map[string]string{}
			}
			maps.Copy(templateLabels, wanted.Spec.Template.Labels)
			actual.Spec.Template.Labels = templateLabels
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.SecurityContext, wanted.Spec.Template.Spec.SecurityContext) {
				actual.Spec.Template.Spec.SecurityContext = wanted.Spec.Template.Spec.SecurityContext
			}
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.Containers, wanted.Spec.Template.Spec.Containers) {
				actual.Spec.Template.Spec.Containers = wanted.Spec.Template.Spec.Containers
			}
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.Volumes, wanted.Spec.Template.Spec.Volumes) {
				actual.Spec.Template.Spec.Volumes = wanted.Spec.Template.Spec.Volumes
			}
		case *corev1.Service:
			wanted := expected.(*corev1.Service)
			actual.Spec.Type = wanted.Spec.Type
			actual.Spec.Selector = wanted.Spec.Selector
			actual.Spec.Ports = wanted.Spec.Ports
		case *networkingv1.Ingress:
			actual.Spec.Rules = expected.(*networkingv1.Ingress).Spec.Rules
		}
		if metav1.IsControlledBy(desired, env) {
			return nil
		}
		return controllerutil.SetControllerReference(env, desired, r.Scheme)
	})
	if err != nil {
		return err
	}
	kind := childKind(desired)
	switch operation {
	case controllerutil.OperationResultCreated:
		ctrl.LoggerFrom(ctx).Info("Created managed child resource", "kind", kind, "name", desired.GetName())
	case controllerutil.OperationResultUpdated, controllerutil.OperationResultUpdatedStatus, controllerutil.OperationResultUpdatedStatusOnly:
		ctrl.LoggerFrom(ctx).Info("Patched managed child resource", "kind", kind, "name", desired.GetName())
	default:
		ctrl.LoggerFrom(ctx).V(2).Info("Managed child resource is unchanged", "kind", kind, "name", desired.GetName())
	}
	return err
}

func childKind(object client.Object) string {
	switch object.(type) {
	case *appsv1.Deployment:
		return "Deployment"
	case *corev1.Service:
		return "Service"
	case *networkingv1.Ingress:
		return "Ingress"
	case *corev1.PersistentVolumeClaim:
		return "PersistentVolumeClaim"
	default:
		return fmt.Sprintf("%T", object)
	}
}

func (r *DevelopmentEnvironmentReconciler) setReadiness(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, previousPhase platformv1alpha1.DevelopmentEnvironmentPhase, refsReady bool, refsMessage string) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.PVC(env)}, pvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("observe PVC: %w", err)
	}
	requestedStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	availableStorage := pvc.Status.Capacity[corev1.ResourceStorage]
	storageReady := pvc.Status.Phase == corev1.ClaimBound &&
		requestedStorage.Cmp(env.Spec.Storage.Size) >= 0 &&
		availableStorage.Cmp(env.Spec.Storage.Size) >= 0
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Deployment(env)}, deployment); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("observe Deployment: %w", err)
	}
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}
	workloadReady := deployment.Status.AvailableReplicas >= desiredReplicas
	service := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Service(env)}, service); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("observe Service: %w", err)
	}
	serviceReady := service.Name != ""
	networkReady := !env.Spec.Network.Enabled
	if env.Spec.Network.Enabled {
		ingress := &networkingv1.Ingress{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Ingress(env)}, ingress); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("observe Ingress: %w", err)
		}
		networkReady = ingress.Name != ""
		env.Status.EnvironmentURL = "http://" + env.Spec.Network.Host
	} else {
		env.Status.EnvironmentURL = ""
	}
	storageMessage := "Workspace PVC is not Bound"
	if pvc.Status.Phase == corev1.ClaimBound && !storageReady {
		storageMessage = fmt.Sprintf("Workspace PVC does not yet provide requested storage %s", env.Spec.Storage.Size.String())
	} else if storageReady {
		storageMessage = "Workspace PVC is Bound"
	}
	workloadMessage := "IDE Deployment has no available replicas"
	if workloadReady {
		workloadMessage = "IDE Deployment has the desired available replicas"
	}
	networkMessage := "Managed Ingress has not been created"
	if networkReady {
		networkMessage = "Network configuration is ready"
	}
	statusutil.Set(env, statusutil.StorageReady, condition(storageReady), "PVCObserved", storageMessage)
	statusutil.Set(env, statusutil.WorkloadReady, condition(workloadReady), "DeploymentObserved", workloadMessage)
	statusutil.Set(env, statusutil.NetworkReady, condition(networkReady), "IngressObserved", networkMessage)
	env.Status.ObservedGeneration = env.Generation
	allReady := storageReady && workloadReady && serviceReady && networkReady && refsReady
	if allReady {
		env.Status.Phase = platformv1alpha1.PhaseReady
		statusutil.Set(env, statusutil.Ready, metav1.ConditionTrue, "ResourcesReady", "DevelopmentEnvironment is ready")
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Reconciled", "Desired resources are ready")
		if previousPhase != platformv1alpha1.PhaseReady {
			ctrl.LoggerFrom(ctx).Info("DevelopmentEnvironment became Ready", "storageReady", storageReady, "workloadReady", workloadReady, "networkReady", networkReady)
		}
		return nil
	}
	message := "Waiting for PVC, Deployment, Service, or Ingress readiness"
	if !refsReady {
		message = refsMessage
	}
	wasReady := previousPhase == platformv1alpha1.PhaseReady || previousPhase == platformv1alpha1.PhaseDegraded
	if wasReady {
		env.Status.Phase = platformv1alpha1.PhaseDegraded
		statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "HealthLost", message)
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Degraded", message)
		if previousPhase == platformv1alpha1.PhaseReady {
			ctrl.LoggerFrom(ctx).Info("DevelopmentEnvironment left Ready state", "storageReady", storageReady, "workloadReady", workloadReady, "networkReady", networkReady)
		}
	} else {
		env.Status.Phase = platformv1alpha1.PhaseProvisioning
		statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "Provisioning", message)
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionTrue, "Provisioning", message)
	}
	return nil
}

func condition(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
func (r *DevelopmentEnvironmentReconciler) writeStatus(ctx context.Context, before, env *platformv1alpha1.DevelopmentEnvironment) error {
	if equality.Semantic.DeepEqual(before.Status, env.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, env, client.MergeFrom(before)); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}
func (r *DevelopmentEnvironmentReconciler) recordInvalidSpec(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, reconcileErr error) (ctrl.Result, error) {
	env.Status.Phase = platformv1alpha1.PhaseFailed
	env.Status.ObservedGeneration = env.Generation
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "InvalidSpecification", reconcileErr.Error())
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Failed", reconcileErr.Error())
	return ctrl.Result{}, r.writeStatus(ctx, before, env)
}

func (r *DevelopmentEnvironmentReconciler) recordStorageFailure(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, issue *terminalStorageError) (ctrl.Result, error) {
	condition := meta.FindStatusCondition(env.Status.Conditions, statusutil.StorageReady)
	transition := env.Status.Phase != issue.phase || env.Status.ObservedGeneration != env.Generation || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != issue.reason || condition.Message != issue.message
	env.Status.Phase = issue.phase
	env.Status.ObservedGeneration = env.Generation
	statusutil.Set(env, statusutil.StorageReady, metav1.ConditionFalse, issue.reason, issue.message)
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, issue.reason, issue.message)
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, issue.reason, issue.message)
	if err := r.writeStatus(ctx, before, env); err != nil {
		return ctrl.Result{}, err
	}
	if transition {
		ctrl.LoggerFrom(ctx).Info(issue.logMessage, issue.logFields...)
	}
	return ctrl.Result{}, nil
}

func (r *DevelopmentEnvironmentReconciler) reconcileDelete(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Reconciling DevelopmentEnvironment deletion", "retentionPolicy", env.Spec.Storage.RetentionPolicy)
	if !deletePVC(env) {
		pvc := resources.DesiredPVC(env)
		existing := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), existing); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		} else if err == nil && metav1.IsControlledBy(existing, env) {
			pvcLogger := logger.WithValues("pvc", existing.Name)
			pvcLogger.Info("Retaining workspace PVC", "storageIdentity", naming.StorageIdentity(env))
			before := existing.DeepCopy()
			if err := controllerutil.RemoveControllerReference(env, existing, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Patch(ctx, existing, client.MergeFrom(before)); apierrors.IsNotFound(err) {
				// The claim disappeared concurrently, so there is nothing left to retain.
			} else if err != nil {
				return ctrl.Result{}, err
			} else {
				pvcLogger.Info("Removed controller ownership from retained workspace PVC", "storageIdentity", naming.StorageIdentity(env))
			}
		} else if err == nil {
			logger.WithValues("pvc", existing.Name).V(2).Info("Retained workspace PVC is already detached", "storageIdentity", naming.StorageIdentity(env))
		}
	}
	objects := []client.Object{resources.DesiredDeployment(env), resources.DesiredService(env), resources.DesiredIngress(env)}
	if deletePVC(env) {
		objects = append(objects, resources.DesiredPVC(env))
	}
	for _, object := range objects {
		done, err := r.deleteAndConfirm(ctx, object)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	before := env.DeepCopy()
	controllerutil.RemoveFinalizer(env, finalizer)
	logger.Info("Removing DevelopmentEnvironment finalizer")
	return ctrl.Result{}, r.patchParent(ctx, env, before, "remove finalizer")
}
func (r *DevelopmentEnvironmentReconciler) deleteAndConfirm(ctx context.Context, object client.Object) (bool, error) {
	key := client.ObjectKeyFromObject(object)
	if err := r.Get(ctx, key, object); apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("get %T before deletion: %w", object, err)
	}
	if object.GetDeletionTimestamp().IsZero() {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			ctrl.LoggerFrom(ctx).WithValues("pvc", pvc.Name).Info("Deleting workspace PVC", "storageIdentity", pvc.Annotations[naming.StorageIdentityAnnotation])
		}
		if err := r.Delete(ctx, object); apierrors.IsNotFound(err) {
			return true, nil
		} else if err != nil {
			return false, fmt.Errorf("delete %T: %w", object, err)
		}
	}
	err := r.Get(ctx, key, object)
	if apierrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Info("Deleted managed child resource", "kind", childKind(object), "name", object.GetName())
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("confirm deletion of %T: %w", object, err)
	}
	return false, nil
}

// patchParent updates parent metadata without rewriting a concurrently edited spec. Once the
// parent has disappeared, finalizer/status work is complete and NotFound is terminal.
func (r *DevelopmentEnvironmentReconciler) patchParent(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, operation string) error {
	if err := r.Patch(ctx, env, client.MergeFrom(before)); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

// SetupWithManager watches owned children so their deletion or drift enqueues the parent and is repaired.
func (r *DevelopmentEnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&platformv1alpha1.DevelopmentEnvironment{}).Owns(&appsv1.Deployment{}).Owns(&corev1.Service{}).Owns(&corev1.PersistentVolumeClaim{}).Owns(&networkingv1.Ingress{}).Named("developmentenvironment").Complete(r)
}
