/* Copyright 2026. Licensed under the Apache License, Version 2.0. */
package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"path"
	"strings"
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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	domainmetrics "github.com/hashemzargari/kimera-operator/internal/metrics"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	"github.com/hashemzargari/kimera-operator/internal/resources"
	statusutil "github.com/hashemzargari/kimera-operator/internal/status"
)

const (
	finalizer          = "platform.kimera.dev/finalizer"
	logFieldPVC        = "pvc"
	defaultGitRevision = "main"
)

// DevelopmentEnvironmentReconciler reconciles DevelopmentEnvironment resources and their children.
type DevelopmentEnvironmentReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
}

// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.kimera.dev,resources=developmentenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;persistentvolumeclaims;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses;networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get

// Reconcile converges the deterministic Phase 2 child resources. API failures are returned so
// controller-runtime retries; only invalid, unfulfillable user input is recorded as Failed.
func (r *DevelopmentEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer r.refreshMetrics(ctx)
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
	if err := validateSpec(env); err != nil {
		return ctrl.Result{}, r.recordInvalidSpec(ctx, env, before, err)
	}
	refsReady, refsMessage, err := r.referencesExist(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePVC(ctx, env); err != nil {
		var storageIssue *terminalStorageError
		if errors.As(err, &storageIssue) {
			return ctrl.Result{}, r.recordStorageFailure(ctx, env, before, storageIssue)
		}
		if _, invalid := err.(*specError); invalid {
			return ctrl.Result{}, r.recordInvalidSpec(ctx, env, before, err)
		}
		return ctrl.Result{}, err
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredServiceAccount(env)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile ServiceAccount: %w", err)
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredNetworkPolicy(env)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile NetworkPolicy: %w", err)
	}
	if err := r.reconcileOwned(ctx, env, resources.DesiredService(env)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile Service: %w", err)
	}
	deploymentReady, err := r.reconcileDeployment(ctx, env)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile Deployment: %w", err)
	}
	if !deploymentReady {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if env.Spec.Network.Enabled {
		if err := r.reconcileOwned(ctx, env, resources.DesiredIngress(env)); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile Ingress: %w", err)
		}
	} else {
		done, err := r.deleteOwnedAndConfirm(ctx, env, resources.DesiredIngress(env))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("delete disabled Ingress: %w", err)
		}
		if !done {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	if err := r.setReadiness(ctx, env, before, refsReady, refsMessage); err != nil {
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

func validateSpec(env *platformv1alpha1.DevelopmentEnvironment) error {
	if env.UID == "" {
		return &specError{"metadata.uid is required to derive safe workload and storage identities"}
	}
	for name, value := range map[string]resource.Quantity{"cpuRequest": env.Spec.Resources.CPURequest, "cpuLimit": env.Spec.Resources.CPULimit, "memoryRequest": env.Spec.Resources.MemoryRequest, "memoryLimit": env.Spec.Resources.MemoryLimit, "storage.size": env.Spec.Storage.Size} {
		if value.Sign() <= 0 {
			return &specError{fmt.Sprintf("spec.%s must be positive", name)}
		}
	}
	if env.Spec.Resources.CPURequest.Cmp(env.Spec.Resources.CPULimit) > 0 {
		return &specError{"spec.resources.cpuRequest must not exceed cpuLimit"}
	}
	if env.Spec.Resources.MemoryRequest.Cmp(env.Spec.Resources.MemoryLimit) > 0 {
		return &specError{"spec.resources.memoryRequest must not exceed memoryLimit"}
	}
	if env.Spec.ConfigMapRef != nil && env.Spec.ConfigMapRef.Name == "" {
		return &specError{"spec.configMapRef.name must not be empty"}
	}
	for i := range env.Spec.SecretRefs {
		if env.Spec.SecretRefs[i].Name == "" {
			return &specError{fmt.Sprintf("spec.secretRefs[%d].name must not be empty", i)}
		}
	}
	if err := validateSource(env.Spec.Source); err != nil {
		return err
	}
	return nil
}

func validateSource(source *platformv1alpha1.SourceSpec) error {
	if source == nil {
		return nil
	}
	if source.Git == nil {
		return &specError{"spec.source.git is required when source is configured"}
	}
	gitURL, err := url.ParseRequestURI(source.Git.URL)
	if err != nil || gitURL.Scheme != "https" || gitURL.Host == "" {
		return &specError{"spec.source.git.url must be an absolute HTTPS URL"}
	}
	if gitURL.User != nil || gitURL.RawQuery != "" || gitURL.Fragment != "" {
		return &specError{"spec.source.git.url must not contain credentials, query parameters, or fragments"}
	}
	revision := source.Git.Revision
	if revision == "" {
		revision = defaultGitRevision
	}
	if strings.Contains(revision, "..") || strings.Contains(revision, "@{") || strings.HasSuffix(revision, ".lock") {
		return &specError{"spec.source.git.revision contains an unsafe Git revision pattern"}
	}
	if source.Git.SubPath != "" {
		cleaned := path.Clean(source.Git.SubPath)
		if cleaned != source.Git.SubPath || path.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return &specError{"spec.source.git.subPath must be a clean relative repository path"}
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

// reconcileDeployment safely replaces a managed Deployment when its immutable selector
// differs from desired state. Its UID-derived PVC is never part of this replacement.
func (r *DevelopmentEnvironmentReconciler) reconcileDeployment(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) (bool, error) {
	desired := resources.DesiredDeployment(env)
	existing := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return true, r.reconcileOwned(ctx, env, desired)
	}
	if err != nil {
		return false, fmt.Errorf("get Deployment before selector reconciliation: %w", err)
	}
	if !metav1.IsControlledBy(existing, env) {
		return false, fmt.Errorf("deployment %s/%s already exists but is not controlled by this DevelopmentEnvironment; refusing adoption or replacement", existing.Namespace, existing.Name)
	}
	if equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return true, r.reconcileOwned(ctx, env, desired)
	}
	if existing.DeletionTimestamp.IsZero() {
		ctrl.LoggerFrom(ctx).Info("Replacing managed Deployment because immutable selector differs from desired state", "deployment", existing.Name, "environmentUID", env.UID)
	}
	done, err := r.deleteOwnedAndConfirm(ctx, env, existing)
	if err != nil || !done {
		return done, err
	}
	return true, r.reconcileOwned(ctx, env, desired)
}

func (r *DevelopmentEnvironmentReconciler) reconcileOwned(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, desired client.Object) error {
	if err := r.verifyOwnedOrAbsent(ctx, env, desired); err != nil {
		return err
	}
	expected := desired.DeepCopyObject().(client.Object)
	// Apply the same built-in Kubernetes defaults the API server applies. Without this,
	// each reconcile can clear defaulted PodTemplate fields that the API server restores.
	r.Scheme.Default(expected)
	operation, err := controllerutil.CreateOrPatch(ctx, r.Client, desired, func() error {
		exists := objectExists(desired)
		if exists && !metav1.IsControlledBy(desired, env) {
			return ownershipCollisionError(desired, env)
		}
		labels := desired.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, expected.GetLabels())
		desired.SetLabels(labels)
		switch actual := desired.(type) {
		case *appsv1.Deployment:
			wanted := expected.(*appsv1.Deployment)
			if exists && !equality.Semantic.DeepEqual(actual.Spec.Selector, wanted.Spec.Selector) {
				return fmt.Errorf("managed Deployment selector differs from the immutable desired selector")
			}
			if !exists {
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
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.InitContainers, wanted.Spec.Template.Spec.InitContainers) {
				actual.Spec.Template.Spec.InitContainers = wanted.Spec.Template.Spec.InitContainers
			}
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.Containers, wanted.Spec.Template.Spec.Containers) {
				actual.Spec.Template.Spec.Containers = wanted.Spec.Template.Spec.Containers
			}
			if !equality.Semantic.DeepEqual(actual.Spec.Template.Spec.Volumes, wanted.Spec.Template.Spec.Volumes) {
				actual.Spec.Template.Spec.Volumes = wanted.Spec.Template.Spec.Volumes
			}
			actual.Spec.Template.Spec.ServiceAccountName = wanted.Spec.Template.Spec.ServiceAccountName
			actual.Spec.Template.Spec.AutomountServiceAccountToken = wanted.Spec.Template.Spec.AutomountServiceAccountToken
		case *corev1.Service:
			wanted := expected.(*corev1.Service)
			actual.Spec.Type = wanted.Spec.Type
			actual.Spec.Selector = wanted.Spec.Selector
			actual.Spec.Ports = wanted.Spec.Ports
		case *networkingv1.Ingress:
			actual.Spec.Rules = expected.(*networkingv1.Ingress).Spec.Rules
		case *corev1.ServiceAccount:
			actual.AutomountServiceAccountToken = expected.(*corev1.ServiceAccount).AutomountServiceAccountToken
		case *networkingv1.NetworkPolicy:
			actual.Spec = expected.(*networkingv1.NetworkPolicy).Spec
		}
		if exists {
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

// objectExists distinguishes an object loaded by CreateOrPatch from the not-yet-created desired
// object. Kubernetes assigns all three metadata fields to persisted objects; checking them inside
// the mutation callback closes the gap between the optional preflight GET and CreateOrPatch's GET.
func objectExists(object client.Object) bool {
	return object.GetResourceVersion() != "" || object.GetUID() != "" || !object.GetCreationTimestamp().Time.IsZero()
}

func ownershipCollisionError(object client.Object, env *platformv1alpha1.DevelopmentEnvironment) error {
	return fmt.Errorf("%s %s/%s already exists but is not controlled by DevelopmentEnvironment %s/%s UID %s; refusing adoption or modification", childKind(object), object.GetNamespace(), object.GetName(), env.Namespace, env.Name, env.UID)
}

func (r *DevelopmentEnvironmentReconciler) verifyOwnedOrAbsent(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, desired client.Object) error {
	existing := desired.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get %s before reconciliation: %w", childKind(desired), err)
	}
	if !metav1.IsControlledBy(existing, env) {
		return ownershipCollisionError(existing, env)
	}
	return nil
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
	case *corev1.ServiceAccount:
		return "ServiceAccount"
	case *networkingv1.NetworkPolicy:
		return "NetworkPolicy"
	default:
		return fmt.Sprintf("%T", object)
	}
}

type readinessObservation struct {
	storageReady    bool
	workloadReady   bool
	serviceExists   bool
	networkReady    bool
	storageMessage  string
	workloadMessage string
	networkMessage  string
}

func (r *DevelopmentEnvironmentReconciler) observeReadiness(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment) (readinessObservation, error) {
	observed := readinessObservation{}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.PVC(env)}, pvc); err != nil && !apierrors.IsNotFound(err) {
		return observed, fmt.Errorf("observe PVC: %w", err)
	}
	requestedStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	availableStorage := pvc.Status.Capacity[corev1.ResourceStorage]
	observed.storageReady = pvc.Status.Phase == corev1.ClaimBound &&
		requestedStorage.Cmp(env.Spec.Storage.Size) >= 0 &&
		availableStorage.Cmp(env.Spec.Storage.Size) >= 0
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Deployment(env)}, deployment); err != nil && !apierrors.IsNotFound(err) {
		return observed, fmt.Errorf("observe Deployment: %w", err)
	}
	observed.workloadReady = deployment.Status.AvailableReplicas >= 1
	service := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Service(env)}, service); err != nil && !apierrors.IsNotFound(err) {
		return observed, fmt.Errorf("observe Service: %w", err)
	}
	// Phase 2 deliberately treats Service readiness as API-object existence. Endpoint health
	// is represented by Deployment availability; this controller does not watch EndpointSlices.
	observed.serviceExists = service.Name != ""
	observed.networkReady = !env.Spec.Network.Enabled
	if env.Spec.Network.Enabled {
		ingress := &networkingv1.Ingress{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: naming.Ingress(env)}, ingress); err != nil && !apierrors.IsNotFound(err) {
			return observed, fmt.Errorf("observe Ingress: %w", err)
		}
		observed.networkReady = ingress.Name != ""
		env.Status.EnvironmentURL = "http://" + env.Spec.Network.Host
	} else {
		env.Status.EnvironmentURL = ""
	}
	observed.storageMessage = "Workspace PVC is not Bound"
	if pvc.Status.Phase == corev1.ClaimBound && !observed.storageReady {
		observed.storageMessage = fmt.Sprintf("Workspace PVC does not yet provide requested storage %s", env.Spec.Storage.Size.String())
	} else if observed.storageReady {
		observed.storageMessage = "Workspace PVC is Bound"
	}
	observed.workloadMessage = "IDE Deployment has no available replicas"
	if observed.workloadReady {
		observed.workloadMessage = "IDE Deployment has the desired available replicas"
	}
	observed.networkMessage = "Managed Ingress has not been created"
	if observed.networkReady {
		observed.networkMessage = "Network configuration is ready"
	}
	return observed, nil
}

func (r *DevelopmentEnvironmentReconciler) setReadiness(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, refsReady bool, refsMessage string) error {
	observed, err := r.observeReadiness(ctx, env)
	if err != nil {
		return err
	}
	statusutil.Set(env, statusutil.StorageReady, condition(observed.storageReady), "PVCObserved", observed.storageMessage)
	statusutil.Set(env, statusutil.NetworkReady, condition(observed.networkReady), "IngressObserved", observed.networkMessage)
	env.Status.ObservedGeneration = env.Generation

	if observed.storageReady && !conditionIsTrue(before, statusutil.StorageReady) {
		r.event(env, corev1.EventTypeNormal, "WorkspacePVCReady", "Workspace PVC is Bound and provides the requested capacity")
	}
	wasSuspended := conditionIsTrue(before, statusutil.Suspended)
	if env.Spec.Suspended {
		r.setSuspendedReadiness(env, before, wasSuspended)
		return nil
	}
	statusutil.Set(env, statusutil.Suspended, metav1.ConditionFalse, "EnvironmentRunning", "Compute is enabled")
	if wasSuspended {
		r.event(env, corev1.EventTypeNormal, "Resumed", "Restored IDE compute using the existing workspace PVC")
	}

	return r.setActiveReadiness(ctx, env, before, observed, refsReady, refsMessage)
}

func (r *DevelopmentEnvironmentReconciler) setSuspendedReadiness(env, before *platformv1alpha1.DevelopmentEnvironment, wasSuspended bool) {
	statusutil.Set(env, statusutil.Suspended, metav1.ConditionTrue, "EnvironmentSuspended", "Compute is intentionally scaled to zero")
	statusutil.Set(env, statusutil.WorkloadReady, metav1.ConditionUnknown, "EnvironmentSuspended", "Workload availability is not evaluated while suspended")
	setSuspendedSourceCondition(env, before)
	env.Status.Phase = platformv1alpha1.PhaseSuspended
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "EnvironmentSuspended", "DevelopmentEnvironment is suspended")
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "EnvironmentSuspended", "No compute is requested while suspended")
	if !wasSuspended {
		r.event(env, corev1.EventTypeNormal, "Suspended", "Scaled IDE compute to zero while preserving storage and network objects")
	}
}

func (r *DevelopmentEnvironmentReconciler) setActiveReadiness(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, observed readinessObservation, refsReady bool, refsMessage string) error {
	sourceStatus, sourceReason, sourceMessage, err := r.observeSource(ctx, env, observed.workloadReady)
	if err != nil {
		return err
	}
	statusutil.Set(env, statusutil.SourceReady, sourceStatus, sourceReason, sourceMessage)
	statusutil.Set(env, statusutil.WorkloadReady, condition(observed.workloadReady), "DeploymentObserved", observed.workloadMessage)
	if sourceStatus == metav1.ConditionTrue && !conditionIsTrue(before, statusutil.SourceReady) && env.Spec.Source != nil {
		r.event(env, corev1.EventTypeNormal, "SourceInitialized", "Git source initialization completed")
	}
	if sourceReason == "SourceInitializationFailed" && !conditionHasReason(before, statusutil.SourceReady, sourceReason) {
		r.event(env, corev1.EventTypeWarning, sourceReason, sourceMessage)
	}

	allReady := observed.storageReady && observed.workloadReady && observed.serviceExists && observed.networkReady && refsReady && sourceStatus == metav1.ConditionTrue
	if allReady {
		env.Status.Phase = platformv1alpha1.PhaseReady
		statusutil.Set(env, statusutil.Ready, metav1.ConditionTrue, "ResourcesReady", "DevelopmentEnvironment is ready")
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Reconciled", "Desired resources are ready")
		if before.Status.Phase != platformv1alpha1.PhaseReady {
			ctrl.LoggerFrom(ctx).Info("DevelopmentEnvironment became Ready", "storageReady", observed.storageReady, "workloadReady", observed.workloadReady, "networkReady", observed.networkReady)
			r.event(env, corev1.EventTypeNormal, "EnvironmentReady", "DevelopmentEnvironment is ready")
		}
		return nil
	}
	message := "Waiting for PVC, Deployment, Service, or Ingress readiness"
	if !refsReady {
		message = refsMessage
	} else if sourceStatus != metav1.ConditionTrue {
		message = sourceMessage
	}
	wasReady := before.Status.Phase == platformv1alpha1.PhaseReady || before.Status.Phase == platformv1alpha1.PhaseDegraded
	if wasReady {
		env.Status.Phase = platformv1alpha1.PhaseDegraded
		statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "HealthLost", message)
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Degraded", message)
		if before.Status.Phase == platformv1alpha1.PhaseReady {
			ctrl.LoggerFrom(ctx).Info("DevelopmentEnvironment left Ready state", "storageReady", observed.storageReady, "workloadReady", observed.workloadReady, "networkReady", observed.networkReady)
		}
	} else {
		env.Status.Phase = platformv1alpha1.PhaseProvisioning
		statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "Provisioning", message)
		statusutil.Set(env, statusutil.Progressing, metav1.ConditionTrue, "Provisioning", message)
	}
	return nil
}

func setSuspendedSourceCondition(env, before *platformv1alpha1.DevelopmentEnvironment) {
	if env.Spec.Source == nil {
		statusutil.Set(env, statusutil.SourceReady, metav1.ConditionTrue, "SourceNotConfigured", "No source initialization is configured")
		return
	}
	if conditionIsTrue(before, statusutil.SourceReady) {
		statusutil.Set(env, statusutil.SourceReady, metav1.ConditionTrue, "SourcePreviouslyInitialized", "Source was initialized before suspension")
		return
	}
	statusutil.Set(env, statusutil.SourceReady, metav1.ConditionUnknown, "InitializationDeferred", "Source initialization is deferred until the environment is resumed")
}

func (r *DevelopmentEnvironmentReconciler) observeSource(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, workloadReady bool) (metav1.ConditionStatus, string, string, error) {
	if env.Spec.Source == nil || env.Spec.Source.Git == nil {
		return metav1.ConditionTrue, "SourceNotConfigured", "No source initialization is configured", nil
	}
	if workloadReady {
		return metav1.ConditionTrue, "SourceInitialized", "Git source initialization completed", nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(env.Namespace), client.MatchingLabels(naming.SelectorLabels(env))); err != nil {
		return metav1.ConditionUnknown, "SourceObservationFailed", "Could not observe Git source initialization", fmt.Errorf("list source initialization Pods: %w", err)
	}
	for i := range pods.Items {
		for j := range pods.Items[i].Status.InitContainerStatuses {
			status := pods.Items[i].Status.InitContainerStatuses[j]
			if status.Name != resources.GitInitContainerName {
				continue
			}
			if status.State.Terminated != nil && status.State.Terminated.ExitCode == 0 {
				return metav1.ConditionTrue, "SourceInitialized", "Git source initialization completed", nil
			}
			failed := status.State.Terminated != nil && status.State.Terminated.ExitCode != 0
			failed = failed || status.LastTerminationState.Terminated != nil && status.LastTerminationState.Terminated.ExitCode != 0
			if failed {
				return metav1.ConditionFalse, "SourceInitializationFailed", "Git source initialization is failing and Kubernetes will retry the init container", nil
			}
		}
	}
	return metav1.ConditionFalse, "SourceInitializing", "Waiting for Git source initialization", nil
}

func conditionIsTrue(env *platformv1alpha1.DevelopmentEnvironment, conditionType string) bool {
	condition := meta.FindStatusCondition(env.Status.Conditions, conditionType)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func conditionHasReason(env *platformv1alpha1.DevelopmentEnvironment, conditionType, reason string) bool {
	condition := meta.FindStatusCondition(env.Status.Conditions, conditionType)
	return condition != nil && condition.Reason == reason
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
func (r *DevelopmentEnvironmentReconciler) recordInvalidSpec(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, reconcileErr error) error {
	transition := before.Status.Phase != platformv1alpha1.PhaseFailed || before.Status.ObservedGeneration != env.Generation || !conditionHasReason(before, statusutil.Ready, "InvalidSpecification")
	env.Status.Phase = platformv1alpha1.PhaseFailed
	env.Status.ObservedGeneration = env.Generation
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, "InvalidSpecification", reconcileErr.Error())
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, "Failed", reconcileErr.Error())
	if err := r.writeStatus(ctx, before, env); err != nil {
		return err
	}
	if transition {
		r.event(env, corev1.EventTypeWarning, "InvalidSpecification", reconcileErr.Error())
	}
	return nil
}

func (r *DevelopmentEnvironmentReconciler) recordStorageFailure(ctx context.Context, env, before *platformv1alpha1.DevelopmentEnvironment, issue *terminalStorageError) error {
	condition := meta.FindStatusCondition(env.Status.Conditions, statusutil.StorageReady)
	transition := env.Status.Phase != issue.phase || env.Status.ObservedGeneration != env.Generation || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != issue.reason || condition.Message != issue.message
	env.Status.Phase = issue.phase
	env.Status.ObservedGeneration = env.Generation
	statusutil.Set(env, statusutil.StorageReady, metav1.ConditionFalse, issue.reason, issue.message)
	statusutil.Set(env, statusutil.Ready, metav1.ConditionFalse, issue.reason, issue.message)
	statusutil.Set(env, statusutil.Progressing, metav1.ConditionFalse, issue.reason, issue.message)
	if err := r.writeStatus(ctx, before, env); err != nil {
		return err
	}
	if transition {
		ctrl.LoggerFrom(ctx).Info(issue.logMessage, issue.logFields...)
		r.event(env, corev1.EventTypeWarning, issue.reason, issue.message)
	}
	return nil
}

func (r *DevelopmentEnvironmentReconciler) event(env *platformv1alpha1.DevelopmentEnvironment, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(env, nil, eventType, reason, "Reconcile", "%s", message)
	}
}

func (r *DevelopmentEnvironmentReconciler) refreshMetrics(ctx context.Context) {
	environments := &platformv1alpha1.DevelopmentEnvironmentList{}
	if err := r.List(ctx, environments); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Could not refresh DevelopmentEnvironment metrics")
		return
	}
	domainmetrics.Observe(environments.Items)
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
	objects := []client.Object{
		resources.DesiredDeployment(env),
		resources.DesiredService(env),
		resources.DesiredIngress(env),
		resources.DesiredNetworkPolicy(env),
		resources.DesiredServiceAccount(env),
	}
	if deletePVC(env) {
		objects = append(objects, resources.DesiredPVC(env))
	}
	for _, object := range objects {
		done, err := r.deleteOwnedAndConfirm(ctx, env, object)
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
func (r *DevelopmentEnvironmentReconciler) deleteOwnedAndConfirm(ctx context.Context, env *platformv1alpha1.DevelopmentEnvironment, object client.Object) (bool, error) {
	key := client.ObjectKeyFromObject(object)
	if err := r.Get(ctx, key, object); apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("get %T before deletion: %w", object, err)
	}
	if !metav1.IsControlledBy(object, env) {
		ctrl.LoggerFrom(ctx).V(1).Info("Skipping unrelated same-name resource during deletion", "kind", childKind(object), "name", object.GetName())
		return true, nil
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
	podToEnvironment := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
		labels := object.GetLabels()
		name := labels[naming.NameLabel]
		if name == "" || labels[naming.EnvironmentUIDLabel] == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: name}}}
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.DevelopmentEnvironment{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&corev1.Pod{}, podToEnvironment).
		Named("developmentenvironment").
		Complete(r)
}
