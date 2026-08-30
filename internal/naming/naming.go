// Package naming contains stable names and labels used for child resources.
package naming

import (
	"strings"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

const (
	NameLabel                         = "platform.kimera.dev/development-environment"
	ManagedByLabel                    = "platform.kimera.dev/managed-by"
	EnvironmentUIDLabel               = "platform.kimera.dev/environment-uid"
	OriginalEnvironmentNameAnnotation = "platform.kimera.dev/original-environment-name"
	OriginalEnvironmentUIDAnnotation  = "platform.kimera.dev/original-environment-uid"
	StorageIdentityAnnotation         = "platform.kimera.dev/storage-identity"
	ManagedByValue                    = "kimera"
	maxObjectNameLength               = 253
)

// StorageIdentity is the immutable identity of one DevelopmentEnvironment instance.
func StorageIdentity(env *platformv1alpha1.DevelopmentEnvironment) string { return string(env.UID) }

// PVC returns a deterministic claim name tied to the CR UID, not merely its reusable name.
func PVC(env *platformv1alpha1.DevelopmentEnvironment) string {
	identity := StorageIdentity(env)
	if identity == "" {
		identity = "missing-uid"
	}
	suffix := "-" + identity + "-workspace"
	baseLength := maxObjectNameLength - len(suffix)
	base := env.Name
	if len(base) > baseLength {
		base = strings.TrimRight(base[:baseLength], "-.")
	}
	if base == "" {
		base = "environment"
	}
	return base + suffix
}

func Deployment(env *platformv1alpha1.DevelopmentEnvironment) string { return env.Name }
func Service(env *platformv1alpha1.DevelopmentEnvironment) string    { return env.Name }
func Ingress(env *platformv1alpha1.DevelopmentEnvironment) string    { return env.Name }
func Labels(env *platformv1alpha1.DevelopmentEnvironment) map[string]string {
	return map[string]string{NameLabel: env.Name, "app.kubernetes.io/name": "development-environment"}
}

// PVCLabels identify KIMERA-managed storage and the immutable environment instance it belongs to.
func PVCLabels(env *platformv1alpha1.DevelopmentEnvironment) map[string]string {
	labels := Labels(env)
	labels[ManagedByLabel] = ManagedByValue
	labels[EnvironmentUIDLabel] = StorageIdentity(env)
	return labels
}

// PVCAnnotations preserve non-sensitive provenance after the parent is deleted.
func PVCAnnotations(env *platformv1alpha1.DevelopmentEnvironment) map[string]string {
	return map[string]string{
		OriginalEnvironmentNameAnnotation: env.Name,
		OriginalEnvironmentUIDAnnotation:  StorageIdentity(env),
		StorageIdentityAnnotation:         StorageIdentity(env),
	}
}
