// Package resources builds deterministic child objects for DevelopmentEnvironments.
package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesiredPVC returns the workspace claim. It has no owner reference initially: Retain claims
// must never be garbage-collected; the controller adds ownership only for Delete retention.
func DesiredPVC(env *platformv1alpha1.DevelopmentEnvironment) *corev1.PersistentVolumeClaim {
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: naming.PVC(env), Namespace: env.Namespace, Labels: naming.PVCLabels(env), Annotations: naming.PVCAnnotations(env)}}
	claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	claim.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: env.Spec.Storage.Size}
	if env.Spec.Storage.StorageClassName != "" {
		claim.Spec.StorageClassName = &env.Spec.Storage.StorageClassName
	}
	return claim
}
