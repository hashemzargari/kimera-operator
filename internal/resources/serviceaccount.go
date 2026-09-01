package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesiredServiceAccount returns the unprivileged identity used only by the IDE Pod.
func DesiredServiceAccount(env *platformv1alpha1.DevelopmentEnvironment) *corev1.ServiceAccount {
	automountToken := false
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      naming.ServiceAccount(env),
			Namespace: env.Namespace,
			Labels:    naming.Labels(env),
		},
		AutomountServiceAccountToken: &automountToken,
	}
}
