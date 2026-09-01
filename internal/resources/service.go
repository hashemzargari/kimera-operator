package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DesiredService returns the stable in-cluster HTTP endpoint for the IDE.
func DesiredService(env *platformv1alpha1.DevelopmentEnvironment) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: naming.Service(env), Namespace: env.Namespace, Labels: naming.Labels(env)}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: naming.SelectorLabels(env), Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(IDEPort)}}}}
}
