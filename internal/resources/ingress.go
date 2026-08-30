package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DesiredIngress returns the optional HTTP ingress.
func DesiredIngress(env *platformv1alpha1.DevelopmentEnvironment) *networkingv1.Ingress {
	pathType := networkingv1.PathTypePrefix
	return &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: naming.Ingress(env), Namespace: env.Namespace, Labels: naming.Labels(env)}, Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{Host: env.Spec.Network.Host, IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{Path: "/", PathType: &pathType, Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: naming.Service(env), Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}}}}}
}
