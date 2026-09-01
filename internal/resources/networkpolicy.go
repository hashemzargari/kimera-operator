package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const namespaceNameLabel = "kubernetes.io/metadata.name"

// DesiredNetworkPolicy restricts the exact UID-scoped workload to its HTTP endpoint,
// DNS, and outbound HTTPS. Enforcement depends on the cluster CNI.
func DesiredNetworkPolicy(env *platformv1alpha1.DevelopmentEnvironment) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      naming.NetworkPolicy(env),
			Namespace: env.Namespace,
			Labels:    naming.Labels(env),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: naming.SelectorLabels(env)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{
					NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key:      namespaceNameLabel,
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{env.Namespace, metav1.NamespaceSystem},
					}}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: intstrPtr(IDEPort)}},
			}},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					To: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{namespaceNameLabel: metav1.NamespaceSystem}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &udp, Port: intstrPtr(53)},
						{Protocol: &tcp, Port: intstrPtr(53)},
					},
				},
				{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: intstrPtr(443)}}},
			},
		},
	}
}

func intstrPtr(port int32) *intstr.IntOrString {
	value := intstr.FromInt32(port)
	return &value
}
