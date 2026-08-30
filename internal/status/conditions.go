// Package status provides helpers for conventional DevelopmentEnvironment conditions.
package status

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	Ready         = "Ready"
	StorageReady  = "StorageReady"
	WorkloadReady = "WorkloadReady"
	NetworkReady  = "NetworkReady"
	Progressing   = "Progressing"
)

// Set records a condition with the environment generation as its observed generation.
func Set(env *platformv1alpha1.DevelopmentEnvironment, typ string, value metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{Type: typ, Status: value, ObservedGeneration: env.Generation, Reason: reason, Message: message})
}
