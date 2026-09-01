package metrics

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

func TestObservePublishesAggregateDomainState(t *testing.T) {
	Observe([]platformv1alpha1.DevelopmentEnvironment{
		{Status: platformv1alpha1.DevelopmentEnvironmentStatus{Phase: platformv1alpha1.PhaseReady}},
		{Spec: platformv1alpha1.DevelopmentEnvironmentSpec{Suspended: true}, Status: platformv1alpha1.DevelopmentEnvironmentStatus{Phase: platformv1alpha1.PhaseSuspended}},
		{ObjectMeta: metav1.ObjectMeta{Name: "provisioning"}, Status: platformv1alpha1.DevelopmentEnvironmentStatus{Phase: platformv1alpha1.PhaseProvisioning}},
	})

	if got := environmentCount.Load(); got != 3 {
		t.Fatalf("environment count = %d, want 3", got)
	}
	if got := readyCount.Load(); got != 1 {
		t.Fatalf("ready count = %d, want 1", got)
	}
	if got := suspendedCount.Load(); got != 1 {
		t.Fatalf("suspended count = %d, want 1", got)
	}
}
