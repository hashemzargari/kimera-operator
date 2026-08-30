package naming

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

func TestPVCIdentityIsStablePerUIDAndIsolatedAcrossUIDs(t *testing.T) {
	first := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "demo", UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")}}
	second := first.DeepCopy()
	second.UID = types.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	if PVC(first) != PVC(first.DeepCopy()) {
		t.Fatal("the same DevelopmentEnvironment UID did not produce a stable PVC name")
	}
	if PVC(first) == PVC(second) {
		t.Fatal("different DevelopmentEnvironment UIDs produced the same PVC name")
	}
}

func TestPVCLongNameRespectsKubernetesDNSSubdomainLimit(t *testing.T) {
	env := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{
		Name: strings.Repeat("a", 253),
		UID:  types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}}

	if got := len(PVC(env)); got > maxObjectNameLength {
		t.Fatalf("PVC name length = %d, want at most %d", got, maxObjectNameLength)
	}
}
