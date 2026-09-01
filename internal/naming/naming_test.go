package naming

import (
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

const testEnvironmentName = "demo"

func TestSelectorLabelsIncludeImmutableEnvironmentIdentity(t *testing.T) {
	envA := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: testEnvironmentName, UID: types.UID("uid-a")}}
	envB := envA.DeepCopy()
	envB.UID = types.UID("uid-b")

	wantA := map[string]string{NameLabel: testEnvironmentName, EnvironmentUIDLabel: "uid-a"}
	if got := SelectorLabels(envA); !reflect.DeepEqual(got, wantA) {
		t.Fatalf("selector labels = %#v, want %#v", got, wantA)
	}
	if reflect.DeepEqual(SelectorLabels(envA), SelectorLabels(envB)) {
		t.Fatal("same-name environments with different UIDs must have different selectors")
	}
}

func TestEnvironmentAndStorageIdentity(t *testing.T) {
	env := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{UID: types.UID("uid-a")}}
	if got, want := DevelopmentEnvironmentIdentity(env), string(env.UID); got != want {
		t.Fatalf("DevelopmentEnvironment identity = %q, want %q", got, want)
	}
	if got, want := StorageIdentity(env), DevelopmentEnvironmentIdentity(env); got != want {
		t.Fatalf("storage identity = %q, want environment identity %q", got, want)
	}
}

func TestSelectorLabelsRejectMissingUID(t *testing.T) {
	env := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: testEnvironmentName}}
	if got := SelectorLabels(env); got != nil {
		t.Fatalf("selector labels without UID = %#v, want nil", got)
	}
}

func TestPVCIdentityIsStablePerUIDAndIsolatedAcrossUIDs(t *testing.T) {
	first := &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: testEnvironmentName, UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")}}
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
