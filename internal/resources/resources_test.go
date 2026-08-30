package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

func testEnvironment() *platformv1alpha1.DevelopmentEnvironment {
	return &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")}, Spec: platformv1alpha1.DevelopmentEnvironmentSpec{Image: "codercom/code-server:latest", Resources: platformv1alpha1.ResourceSpec{CPURequest: resource.MustParse("250m"), CPULimit: resource.MustParse("1"), MemoryRequest: resource.MustParse("512Mi"), MemoryLimit: resource.MustParse("1Gi")}, Storage: platformv1alpha1.StorageSpec{Size: resource.MustParse("2Gi")}, Network: platformv1alpha1.NetworkSpec{Enabled: true, Host: "demo.kimera.local"}}}
}

func TestDesiredDeploymentConfiguresCodeServerSafely(t *testing.T) {
	deployment := DesiredDeployment(testEnvironment())
	container := deployment.Spec.Template.Spec.Containers[0]
	if got, want := container.Ports[0].ContainerPort, IDEPort; got != want {
		t.Fatalf("container port = %d, want %d", got, want)
	}
	if container.ReadinessProbe == nil || container.LivenessProbe == nil {
		t.Fatal("expected code-server health probes")
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("expected privilege escalation to be disabled")
	}
	if deployment.Spec.Template.Spec.SecurityContext.SeccompProfile == nil || deployment.Spec.Template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("expected RuntimeDefault seccomp profile")
	}
}

func TestDesiredServiceAndIngressUseStableHTTPRouting(t *testing.T) {
	env := testEnvironment()
	service := DesiredService(env)
	ingress := DesiredIngress(env)
	if service.Spec.Type != corev1.ServiceTypeClusterIP || service.Spec.Ports[0].Port != 80 {
		t.Fatal("expected ClusterIP service on port 80")
	}
	if ingress.Spec.Rules[0].Host != env.Spec.Network.Host || ingress.Spec.Rules[0].IngressRuleValue.HTTP.Paths[0].Backend.Service.Name != service.Name {
		t.Fatal("expected ingress to route to the generated service")
	}
}

func TestDesiredPVCOmitsStorageClassWhenNotSpecified(t *testing.T) {
	env := testEnvironment()
	pvc := DesiredPVC(env)
	if pvc.Spec.StorageClassName != nil {
		t.Fatal("expected unspecified StorageClass to use the cluster default")
	}
	storage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if storage.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatal("unexpected PVC storage request")
	}
	if pvc.Labels["platform.kimera.dev/environment-uid"] != string(env.UID) || pvc.Annotations["platform.kimera.dev/storage-identity"] != string(env.UID) {
		t.Fatal("PVC is missing immutable environment provenance")
	}
}
