package resources

import (
	"maps"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
)

func testEnvironment() *platformv1alpha1.DevelopmentEnvironment {
	return &platformv1alpha1.DevelopmentEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")}, Spec: platformv1alpha1.DevelopmentEnvironmentSpec{Image: "ghcr.io/coder/code-server:4.104.2", Resources: platformv1alpha1.ResourceSpec{CPURequest: resource.MustParse("250m"), CPULimit: resource.MustParse("1"), MemoryRequest: resource.MustParse("512Mi"), MemoryLimit: resource.MustParse("1Gi")}, Storage: platformv1alpha1.StorageSpec{Size: resource.MustParse("2Gi")}, Network: platformv1alpha1.NetworkSpec{Enabled: true, Host: "demo.kimera.local"}}}
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
	if deployment.Spec.Template.Spec.SecurityContext.RunAsUser == nil || *deployment.Spec.Template.Spec.SecurityContext.RunAsUser != 1000 || deployment.Spec.Template.Spec.SecurityContext.FSGroup == nil || *deployment.Spec.Template.Spec.SecurityContext.FSGroup != 1000 {
		t.Fatal("expected code-server-compatible UID and fsGroup 1000")
	}
	if deployment.Spec.Template.Spec.ServiceAccountName != naming.ServiceAccount(testEnvironment()) || deployment.Spec.Template.Spec.AutomountServiceAccountToken == nil || *deployment.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatal("expected the dedicated ServiceAccount with token automount disabled")
	}
	if deployment.Spec.Template.Spec.EnableServiceLinks == nil || *deployment.Spec.Template.Spec.EnableServiceLinks {
		t.Fatal("expected Kubernetes Service environment-variable injection to be disabled")
	}
}

func TestDesiredDeploymentImplementsSuspendWithoutChangingStorage(t *testing.T) {
	env := testEnvironment()
	running := DesiredDeployment(env)
	env.Spec.Suspended = true
	suspended := DesiredDeployment(env)
	if running.Spec.Replicas == nil || *running.Spec.Replicas != 1 || suspended.Spec.Replicas == nil || *suspended.Spec.Replicas != 0 {
		t.Fatalf("replicas running=%v suspended=%v, want 1 and 0", running.Spec.Replicas, suspended.Spec.Replicas)
	}
	if running.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != suspended.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName {
		t.Fatal("suspension changed the workspace PVC")
	}
	if !reflect.DeepEqual(running.Spec.Selector, suspended.Spec.Selector) {
		t.Fatal("suspension changed the immutable workload selector")
	}
}

func TestDesiredServiceAccountIsTokenless(t *testing.T) {
	env := testEnvironment()
	account := DesiredServiceAccount(env)
	if account.Name != naming.ServiceAccount(env) || account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatal("expected deterministic ServiceAccount with token automount disabled")
	}
}

func TestDesiredNetworkPolicySelectsOnlyCurrentUID(t *testing.T) {
	env := testEnvironment()
	policy := DesiredNetworkPolicy(env)
	if !reflect.DeepEqual(policy.Spec.PodSelector.MatchLabels, naming.SelectorLabels(env)) {
		t.Fatalf("NetworkPolicy selector = %#v, want %#v", policy.Spec.PodSelector.MatchLabels, naming.SelectorLabels(env))
	}
	selector := labels.SelectorFromSet(policy.Spec.PodSelector.MatchLabels)
	differentUID := labels.Set(naming.Labels(env))
	differentUID[naming.EnvironmentUIDLabel] = "different-uid"
	if selector.Matches(differentUID) {
		t.Fatal("NetworkPolicy selected a different environment UID")
	}
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Egress) != 2 {
		t.Fatalf("unexpected NetworkPolicy rules: ingress=%d egress=%d", len(policy.Spec.Ingress), len(policy.Spec.Egress))
	}
	if len(policy.Spec.PolicyTypes) != 2 || policy.Spec.PolicyTypes[0] != "Ingress" || policy.Spec.PolicyTypes[1] != "Egress" {
		t.Fatalf("unexpected NetworkPolicy types: %#v", policy.Spec.PolicyTypes)
	}
	ingress := policy.Spec.Ingress[0]
	if len(ingress.Ports) != 1 || ingress.Ports[0].Port == nil || ingress.Ports[0].Port.IntVal != IDEPort || ingress.Ports[0].Protocol == nil || *ingress.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("unexpected IDE ingress ports: %#v", ingress.Ports)
	}
	if len(ingress.From) != 1 || ingress.From[0].NamespaceSelector == nil || len(ingress.From[0].NamespaceSelector.MatchExpressions) != 1 {
		t.Fatalf("unexpected IDE ingress peers: %#v", ingress.From)
	}
	allowedNamespaces := ingress.From[0].NamespaceSelector.MatchExpressions[0].Values
	if !reflect.DeepEqual(allowedNamespaces, []string{env.Namespace, metav1.NamespaceSystem}) {
		t.Fatalf("allowed ingress namespaces = %#v", allowedNamespaces)
	}
	dns := policy.Spec.Egress[0]
	if len(dns.Ports) != 2 || dns.Ports[0].Port == nil || dns.Ports[0].Port.IntVal != 53 || dns.Ports[0].Protocol == nil || *dns.Ports[0].Protocol != corev1.ProtocolUDP || dns.Ports[1].Protocol == nil || *dns.Ports[1].Protocol != corev1.ProtocolTCP {
		t.Fatalf("unexpected DNS egress rule: %#v", dns)
	}
	https := policy.Spec.Egress[1]
	if len(https.Ports) != 1 || https.Ports[0].Port == nil || https.Ports[0].Port.IntVal != 443 || https.Ports[0].Protocol == nil || *https.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("unexpected HTTPS egress rule: %#v", https)
	}
}

func TestGitSourceInitializationIsIdempotentAndSeparatedFromIDE(t *testing.T) {
	env := testEnvironment()
	withoutSource := DesiredDeployment(env)
	if len(withoutSource.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatal("source-free environment unexpectedly has an init container")
	}
	env.Spec.Source = &platformv1alpha1.SourceSpec{Git: &platformv1alpha1.GitSourceSpec{URL: "https://github.com/example/repository.git", Revision: "feature/demo", SubPath: "examples/go"}}
	deployment := DesiredDeployment(env)
	if len(deployment.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("init container count = %d, want 1", len(deployment.Spec.Template.Spec.InitContainers))
	}
	initContainer := deployment.Spec.Template.Spec.InitContainers[0]
	if initContainer.Name != GitInitContainerName || initContainer.Image != gitInitImage || strings.Contains(initContainer.Args[0], env.Spec.Source.Git.URL) || strings.Contains(initContainer.Args[0], env.Spec.Source.Git.Revision) {
		t.Fatal("Git values must be passed separately to the pinned static init script")
	}
	values := map[string]string{}
	for _, variable := range initContainer.Env {
		values[variable.Name] = variable.Value
	}
	if values["REPOSITORY_URL"] != env.Spec.Source.Git.URL || values["REVISION"] != env.Spec.Source.Git.Revision || values["SUB_PATH"] != env.Spec.Source.Git.SubPath {
		t.Fatalf("unexpected Git init environment: %#v", values)
	}
	if !strings.Contains(initContainer.Args[0], ".kimera-initialized") || !strings.Contains(initContainer.Args[0], "Workspace already contains data; leaving it unchanged") {
		t.Fatal("Git init script is missing non-destructive idempotency semantics")
	}
	if initContainer.SecurityContext == nil || initContainer.SecurityContext.ReadOnlyRootFilesystem == nil || !*initContainer.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("Git init container must use a read-only root filesystem")
	}
	if !containerMounts(initContainer, workspaceVolume) || !containerMounts(deployment.Spec.Template.Spec.Containers[0], workspaceVolume) {
		t.Fatal("Git init and IDE containers must mount the same workspace PVC")
	}
	for _, variable := range deployment.Spec.Template.Spec.Containers[0].Env {
		if variable.Name == "REPOSITORY_URL" {
			t.Fatal("Git bootstrap configuration leaked into the IDE container")
		}
	}
}

func containerMounts(container corev1.Container, volumeName string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == volumeName {
			return true
		}
	}
	return false
}

func TestWorkloadSelectorsAreUIDScoped(t *testing.T) {
	env := testEnvironment()
	deployment := DesiredDeployment(env)
	service := DesiredService(env)
	want := naming.SelectorLabels(env)
	if !reflect.DeepEqual(deployment.Spec.Selector.MatchLabels, want) {
		t.Fatalf("Deployment selector = %#v, want %#v", deployment.Spec.Selector.MatchLabels, want)
	}
	if !reflect.DeepEqual(service.Spec.Selector, want) {
		t.Fatalf("Service selector = %#v, want %#v", service.Spec.Selector, want)
	}
	for key, value := range want {
		if deployment.Spec.Template.Labels[key] != value {
			t.Fatalf("PodTemplate label %q = %q, want %q", key, deployment.Spec.Template.Labels[key], value)
		}
	}
	if deployment.Spec.Template.Labels[naming.AppNameLabel] != naming.AppNameValue {
		t.Fatal("PodTemplate should retain descriptive app metadata")
	}

	selector := labels.SelectorFromSet(service.Spec.Selector)
	podA := labels.Set(deployment.Spec.Template.Labels)
	podB := maps.Clone(podA)
	podB[naming.EnvironmentUIDLabel] = "another-environment-uid"
	missingUID := maps.Clone(podA)
	delete(missingUID, naming.EnvironmentUIDLabel)
	if !selector.Matches(podA) {
		t.Fatal("selector did not match a pod from the current environment instance")
	}
	if selector.Matches(podB) {
		t.Fatal("selector matched a same-name pod from a different environment UID")
	}
	if selector.Matches(missingUID) {
		t.Fatal("selector matched a pod missing the environment UID")
	}
}

func TestDesiredServiceAndIngressUseStableHTTPRouting(t *testing.T) {
	env := testEnvironment()
	service := DesiredService(env)
	ingress := DesiredIngress(env)
	if service.Spec.Type != corev1.ServiceTypeClusterIP || service.Spec.Ports[0].Port != 80 {
		t.Fatal("expected ClusterIP service on port 80")
	}
	if ingress.Spec.Rules[0].Host != env.Spec.Network.Host || ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Name != service.Name {
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
