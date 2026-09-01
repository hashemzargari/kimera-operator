package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const IDEPort int32 = 8080

// DesiredDeployment returns the single-replica IDE workload.
func DesiredDeployment(env *platformv1alpha1.DevelopmentEnvironment) *appsv1.Deployment {
	metadataLabels := naming.Labels(env)
	selectorLabels := naming.SelectorLabels(env)
	replicas := int32(1)
	noEscalation := false
	terminationGracePeriod := int64(30)
	enableServiceLinks := true
	envFrom := []corev1.EnvFromSource{}
	if env.Spec.ConfigMapRef != nil {
		envFrom = append(envFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: *env.Spec.ConfigMapRef}})
	}
	for _, ref := range env.Spec.SecretRefs {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref}})
	}
	container := corev1.Container{Name: "ide", Image: env.Spec.Image, ImagePullPolicy: corev1.PullIfNotPresent, Args: []string{"--bind-addr", "0.0.0.0:8080", "--auth", "none"}, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: IDEPort, Protocol: corev1.ProtocolTCP}}, EnvFrom: envFrom, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPURequest, corev1.ResourceMemory: env.Spec.Resources.MemoryRequest}, Limits: corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPULimit, corev1.ResourceMemory: env.Spec.Resources.MemoryLimit}}, SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &noEscalation, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}, ReadinessProbe: httpProbe("/healthz", 10, 5), LivenessProbe: httpProbe("/healthz", 30, 10), VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/home/coder/project"}}, TerminationMessagePath: corev1.TerminationMessagePathDefault, TerminationMessagePolicy: corev1.TerminationMessageReadFile}
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: naming.Deployment(env), Namespace: env.Namespace, Labels: metadataLabels}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: selectorLabels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: metadataLabels}, Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}, Containers: []corev1.Container{container}, Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: naming.PVC(env)}}}}, RestartPolicy: corev1.RestartPolicyAlways, TerminationGracePeriodSeconds: &terminationGracePeriod, DNSPolicy: corev1.DNSClusterFirst, SchedulerName: corev1.DefaultSchedulerName, EnableServiceLinks: &enableServiceLinks}}}}
}

func httpProbe(path string, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(IDEPort), Scheme: corev1.URISchemeHTTP}}, InitialDelaySeconds: initialDelay, PeriodSeconds: period, TimeoutSeconds: 3, SuccessThreshold: 1, FailureThreshold: 6}
}
