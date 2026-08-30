package resources

import (
	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const IDEPort int32 = 8080

// DesiredDeployment returns the single-replica IDE workload.
func DesiredDeployment(env *platformv1alpha1.DevelopmentEnvironment) *appsv1.Deployment {
	labels := naming.Labels(env)
	replicas := int32(1)
	nonRoot := true
	user := int64(1000)
	noEscalation := false
	envFrom := []corev1.EnvFromSource{}
	if env.Spec.ConfigMapRef != nil {
		envFrom = append(envFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: *env.Spec.ConfigMapRef}})
	}
	for _, ref := range env.Spec.SecretRefs {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref}})
	}
	container := corev1.Container{Name: "ide", Image: env.Spec.Image, ImagePullPolicy: corev1.PullIfNotPresent, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: IDEPort}}, EnvFrom: envFrom, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPURequest, corev1.ResourceMemory: env.Spec.Resources.MemoryRequest}, Limits: corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPULimit, corev1.ResourceMemory: env.Spec.Resources.MemoryLimit}}, SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &noEscalation, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}, VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/home/coder/project"}}}
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: naming.Deployment(env), Namespace: env.Namespace, Labels: labels}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &nonRoot, RunAsUser: &user, RunAsGroup: &user, FSGroup: &user}, Containers: []corev1.Container{container}, Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: naming.PVC(env)}}}}}}}}
}
