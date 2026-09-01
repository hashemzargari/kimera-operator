package resources

import (
	"fmt"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	IDEPort            int32 = 8080
	workspaceVolume          = "workspace"
	workspaceMountPath       = "/home/coder/project"
	// GitInitContainerName identifies the source initialization status in observed Pods.
	GitInitContainerName    = "git-init"
	gitInitImage            = "alpine/git:v2.54.0"
	gitInitializationScript = `
set -eu
marker="${WORKSPACE}/.kimera-initialized"
if [ -e "${marker}" ]; then
  exit 0
fi
if [ -n "$(find "${WORKSPACE}" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  echo "Workspace already contains data; leaving it unchanged"
  touch "${marker}"
  exit 0
fi
clone_dir="/tmp/repository"
rm -rf "${clone_dir}"
git clone --no-checkout "${REPOSITORY_URL}" "${clone_dir}"
commit="$(git -C "${clone_dir}" rev-parse --verify --end-of-options "${REVISION}^{commit}")"
git -C "${clone_dir}" checkout --detach "${commit}"
if [ -n "${SUB_PATH}" ]; then
  source_dir="${clone_dir}/${SUB_PATH}"
  if [ ! -d "${source_dir}" ]; then
    echo "Configured Git subPath does not exist" >&2
    exit 1
  fi
  cp -a "${source_dir}"/. "${WORKSPACE}"/
else
  cp -a "${clone_dir}"/. "${WORKSPACE}"/
fi
touch "${marker}"
`
)

// DesiredDeployment returns the persistent IDE workload at the requested suspend state.
func DesiredDeployment(env *platformv1alpha1.DevelopmentEnvironment) *appsv1.Deployment {
	metadataLabels := naming.Labels(env)
	selectorLabels := naming.SelectorLabels(env)
	replicas := int32(1)
	if env.Spec.Suspended {
		replicas = 0
	}
	terminationGracePeriod := int64(30)
	enableServiceLinks := false
	automountToken := false
	runAsNonRoot := true
	noEscalation := false
	runAsUser := int64(1000)
	runAsGroup := int64(1000)
	fsGroup := int64(1000)
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch

	podSpec := corev1.PodSpec{
		ServiceAccountName:           naming.ServiceAccount(env),
		AutomountServiceAccountToken: &automountToken,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:        &runAsNonRoot,
			RunAsUser:           &runAsUser,
			RunAsGroup:          &runAsGroup,
			FSGroup:             &fsGroup,
			FSGroupChangePolicy: &fsGroupChangePolicy,
			SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers:                    []corev1.Container{ideContainer(env, noEscalation, runAsNonRoot, runAsUser, runAsGroup)},
		Volumes:                       []corev1.Volume{{Name: workspaceVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: naming.PVC(env)}}}},
		RestartPolicy:                 corev1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: &terminationGracePeriod,
		DNSPolicy:                     corev1.DNSClusterFirst,
		SchedulerName:                 corev1.DefaultSchedulerName,
		EnableServiceLinks:            &enableServiceLinks,
	}
	if env.Spec.Source != nil && env.Spec.Source.Git != nil {
		podSpec.InitContainers = []corev1.Container{gitInitContainer(env.Spec.Source.Git, noEscalation, runAsNonRoot, runAsUser, runAsGroup)}
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{Name: "git-tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: naming.Deployment(env), Namespace: env.Namespace, Labels: metadataLabels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: metadataLabels},
				Spec:       podSpec,
			},
		},
	}
}

func ideContainer(env *platformv1alpha1.DevelopmentEnvironment, noEscalation, runAsNonRoot bool, runAsUser, runAsGroup int64) corev1.Container {
	envFrom := []corev1.EnvFromSource{}
	if env.Spec.ConfigMapRef != nil {
		envFrom = append(envFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: *env.Spec.ConfigMapRef}})
	}
	for _, ref := range env.Spec.SecretRefs {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref}})
	}
	return corev1.Container{
		Name:            "ide",
		Image:           env.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            []string{"--bind-addr", fmt.Sprintf("0.0.0.0:%d", IDEPort), "--auth", "none"},
		Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: IDEPort, Protocol: corev1.ProtocolTCP}},
		EnvFrom:         envFrom,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPURequest, corev1.ResourceMemory: env.Spec.Resources.MemoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: env.Spec.Resources.CPULimit, corev1.ResourceMemory: env.Spec.Resources.MemoryLimit},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &noEscalation,
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		ReadinessProbe:           httpProbe("/healthz", 10, 5),
		LivenessProbe:            httpProbe("/healthz", 30, 10),
		VolumeMounts:             []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMountPath}},
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}
}

func gitInitContainer(source *platformv1alpha1.GitSourceSpec, noEscalation, runAsNonRoot bool, runAsUser, runAsGroup int64) corev1.Container {
	revision := source.Revision
	if revision == "" {
		revision = "main"
	}
	readOnlyRootFilesystem := true
	return corev1.Container{
		Name:            GitInitContainerName,
		Image:           gitInitImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/sh", "-ceu"},
		Args:            []string{gitInitializationScript},
		Env: []corev1.EnvVar{
			{Name: "REPOSITORY_URL", Value: source.URL},
			{Name: "REVISION", Value: revision},
			{Name: "SUB_PATH", Value: source.SubPath},
			{Name: "WORKSPACE", Value: workspaceMountPath},
			{Name: "HOME", Value: "/tmp"},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &noEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: workspaceVolume, MountPath: workspaceMountPath},
			{Name: "git-tmp", MountPath: "/tmp"},
		},
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}
}

func httpProbe(path string, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(IDEPort), Scheme: corev1.URISchemeHTTP}},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
		TimeoutSeconds:      3,
		SuccessThreshold:    1,
		FailureThreshold:    6,
	}
}
