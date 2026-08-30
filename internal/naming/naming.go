// Package naming contains stable names and labels used for child resources.
package naming

import platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"

const NameLabel = "platform.kimera.dev/development-environment"

func PVC(env *platformv1alpha1.DevelopmentEnvironment) string        { return env.Name + "-workspace" }
func Deployment(env *platformv1alpha1.DevelopmentEnvironment) string { return env.Name }
func Service(env *platformv1alpha1.DevelopmentEnvironment) string    { return env.Name }
func Ingress(env *platformv1alpha1.DevelopmentEnvironment) string    { return env.Name }
func Labels(env *platformv1alpha1.DevelopmentEnvironment) map[string]string {
	return map[string]string{NameLabel: env.Name, "app.kubernetes.io/name": "development-environment"}
}
