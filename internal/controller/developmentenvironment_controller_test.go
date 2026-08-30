/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

var _ = Describe("DevelopmentEnvironment Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		developmentenvironment := &platformv1alpha1.DevelopmentEnvironment{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind DevelopmentEnvironment")
			err := k8sClient.Get(ctx, typeNamespacedName, developmentenvironment)
			if err != nil && errors.IsNotFound(err) {
				environment := &platformv1alpha1.DevelopmentEnvironment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: platformv1alpha1.DevelopmentEnvironmentSpec{
						Image:     "example.invalid/code-server:v1",
						Resources: platformv1alpha1.ResourceSpec{CPURequest: resource.MustParse("100m"), CPULimit: resource.MustParse("1"), MemoryRequest: resource.MustParse("128Mi"), MemoryLimit: resource.MustParse("512Mi")},
						Storage:   platformv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), RetentionPolicy: "Retain"},
					},
				}
				Expect(k8sClient.Create(ctx, environment)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &platformv1alpha1.DevelopmentEnvironment{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance DevelopmentEnvironment")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &DevelopmentEnvironmentReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// The first pass adds the finalizer; the second creates child resources.
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-workspace", Namespace: resourceNamespace}, &corev1.PersistentVolumeClaim{})).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &corev1.Service{})).To(Succeed())
		})
	})
})

var _ = Describe("DevelopmentEnvironment API validation", func() {
	It("validates optional network fields without evaluating absent fields", func() {
		tests := []struct {
			name    string
			network map[string]any
			valid   bool
		}{
			{name: "network-omitted", valid: true},
			{name: "empty-network", network: map[string]any{}, valid: true},
			{name: "network-disabled", network: map[string]any{"enabled": false}, valid: true},
			{name: "enabled-without-host", network: map[string]any{"enabled": true}, valid: false},
			{name: "enabled-with-empty-host", network: map[string]any{"enabled": true, "host": ""}, valid: false},
			{name: "enabled-with-host", network: map[string]any{"enabled": true, "host": "demo.kimera.local"}, valid: true},
		}

		for _, test := range tests {
			object := validationObject(test.name, test.network)
			err := k8sClient.Create(ctx, object)
			if test.valid {
				Expect(err).NotTo(HaveOccurred(), test.name)
				Expect(k8sClient.Delete(ctx, object)).To(Succeed())
			} else {
				Expect(errors.IsInvalid(err)).To(BeTrue(), test.name)
			}
		}
	})
})

func validationObject(name string, network map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"image":     "codercom/code-server:latest",
		"resources": map[string]any{"cpuRequest": "250m", "cpuLimit": "1", "memoryRequest": "512Mi", "memoryLimit": "1Gi"},
		"storage":   map[string]any{"size": "2Gi"},
	}
	if network != nil {
		spec["network"] = network
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "platform.kimera.dev/v1alpha1",
		"kind":       "DevelopmentEnvironment",
		"metadata":   map[string]any{"name": name, "namespace": "default"},
		"spec":       spec,
	}}
}
