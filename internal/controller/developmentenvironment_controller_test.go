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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
	"github.com/hashemzargari/kimera-operator/internal/naming"
)

const (
	networkEnabledFieldKey = "enabled"
	gitFieldKey            = "git"
	urlFieldKey            = "url"
	sourceFieldKey         = "source"
	revisionFieldKey       = "revision"
	subPathFieldKey        = "subPath"
)

var _ = Describe("DevelopmentEnvironment Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = unitNamespace
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
						Storage:   platformv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), RetentionPolicy: unitRetainPolicy},
						Network:   platformv1alpha1.NetworkSpec{Enabled: true, Host: "test-resource.example.com"},
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
			current := &platformv1alpha1.DevelopmentEnvironment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, current)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: naming.PVC(current), Namespace: resourceNamespace}, &corev1.PersistentVolumeClaim{})).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &corev1.Service{})).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &networkingv1.Ingress{})).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &corev1.ServiceAccount{})).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &networkingv1.NetworkPolicy{})).To(Succeed())

			recorder := &stablePatchCountingClient{Client: k8sClient}
			controllerReconciler.Client = recorder
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(recorder.deploymentPatches).To(Equal(0))
			Expect(recorder.servicePatches).To(Equal(0))
			Expect(recorder.ingressPatches).To(Equal(0))
			Expect(recorder.serviceAccountPatches).To(Equal(0))
			Expect(recorder.networkPolicyPatches).To(Equal(0))
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
			{name: "network-disabled", network: map[string]any{networkEnabledFieldKey: false}, valid: true},
			{name: "enabled-without-host", network: map[string]any{networkEnabledFieldKey: true}, valid: false},
			{name: "enabled-with-empty-host", network: map[string]any{networkEnabledFieldKey: true, "host": ""}, valid: false},
			{name: "enabled-with-host", network: map[string]any{networkEnabledFieldKey: true, "host": "demo.kimera.local"}, valid: true},
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

	It("defaults Phase 2 fields and validates Git source shape", func() {
		valid := validationObject("phase2-defaults", nil)
		valid.Object["spec"].(map[string]any)[sourceFieldKey] = map[string]any{gitFieldKey: map[string]any{urlFieldKey: "https://github.com/example/repository.git"}}
		Expect(k8sClient.Create(ctx, valid)).To(Succeed())
		created := &unstructured.Unstructured{}
		created.SetAPIVersion(valid.GetAPIVersion())
		created.SetKind(valid.GetKind())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(valid), created)).To(Succeed())
		suspended, found, err := unstructured.NestedBool(created.Object, "spec", "suspended")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(suspended).To(BeFalse())
		revision, found, err := unstructured.NestedString(created.Object, "spec", sourceFieldKey, gitFieldKey, revisionFieldKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(revision).To(Equal("main"))
		Expect(k8sClient.Delete(ctx, valid)).To(Succeed())

		for _, source := range []map[string]any{
			{},
			{gitFieldKey: map[string]any{urlFieldKey: "http://example.com/repository.git"}},
			{gitFieldKey: map[string]any{urlFieldKey: "https://github.com/example/repository.git", "revision": "-unsafe"}},
		} {
			invalid := validationObject("invalid-source-"+fmt.Sprint(len(source)), nil)
			invalid.Object["metadata"].(map[string]any)["generateName"] = invalid.GetName() + "-"
			delete(invalid.Object["metadata"].(map[string]any), "name")
			invalid.Object["spec"].(map[string]any)[sourceFieldKey] = source
			Expect(errors.IsInvalid(k8sClient.Create(ctx, invalid))).To(BeTrue())
		}
	})

	It("keeps source immutable while allowing unrelated spec updates", func() {
		withoutSource := validationObject("source-immutable-absent", nil)
		Expect(k8sClient.Create(ctx, withoutSource)).To(Succeed())
		current := &unstructured.Unstructured{}
		current.SetAPIVersion(withoutSource.GetAPIVersion())
		current.SetKind(withoutSource.GetKind())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(withoutSource), current)).To(Succeed())
		current.Object["spec"].(map[string]any)[sourceFieldKey] = gitSource("https://github.com/example/added.git", "main", "")
		Expect(errors.IsInvalid(k8sClient.Update(ctx, current))).To(BeTrue())
		Expect(k8sClient.Delete(ctx, withoutSource)).To(Succeed())

		withSource := validationObject("source-immutable-present", nil)
		withSource.Object["spec"].(map[string]any)[sourceFieldKey] = gitSource("https://github.com/example/original.git", "main", "src")
		Expect(k8sClient.Create(ctx, withSource)).To(Succeed())

		mutations := []struct {
			name   string
			mutate func(map[string]any)
		}{
			{name: "url", mutate: func(git map[string]any) { git[urlFieldKey] = "https://github.com/example/changed.git" }},
			{name: revisionFieldKey, mutate: func(git map[string]any) { git[revisionFieldKey] = "develop" }},
			{name: subPathFieldKey, mutate: func(git map[string]any) { git[subPathFieldKey] = "cmd" }},
			{name: "removal", mutate: nil},
		}
		for _, mutation := range mutations {
			By("rejecting source " + mutation.name)
			candidate := &unstructured.Unstructured{}
			candidate.SetAPIVersion(withSource.GetAPIVersion())
			candidate.SetKind(withSource.GetKind())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(withSource), candidate)).To(Succeed())
			spec := candidate.Object["spec"].(map[string]any)
			if mutation.mutate == nil {
				delete(spec, sourceFieldKey)
			} else {
				git := spec[sourceFieldKey].(map[string]any)[gitFieldKey].(map[string]any)
				mutation.mutate(git)
			}
			Expect(errors.IsInvalid(k8sClient.Update(ctx, candidate))).To(BeTrue())
		}

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(withSource), current)).To(Succeed())
		current.Object["spec"].(map[string]any)["resources"].(map[string]any)["cpuRequest"] = "300m"
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(withSource), current)).To(Succeed())
		current.Object["spec"].(map[string]any)["suspended"] = true
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
	})
})

func gitSource(repositoryURL, revision, subPath string) map[string]any {
	return map[string]any{gitFieldKey: map[string]any{urlFieldKey: repositoryURL, revisionFieldKey: revision, subPathFieldKey: subPath}}
}

func validationObject(name string, network map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"image":     testIDEImage,
		"resources": map[string]any{"cpuRequest": "250m", "cpuLimit": "1", "memoryRequest": "512Mi", "memoryLimit": "1Gi"},
		"storage":   map[string]any{"size": "2Gi"},
	}
	if network != nil {
		spec["network"] = network
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "platform.kimera.dev/v1alpha1",
		"kind":       "DevelopmentEnvironment",
		"metadata":   map[string]any{"name": name, "namespace": unitNamespace},
		"spec":       spec,
	}}
}

type stablePatchCountingClient struct {
	client.Client
	deploymentPatches     int
	servicePatches        int
	ingressPatches        int
	serviceAccountPatches int
	networkPolicyPatches  int
}

func (c *stablePatchCountingClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	switch object.(type) {
	case *appsv1.Deployment:
		c.deploymentPatches++
	case *corev1.Service:
		c.servicePatches++
	case *networkingv1.Ingress:
		c.ingressPatches++
	case *corev1.ServiceAccount:
		c.serviceAccountPatches++
	case *networkingv1.NetworkPolicy:
		c.networkPolicyPatches++
	}
	return c.Client.Patch(ctx, object, patch, options...)
}
