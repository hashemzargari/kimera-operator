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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DevelopmentEnvironmentPhase describes the lifecycle state reported by the operator.
type DevelopmentEnvironmentPhase string

const (
	// PhasePending means the resource has not yet been provisioned.
	PhasePending DevelopmentEnvironmentPhase = "Pending"
	// PhaseProvisioning means Kubernetes resources are being created or updated.
	PhaseProvisioning DevelopmentEnvironmentPhase = "Provisioning"
	// PhaseReady means all required resources are available.
	PhaseReady DevelopmentEnvironmentPhase = "Ready"
	// PhaseDegraded means an existing environment cannot currently satisfy its specification.
	PhaseDegraded DevelopmentEnvironmentPhase = "Degraded"
	// PhaseFailed means the specification has an unrecoverable error.
	PhaseFailed DevelopmentEnvironmentPhase = "Failed"
)

// ResourceSpec defines CPU and memory requests and limits for the IDE container.
type ResourceSpec struct {
	// CPURequest is the requested CPU capacity, for example "500m".
	// +kubebuilder:validation:Required
	CPURequest resource.Quantity `json:"cpuRequest"`
	// CPULimit is the maximum CPU capacity, for example "2".
	// +kubebuilder:validation:Required
	CPULimit resource.Quantity `json:"cpuLimit"`
	// MemoryRequest is the requested memory capacity, for example "1Gi".
	// +kubebuilder:validation:Required
	MemoryRequest resource.Quantity `json:"memoryRequest"`
	// MemoryLimit is the maximum memory capacity, for example "4Gi".
	// +kubebuilder:validation:Required
	MemoryLimit resource.Quantity `json:"memoryLimit"`
}

// StorageSpec defines the persistent volume claim used by the environment.
type StorageSpec struct {
	// Size is the requested persistent storage capacity, for example "10Gi".
	// +kubebuilder:validation:Required
	Size resource.Quantity `json:"size"`
	// StorageClassName selects a StorageClass. Empty uses the cluster default.
	// +kubebuilder:validation:MaxLength=253
	StorageClassName string `json:"storageClassName,omitempty"`
	// RetentionPolicy controls whether the PVC is deleted with this resource.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	RetentionPolicy string `json:"retentionPolicy,omitempty"`
}

// NetworkSpec configures optional HTTP ingress for the environment.
type NetworkSpec struct {
	// Enabled creates an Ingress when true.
	Enabled bool `json:"enabled,omitempty"`
	// Host is the DNS host served by the Ingress.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?)(\\.([a-z0-9]([-a-z0-9]*[a-z0-9])?))*$`
	Host string `json:"host,omitempty"`
}

// DevelopmentEnvironmentSpec defines the desired state of a DevelopmentEnvironment.
// +kubebuilder:validation:XValidation:rule="!self.network.enabled || self.network.host != ”",message="network.host is required when network.enabled is true"
type DevelopmentEnvironmentSpec struct {
	// Image is the container image for the IDE workload.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// Resources configures compute resources for the IDE container.
	Resources ResourceSpec `json:"resources"`
	// Storage configures the environment's persistent workspace.
	Storage StorageSpec `json:"storage"`
	// Network configures optional ingress. It defaults to disabled.
	Network NetworkSpec `json:"network,omitempty"`
	// ConfigMapRef, when set, is injected into the container with envFrom.
	ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`
	// SecretRefs are injected into the container with envFrom. Values are never stored in status or logs.
	// +listType=map
	// +listMapKey=name
	SecretRefs []corev1.LocalObjectReference `json:"secretRefs,omitempty"`
}

// DevelopmentEnvironmentStatus defines the observed state of DevelopmentEnvironment.
type DevelopmentEnvironmentStatus struct {
	// Phase is the high-level lifecycle state reported by the controller.
	Phase DevelopmentEnvironmentPhase `json:"phase,omitempty"`

	// EnvironmentURL is the HTTPS URL served by the managed Ingress, when enabled.
	EnvironmentURL string `json:"environmentURL,omitempty"`

	// ObservedGeneration is the most recent generation processed by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions describe readiness of storage, workload, network, and overall progress.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DevelopmentEnvironment is the Schema for a managed, persistent IDE environment.
type DevelopmentEnvironment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// Spec defines the desired environment.
	Spec DevelopmentEnvironmentSpec `json:"spec"`

	// Status defines the observed environment state.
	// +optional
	Status DevelopmentEnvironmentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DevelopmentEnvironmentList contains a list of DevelopmentEnvironment resources.
type DevelopmentEnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DevelopmentEnvironment `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DevelopmentEnvironment{}, &DevelopmentEnvironmentList{})
		return nil
	})
}
