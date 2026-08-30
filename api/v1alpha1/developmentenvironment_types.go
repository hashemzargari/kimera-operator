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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	PhasePending      = "Pending"
	PhaseProvisioning = "Provisioning"
	PhaseReady        = "Ready"
	PhaseDegraded     = "Degraded"
	PhaseFailed       = "Failed"
)

type ResourceSpec struct {
	CPURequest    string `json:"cpuRequest"`
	CPULimit      string `json:"cpuLimit"`
	MemoryRequest string `json:"memoryRequest"`
	MemoryLimit   string `json:"memoryLimit"`
}

type StorageSpec struct {
	Size             string `json:"size"`
	StorageClassName string `json:"storageClassName,omitempty"`

	RetentionPolicy string `json:"retentionPolicy,omitempty"`
}

type NetworkSpec struct {
	Enabled bool   `json:"enabled,omitempty"`
	Host    string `json:"host,omitempty"`
}

// DevelopmentEnvironmentSpec defines the desired state of DevelopmentEnvironment
type DevelopmentEnvironmentSpec struct {
	Image string `json:"image"`

	Resources ResourceSpec `json:"resources"`

	Storage StorageSpec `json:"storage"`

	Network NetworkSpec `json:"network,omitempty"`

	ConfigMapRef *string `json:"configMapRef,omitempty"`

	SecretRefs []string `json:"secretRefs,omitempty"`
}

// DevelopmentEnvironmentStatus defines the observed state of DevelopmentEnvironment.
type DevelopmentEnvironmentStatus struct {
	Phase string `json:"phase,omitempty"`

	EnvironmentURL string `json:"environmentURL,omitempty"`

	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// conditions represent the current state of the DevelopmentEnvironment resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DevelopmentEnvironment is the Schema for the developmentenvironments API
type DevelopmentEnvironment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DevelopmentEnvironment
	// +required
	Spec DevelopmentEnvironmentSpec `json:"spec"`

	// status defines the observed state of DevelopmentEnvironment
	// +optional
	Status DevelopmentEnvironmentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DevelopmentEnvironmentList contains a list of DevelopmentEnvironment
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
