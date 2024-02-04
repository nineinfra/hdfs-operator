/*
Copyright 2024 nineinfra.

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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ClusterType string // Different types of clusters.
const (
	NameNodeClusterType    ClusterType = "namenode"
	DataNodeClusterType    ClusterType = "datanode"
	JournalNodeClusterType ClusterType = "journalnode"
	HttpFSClusterType      ClusterType = "httpfs"
	ZookeeperClusterType   ClusterType = "zookeeper"
)

const (
	// RefClusterZKReplicasKey  zookeeper replicas.must be odd and >= 3.
	RefClusterZKReplicasKey string = "reference-zookeeper-replicas"
	// RefClusterZKEndpointsKey zookeeper endpoints.such as 10.224.1.218:2181,10.224.2.222:2181,10.224.3.253:2181.
	RefClusterZKEndpointsKey string = "reference-zookeeper-endpoints"
)

type ResourceConfig struct {
	// The replicas of the cluster workload.Default value is 1
	// +optional
	Replicas int32 `json:"replicas"`
	// num of the disks. default value is 1
	// +optional
	Disks int32 `json:"disks"`
	// the storage class. default value is nineinfra-default
	// +optional
	StorageClass string `json:"storageClass"`
	// The resource requirements of the cluster workload.
	// +optional
	ResourceRequirements corev1.ResourceRequirements `json:"resourceRequirements"`
}

type ImageConfig struct {
	Repository string `json:"repository"`
	// Image tag. Usually the vesion of the kyuubi, default: `latest`.
	// +optional
	Tag string `json:"tag,omitempty"`
	// Image pull policy. One of `Always, Never, IfNotPresent`, default: `Always`.
	// +kubebuilder:default:=Always
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +optional
	PullPolicy string `json:"pullPolicy,omitempty"`
	// Secrets for image pull.
	// +optional
	PullSecrets string `json:"pullSecret,omitempty"`
}
type Cluster struct {
	// Version. version of the cluster.
	Version string `json:"version"`
	// +kubebuilder:validation:Enum={namenode,datanode,journalnode,zookeeper}
	Type ClusterType `json:"type"`
	// Name. name of the cluster.
	// +optional
	Name string `json:"name"`
	// Image. image config of the cluster.
	// +optional
	Image ImageConfig `json:"image"`
	// Resource. resouce config of the cluster.
	// +optional
	Resource ResourceConfig `json:"resource,omitempty"`
	// Conf. k/v configs for the cluster.
	// +optional
	Conf map[string]string `json:"conf,omitempty"`
}

// HdfsClusterSpec defines the desired state of HdfsCluster
type HdfsClusterSpec struct {
	// Version. version of the cluster.
	Version string `json:"version"`
	// Image. image config of the cluster.
	Image ImageConfig `json:"image"`
	// Resource. resource config of the datanodecluster if the datanode cluster not in clusters.
	// +optional
	Resource ResourceConfig `json:"resource,omitempty"`
	// Conf. k/v configs for the cluster.
	// +optional
	Conf map[string]string `json:"conf,omitempty"`
	// K8sConf. k/v configs for the cluster in k8s.such as the cluster domain
	// +optional
	K8sConf map[string]string `json:"k8sConf,omitempty"`
	// ClusterRefs. k/v configs for the cluster in k8s.such as the cluster domain
	// +optional
	ClusterRefs []Cluster `json:"clusterRefs,omitempty"`
	// Clusters. namenode,datanode and journalnode cluster.
	// +optional
	Clusters []Cluster `json:"clusters"`
}

// +genclient
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// HdfsCluster is the Schema for the hdfsclusters API
type HdfsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HdfsClusterSpec   `json:"spec,omitempty"`
	Status HdfsClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HdfsClusterList contains a list of HdfsCluster
type HdfsClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HdfsCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HdfsCluster{}, &HdfsClusterList{})
}
