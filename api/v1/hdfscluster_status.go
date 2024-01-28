package v1

import (
	corev1 "k8s.io/api/core/v1"
	"time"
)

type ClusterConditionType string

const (
	ClusterConditionPodsReady ClusterConditionType = "PodsReady"
	ClusterConditionUpgrading                      = "Upgrading"
	ClusterConditionError                          = "Error"

	// UpdatingClusterReason Reasons for cluster upgrading condition
	UpdatingClusterReason = "Updating Cluster"
	UpgradeErrorReason    = "Upgrade Error"
)

// MembersStatus is the status of the members of the cluster with both
// ready and unready node membership lists
type MembersStatus struct {
	//+nullable
	Ready []string `json:"ready,omitempty"`
	//+nullable
	Unready []string `json:"unready,omitempty"`
}

// ClusterCondition shows the current condition of a cluster.
// Comply with k8s API conventions
type ClusterCondition struct {
	// Type of cluster condition.
	Type ClusterConditionType `json:"type,omitempty"`

	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status,omitempty"`

	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`

	// A human-readable message indicating details about the transition.
	Message string `json:"message,omitempty"`

	// The last time this condition was updated.
	LastUpdateTime string `json:"lastUpdateTime,omitempty"`

	// Last time the condition transitioned from one status to another.
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type ClusterStatus struct {
	// Name is the name of the cluster
	Name string `json:"name"`

	// Type is the type of the cluster
	Type ClusterType `json:"type"`

	// Members is the cluster members in the cluster
	Members MembersStatus `json:"members,omitempty"`

	// Replicas is the number of desired replicas in the cluster
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of ready replicas in the cluster
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// InternalClientEndpoint is the internal client IP and port
	InternalClientEndpoint string `json:"internalClientEndpoint,omitempty"`

	// ExternalClientEndpoint is the internal client IP and port
	ExternalClientEndpoint string `json:"externalClientEndpoint,omitempty"`

	//MetaRootCreated bool `json:"metaRootCreated,omitempty"`

	// CurrentVersion is the current cluster version
	CurrentVersion string `json:"currentVersion,omitempty"`

	TargetVersion string `json:"targetVersion,omitempty"`

	// Conditions list all the applied conditions
	Conditions []ClusterCondition `json:"conditions,omitempty"`
}

// HdfsClusterStatus defines the observed state of HdfsCluster
type HdfsClusterStatus struct {
	ClustersStatus []ClusterStatus `json:"clustersStatus"`
}

func (zs *HdfsClusterStatus) Init(role string) {
	// Initialise conditions
	conditionTypes := []ClusterConditionType{
		ClusterConditionPodsReady,
		ClusterConditionUpgrading,
		ClusterConditionError,
	}

	for _, conditionType := range conditionTypes {
		if _, condition := zs.GetClusterCondition(conditionType, role); condition == nil {
			c := newClusterCondition(conditionType, corev1.ConditionFalse, "", "")
			zs.setClusterCondition(*c, role)
		}
	}
}

func newClusterCondition(condType ClusterConditionType, status corev1.ConditionStatus, reason, message string) *ClusterCondition {
	return &ClusterCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastUpdateTime:     "",
		LastTransitionTime: "",
	}
}

func (zs *HdfsClusterStatus) SetPodsReadyConditionTrue(role string) {
	c := newClusterCondition(ClusterConditionPodsReady, corev1.ConditionTrue, "", "")
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) SetPodsReadyConditionFalse(role string) {
	c := newClusterCondition(ClusterConditionPodsReady, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) SetUpgradingConditionTrue(reason, message string, role string) {
	c := newClusterCondition(ClusterConditionUpgrading, corev1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) SetUpgradingConditionFalse(role string) {
	c := newClusterCondition(ClusterConditionUpgrading, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) SetErrorConditionTrue(reason, message string, role string) {
	c := newClusterCondition(ClusterConditionError, corev1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) SetErrorConditionFalse(role string) {
	c := newClusterCondition(ClusterConditionError, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c, role)
}

func (zs *HdfsClusterStatus) GetClusterCondition(t ClusterConditionType, role string) (int, *ClusterCondition) {
	for _, cs := range zs.ClustersStatus {
		if cs.Type == ClusterType(role) {
			for i, c := range cs.Conditions {
				if t == c.Type {
					return i, &c
				}
			}
		}
	}
	return -1, nil
}

func (zs *HdfsClusterStatus) setClusterCondition(newCondition ClusterCondition, role string) {
	now := time.Now().Format(time.RFC3339)
	position, existingCondition := zs.GetClusterCondition(newCondition.Type, role)

	if existingCondition == nil {
		for _, cs := range zs.ClustersStatus {
			if cs.Type == ClusterType(role) {
				cs.Conditions = append(cs.Conditions, newCondition)
			}
		}
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = now
		existingCondition.LastUpdateTime = now
	}

	if existingCondition.Reason != newCondition.Reason || existingCondition.Message != newCondition.Message {
		existingCondition.Reason = newCondition.Reason
		existingCondition.Message = newCondition.Message
		existingCondition.LastUpdateTime = now
	}
	for _, cs := range zs.ClustersStatus {
		if cs.Type == ClusterType(role) {
			cs.Conditions[position] = *existingCondition
		}
	}
}

func (zs *HdfsClusterStatus) IsClusterInUpgradeFailedState(role string) bool {
	_, errorCondition := zs.GetClusterCondition(ClusterConditionError, role)
	if errorCondition == nil {
		return false
	}
	if errorCondition.Status == corev1.ConditionTrue && errorCondition.Reason == "UpgradeFailed" {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) IsClusterInUpgradingState(role string) bool {
	_, upgradeCondition := zs.GetClusterCondition(ClusterConditionUpgrading, role)
	if upgradeCondition == nil {
		return false
	}
	if upgradeCondition.Status == corev1.ConditionTrue {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) IsClusterInReadyState(role string) bool {
	_, readyCondition := zs.GetClusterCondition(ClusterConditionPodsReady, role)
	if readyCondition != nil && readyCondition.Status == corev1.ConditionTrue {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) UpdateProgress(reason, updatedReplicas string, role string) {
	if zs.IsClusterInUpgradingState(role) {
		// Set the upgrade condition reason to be UpgradingClusterReason, message to be the upgradedReplicas
		zs.SetUpgradingConditionTrue(reason, updatedReplicas, role)
	}
}

func (zs *HdfsClusterStatus) GetLastCondition(role string) (lastCondition *ClusterCondition) {
	if zs.IsClusterInUpgradingState(role) {
		_, lastCondition := zs.GetClusterCondition(ClusterConditionUpgrading, role)
		return lastCondition
	}
	// nothing to do if we are not upgrading
	return nil
}
