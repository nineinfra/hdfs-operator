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

	// UpdatingZookeeperReason Reasons for cluster upgrading condition
	UpdatingZookeeperReason = "Updating Zookeeper"
	UpgradeErrorReason      = "Upgrade Error"
)

// MembersStatus is the status of the members of the cluster with both
// ready and unready node membership lists
type MembersStatus struct {
	//+nullable
	Ready []string `json:"ready,omitempty"`
	//+nullable
	Unready []string `json:"unready,omitempty"`
}

// ClusterCondition shows the current condition of a Zookeeper cluster.
// Comply with k8s API conventions
type ClusterCondition struct {
	// Type of Zookeeper cluster condition.
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

// HdfsClusterStatus defines the observed state of HdfsCluster
type HdfsClusterStatus struct {
	// Members is the zookeeper members in the cluster
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

func (zs *HdfsClusterStatus) Init() {
	// Initialise conditions
	conditionTypes := []ClusterConditionType{
		ClusterConditionPodsReady,
		ClusterConditionUpgrading,
		ClusterConditionError,
	}
	for _, conditionType := range conditionTypes {
		if _, condition := zs.GetClusterCondition(conditionType); condition == nil {
			c := newClusterCondition(conditionType, corev1.ConditionFalse, "", "")
			zs.setClusterCondition(*c)
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

func (zs *HdfsClusterStatus) SetPodsReadyConditionTrue() {
	c := newClusterCondition(ClusterConditionPodsReady, corev1.ConditionTrue, "", "")
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) SetPodsReadyConditionFalse() {
	c := newClusterCondition(ClusterConditionPodsReady, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) SetUpgradingConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionUpgrading, corev1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) SetUpgradingConditionFalse() {
	c := newClusterCondition(ClusterConditionUpgrading, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) SetErrorConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionError, corev1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) SetErrorConditionFalse() {
	c := newClusterCondition(ClusterConditionError, corev1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *HdfsClusterStatus) GetClusterCondition(t ClusterConditionType) (int, *ClusterCondition) {
	for i, c := range zs.Conditions {
		if t == c.Type {
			return i, &c
		}
	}
	return -1, nil
}

func (zs *HdfsClusterStatus) setClusterCondition(newCondition ClusterCondition) {
	now := time.Now().Format(time.RFC3339)
	position, existingCondition := zs.GetClusterCondition(newCondition.Type)

	if existingCondition == nil {
		zs.Conditions = append(zs.Conditions, newCondition)
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

	zs.Conditions[position] = *existingCondition
}

func (zs *HdfsClusterStatus) IsClusterInUpgradeFailedState() bool {
	_, errorCondition := zs.GetClusterCondition(ClusterConditionError)
	if errorCondition == nil {
		return false
	}
	if errorCondition.Status == corev1.ConditionTrue && errorCondition.Reason == "UpgradeFailed" {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) IsClusterInUpgradingState() bool {
	_, upgradeCondition := zs.GetClusterCondition(ClusterConditionUpgrading)
	if upgradeCondition == nil {
		return false
	}
	if upgradeCondition.Status == corev1.ConditionTrue {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) IsClusterInReadyState() bool {
	_, readyCondition := zs.GetClusterCondition(ClusterConditionPodsReady)
	if readyCondition != nil && readyCondition.Status == corev1.ConditionTrue {
		return true
	}
	return false
}

func (zs *HdfsClusterStatus) UpdateProgress(reason, updatedReplicas string) {
	if zs.IsClusterInUpgradingState() {
		// Set the upgrade condition reason to be UpgradingZookeeperReason, message to be the upgradedReplicas
		zs.SetUpgradingConditionTrue(reason, updatedReplicas)
	}
}

func (zs *HdfsClusterStatus) GetLastCondition() (lastCondition *ClusterCondition) {
	if zs.IsClusterInUpgradingState() {
		_, lastCondition := zs.GetClusterCondition(ClusterConditionUpgrading)
		return lastCondition
	}
	// nothing to do if we are not upgrading
	return nil
}
