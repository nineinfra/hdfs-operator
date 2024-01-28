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

package controller

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	githuberrors "github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"strings"
	"time"

	hdfsv1 "github.com/nineinfra/hdfs-operator/api/v1"
)

// HdfsClusterReconciler reconciles a HdfsCluster object
type HdfsClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type reconcileFun func(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) error

//+kubebuilder:rbac:groups=hdfs.nineinfra.tech,resources=hdfsclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hdfs.nineinfra.tech,resources=hdfsclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hdfs.nineinfra.tech,resources=hdfsclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the HdfsCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.0/pkg/reconcile
func (r *HdfsClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var cluster hdfsv1.HdfsCluster
	err := r.Get(ctx, req.NamespacedName, &cluster)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Object not found, it could have been deleted")
		} else {
			logger.Error(err, "Error occurred during fetching the object")
		}
		return ctrl.Result{}, err
	}
	requestArray := strings.Split(fmt.Sprint(req), "/")
	requestName := requestArray[1]
	logger.Info(fmt.Sprintf("Reconcile requestName %s,cluster.Name %s", requestName, cluster.Name))
	if requestName == cluster.Name {
		logger.Info("Create or update clusters")
		err = r.reconcileClusters(ctx, &cluster, logger)
		if err != nil {
			logger.Error(err, "Error occurred during create or update clusters")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *HdfsClusterReconciler) reconcileClusterStatus(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {

	if cluster.Status.ClustersStatus == nil {
		cluster.Status.ClustersStatus = make([]hdfsv1.ClusterStatus, 0)
	}
	var roleList = []string{
		HdfsRoleNameNode,
		HdfsRoleDataNode,
		HdfsRoleJournalNode,
		//HdfsRoleHttpFS,
	}
	for _, role := range roleList {
		cluster.Status.Init(role)
		existsPods := &corev1.PodList{}
		labelSelector := labels.SelectorFromSet(ClusterResourceLabels(cluster, role))
		listOps := &client.ListOptions{
			Namespace:     cluster.Namespace,
			LabelSelector: labelSelector,
		}
		err = r.Client.List(context.TODO(), existsPods, listOps)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
		var (
			readyMembers   []string
			unreadyMembers []string
		)
		for _, p := range existsPods.Items {
			ready := true
			for _, c := range p.Status.ContainerStatuses {
				if !c.Ready {
					ready = false
				}
			}
			if ready {
				readyMembers = append(readyMembers, p.Name)
			} else {
				unreadyMembers = append(unreadyMembers, p.Name)
			}
		}
		bFlag := false
		for _, cs := range cluster.Status.ClustersStatus {
			if cs.Type == hdfsv1.ClusterType(role) {
				cs.Members.Ready = readyMembers
				cs.Members.Unready = unreadyMembers
				cs.ReadyReplicas = int32(len(readyMembers))
				if cs.ReadyReplicas == GetReplicas(cluster, role) {
					cluster.Status.SetPodsReadyConditionTrue(role)
				} else {
					cluster.Status.SetPodsReadyConditionFalse(role)
				}
				if cs.CurrentVersion == "" && cluster.Status.IsClusterInReadyState(role) {
					cs.CurrentVersion = cluster.Spec.Image.Tag
				}
				bFlag = true
			}
		}
		if !bFlag {
			cs := hdfsv1.ClusterStatus{
				Type: hdfsv1.ClusterType(role),
				Members: hdfsv1.MembersStatus{
					Ready:   readyMembers,
					Unready: unreadyMembers,
				},
				ReadyReplicas: int32(len(readyMembers)),
			}
			if cs.ReadyReplicas == GetReplicas(cluster, role) {
				cluster.Status.SetPodsReadyConditionTrue(role)
			} else {
				cluster.Status.SetPodsReadyConditionFalse(role)
			}
			if cs.CurrentVersion == "" && cluster.Status.IsClusterInReadyState(role) {
				cs.CurrentVersion = cluster.Spec.Image.Tag
			}
			cluster.Status.ClustersStatus = append(cluster.Status.ClustersStatus, cs)
		}
	}
	return r.Client.Status().Update(context.TODO(), cluster)
}

func (r *HdfsClusterReconciler) reconcileHdfsHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, desiredSvc *corev1.Service, role string, logger logr.Logger) (err error) {
	existsSvc := &corev1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: desiredSvc.Name, Namespace: desiredSvc.Namespace}, existsSvc)
	if err != nil && errors.IsNotFound(err) {
		logger.Info(fmt.Sprintf("Creating a new headless service,Name:%s,Role:%s", desiredSvc.Name, role))
		err = r.Client.Create(context.TODO(), desiredSvc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		logger.Info(fmt.Sprintf("Updating existing headless service,Name:%s,Role:%s", desiredSvc.Name, role))
		existsSvc.Spec.Ports = desiredSvc.Spec.Ports
		existsSvc.Spec.Type = desiredSvc.Spec.Type
		err = r.Client.Update(context.TODO(), existsSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HdfsClusterReconciler) reconcileNamenodeHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructHeadlessService(cluster, HdfsRoleNameNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsHeadlessService(ctx, cluster, desiredSvc, HdfsRoleNameNode, logger)
}

func (r *HdfsClusterReconciler) reconcileDatanodeHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructHeadlessService(cluster, HdfsRoleDataNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsHeadlessService(ctx, cluster, desiredSvc, HdfsRoleDataNode, logger)
}

func (r *HdfsClusterReconciler) reconcileJournalnodeHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructHeadlessService(cluster, HdfsRoleJournalNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsHeadlessService(ctx, cluster, desiredSvc, HdfsRoleJournalNode, logger)
}

func (r *HdfsClusterReconciler) reconcileHttpFSHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructHeadlessService(cluster, HdfsRoleHttpFS)
	if err != nil {
		return err
	}

	return r.reconcileHdfsHeadlessService(ctx, cluster, desiredSvc, HdfsRoleHttpFS, logger)
}

func (r *HdfsClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	for _, fun := range []reconcileFun{
		r.reconcileNamenodeHeadlessService,
		r.reconcileDatanodeHeadlessService,
		r.reconcileJournalnodeHeadlessService,
		//r.reconcileHttpFSHeadlessService,
	} {
		if err := fun(ctx, cluster, logger); err != nil {
			return err
		}
	}

	return nil
}

func (r *HdfsClusterReconciler) reconcileHdfsService(ctx context.Context, cluster *hdfsv1.HdfsCluster, desiredSvc *corev1.Service, role string, logger logr.Logger) (err error) {
	existsSvc := &corev1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: desiredSvc.Name, Namespace: desiredSvc.Namespace}, existsSvc)
	if err != nil && errors.IsNotFound(err) {
		logger.Info(fmt.Sprintf("Creating a new service,Name:%s,Role:%s", desiredSvc.Name, role))
		err = r.Client.Create(context.TODO(), desiredSvc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		logger.Info(fmt.Sprintf("Updating existing service,Name:%s,Role:%s", existsSvc.Name, role))
		existsSvc.Spec.Ports = desiredSvc.Spec.Ports
		existsSvc.Spec.Type = desiredSvc.Spec.Type
		err = r.Client.Update(context.TODO(), existsSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HdfsClusterReconciler) reconcileNamenodeService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructService(cluster, HdfsRoleNameNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsService(ctx, cluster, desiredSvc, HdfsRoleNameNode, logger)
}

func (r *HdfsClusterReconciler) reconcileDatanodeService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructService(cluster, HdfsRoleDataNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsService(ctx, cluster, desiredSvc, HdfsRoleDataNode, logger)
}

func (r *HdfsClusterReconciler) reconcileJournalnodeService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructService(cluster, HdfsRoleJournalNode)
	if err != nil {
		return err
	}

	return r.reconcileHdfsService(ctx, cluster, desiredSvc, HdfsRoleJournalNode, logger)
}

func (r *HdfsClusterReconciler) reconcileHttpFSService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSvc, err := r.constructService(cluster, HdfsRoleHttpFS)
	if err != nil {
		return err
	}

	return r.reconcileHdfsService(ctx, cluster, desiredSvc, HdfsRoleHttpFS, logger)
}

func (r *HdfsClusterReconciler) reconcileService(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	for _, fun := range []reconcileFun{
		r.reconcileNamenodeService,
		r.reconcileDatanodeService,
		r.reconcileJournalnodeService,
		//r.reconcileHttpFSService,
	} {
		if err := fun(ctx, cluster, logger); err != nil {
			return err
		}
	}

	return nil
}

func (r *HdfsClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredCm, err := r.constructConfigMap(cluster)
	if err != nil {
		return err
	}
	existsCm := &corev1.ConfigMap{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: desiredCm.Name, Namespace: desiredCm.Namespace}, existsCm)
	if err != nil && errors.IsNotFound(err) {
		logger.Info(fmt.Sprintf("Creating a new Config Map,Name:%s", desiredCm.Name))
		err = r.Client.Create(context.TODO(), desiredCm)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		logger.Info(fmt.Sprintf("Updating existing Config Map,Name:%s", existsCm.Name))
		existsCm.Data = desiredCm.Data
		err = r.Client.Update(context.TODO(), existsCm)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *HdfsClusterReconciler) reconcileHdfsRole(ctx context.Context, cluster *hdfsv1.HdfsCluster, desiredSts *appsv1.StatefulSet, role string, logger logr.Logger) (err error) {
	existsSts := &appsv1.StatefulSet{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: desiredSts.Name, Namespace: desiredSts.Namespace}, existsSts)
	if err != nil && errors.IsNotFound(err) {
		logger.Info(fmt.Sprintf("Creating a new %s StatefulSet", role))
		err = r.Client.Create(context.TODO(), desiredSts)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Creating a new %s successfully", role))
	return nil
}

func (r *HdfsClusterReconciler) reconcileNamenode(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSts, err := r.constructWorkload(cluster, HdfsRoleNameNode)
	if err != nil {
		return err
	}
	var waitErr error = nil
	if CheckHdfsHA(cluster) {
		//wait journalnode ready
		condition := make(chan struct{})
		go func(clus *hdfsv1.HdfsCluster, waitErr *error) {
			waitSeconds := 0
			for {
				LogInfoInterval(ctx, 5, "Try to get journal node endpoints...")
				jnEndpoints := &corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ClusterResourceName(clus, fmt.Sprintf("-%s", HdfsRoleJournalNode)),
						Namespace: clus.Namespace,
						Labels:    ClusterResourceLabels(clus, HdfsRoleJournalNode),
					},
				}
				existsEndpoints := &corev1.Endpoints{}
				err = r.Get(context.TODO(), types.NamespacedName{Name: jnEndpoints.Name, Namespace: jnEndpoints.Namespace}, existsEndpoints)
				if err != nil && errors.IsNotFound(err) {
					waitSeconds += 1
					if waitSeconds < DefaultMaxWaitSeconds {
						time.Sleep(time.Second)
						continue
					} else {
						logger.Error(err, "wait for journal node service timeout")
						*waitErr = githuberrors.New("wait for journal node service timeout")
						close(condition)
						break
					}
				} else if err != nil {
					logger.Error(err, "get journal node endpoints failed")
				}
				needReplicas := GetReplicas(clus, HdfsRoleJournalNode)
				logger.Info(fmt.Sprintf("subsets:%v,needReplicas:%d\n", existsEndpoints.Subsets, needReplicas))
				if len(existsEndpoints.Subsets) > 0 {
					logger.Info(fmt.Sprintf("addresses in subset 0:%v and len:%d\n",
						existsEndpoints.Subsets[0].Addresses,
						len(existsEndpoints.Subsets[0].Addresses)))
				}
				if len(existsEndpoints.Subsets) == 0 ||
					(len(existsEndpoints.Subsets) > 0 &&
						len(existsEndpoints.Subsets[0].Addresses) < int(needReplicas)) {
					time.Sleep(time.Second)
					continue
				}
				close(condition)
				break
			}
		}(cluster, &waitErr)
		<-condition
	}
	if waitErr != nil {
		return waitErr
	}
	return r.reconcileHdfsRole(ctx, cluster, desiredSts, HdfsRoleNameNode, logger)
}

func (r *HdfsClusterReconciler) reconcileDatanode(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSts, err := r.constructWorkload(cluster, HdfsRoleDataNode)
	if err != nil {
		return err
	}
	return r.reconcileHdfsRole(ctx, cluster, desiredSts, HdfsRoleDataNode, logger)
}

func (r *HdfsClusterReconciler) reconcileJournalnode(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	if CheckHdfsHA(cluster) {
		desiredSts, err := r.constructWorkload(cluster, HdfsRoleJournalNode)
		if err != nil {
			return err
		}
		return r.reconcileHdfsRole(ctx, cluster, desiredSts, HdfsRoleJournalNode, logger)
	}
	return nil
}

func (r *HdfsClusterReconciler) reconcileHttpFS(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	desiredSts, err := r.constructWorkload(cluster, HdfsRoleHttpFS)
	if err != nil {
		return err
	}
	return r.reconcileHdfsRole(ctx, cluster, desiredSts, HdfsRoleHttpFS, logger)
}

func (r *HdfsClusterReconciler) reconcileWorkload(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) (err error) {
	for _, fun := range []reconcileFun{
		r.reconcileJournalnode,
		r.reconcileNamenode,
		r.reconcileDatanode,
		//r.reconcileHttpFS,
	} {
		if err := fun(ctx, cluster, logger); err != nil {
			return err
		}
	}

	return nil
}

func (r *HdfsClusterReconciler) checkClusterInvalid(cluster *hdfsv1.HdfsCluster) error {
	if CheckHdfsHA(cluster) {
		replicas, endpoints, err := GetRefZookeeperInfo(cluster)
		if err != nil {
			return err
		} else {
			if replicas == 0 || endpoints == "" {
				return githuberrors.New("no zookeeper config provided,include reference-zookeeper-replicas and reference-zookeeper-endpoints")
			}
		}
		HDFS_HA = "true"
	}
	return nil
}

func (r *HdfsClusterReconciler) reconcileClusters(ctx context.Context, cluster *hdfsv1.HdfsCluster, logger logr.Logger) error {
	if err := r.checkClusterInvalid(cluster); err != nil {
		return err
	}
	for _, fun := range []reconcileFun{
		r.reconcileConfigMap,
		r.reconcileService,
		r.reconcileHeadlessService,
		r.reconcileWorkload,
		r.reconcileClusterStatus,
	} {
		if err := fun(ctx, cluster, logger); err != nil {
			return err
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HdfsClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfsv1.HdfsCluster{}).
		Complete(r)
}
