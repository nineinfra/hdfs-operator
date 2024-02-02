package controller

import (
	"context"
	"github.com/go-logr/logr"
	hdfsv1 "github.com/nineinfra/hdfs-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestCheckHdfsHA(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckHdfsHA(tt.args.cluster); got != tt.want {
				t.Errorf("CheckHdfsHA() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterResourceLabels(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClusterResourceLabels(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClusterResourceLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterResourceName(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		suffixs []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClusterResourceName(tt.args.cluster, tt.args.suffixs...); got != tt.want {
				t.Errorf("ClusterResourceName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultEnvVars(t *testing.T) {
	type args struct {
		role string
	}
	tests := []struct {
		name string
		args args
		want []corev1.EnvVar
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultEnvVars(tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultEnvVars() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultXml2Map(t *testing.T) {
	tests := []struct {
		name    string
		want    map[string]string
		want1   map[string]string
		want2   map[string]string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, err := DefaultXml2Map()
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultXml2Map() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultXml2Map() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DefaultXml2Map() got1 = %v, want %v", got1, tt.want1)
			}

			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("DefaultXml2Map() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestFillJNEnvs(t *testing.T) {
	type args struct {
		qjournal string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "testwithNormalArgs",
			args: args{
				qjournal: "qjournal://hdfscluster-sample-hdfs-journalnode-0.hdfscluster-sample-hdfs-journalnode.dwh.svc.cluster.local:8485;hdfscluster-sample-hdfs-journalnode-1.hdfscluster-sample-hdfs-journalnode.dwh.svc.cluster.local:8485;hdfscluster-sample-hdfs-journalnode-2.hdfscluster-sample-hdfs-journalnode.dwh.svc.cluster.local:8485/nineinfra",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FillJNEnvs(tt.args.qjournal)
		})
	}
}

func TestFillZKEnvs(t *testing.T) {
	type args struct {
		zkEndpoints string
		zkReplicas  int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FillZKEnvs(tt.args.zkEndpoints, tt.args.zkReplicas)
		})
	}
}

func TestGetClusterDomain(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetClusterDomain(tt.args.cluster); got != tt.want {
				t.Errorf("GetClusterDomain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRefZookeeperInfo(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name    string
		args    args
		want    int
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := GetRefZookeeperInfo(tt.args.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRefZookeeperInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetRefZookeeperInfo() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetRefZookeeperInfo() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetReplicas(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetReplicas(tt.args.cluster, tt.args.role); got != tt.want {
				t.Errorf("GetReplicas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetStorageClassName(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStorageClassName(tt.args.cluster); got != tt.want {
				t.Errorf("GetStorageClassName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_Reconcile(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx context.Context
		req controllerruntime.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    controllerruntime.Result
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.Reconcile(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Reconcile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reconcile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_SetupWithManager(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		mgr controllerruntime.Manager
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.SetupWithManager(tt.args.mgr); (err != nil) != tt.wantErr {
				t.Errorf("SetupWithManager() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_checkClusterInvalid(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.checkClusterInvalid(tt.args.cluster); (err != nil) != tt.wantErr {
				t.Errorf("checkClusterInvalid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructConfigMap(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *corev1.ConfigMap
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.constructConfigMap(tt.args.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructConfigMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructConfigMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructContainerPorts(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []corev1.ContainerPort
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructContainerPorts(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructContainerPorts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *corev1.Service
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.constructHeadlessService(tt.args.cluster, tt.args.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructHeadlessService() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructLivenessProbe(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *corev1.Probe
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructLivenessProbe(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructLivenessProbe() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructPVCs(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []corev1.PersistentVolumeClaim
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.constructPVCs(tt.args.cluster, tt.args.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructPVCs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructPVCs() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructPodSpec(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   corev1.PodSpec
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructPodSpec(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructPodSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructReadinessProbe(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *corev1.Probe
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructReadinessProbe(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructReadinessProbe() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *corev1.Service
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.constructService(tt.args.cluster, tt.args.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructService() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructServicePorts(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []corev1.ServicePort
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructServicePorts(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructServicePorts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructVolumeMounts(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []corev1.VolumeMount
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructVolumeMounts(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructVolumeMounts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructVolumes(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []corev1.Volume
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if got := r.constructVolumes(tt.args.cluster, tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructVolumes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_constructStatefulSet(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *appsv1.StatefulSet
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.constructStatefulSet(tt.args.cluster, tt.args.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructStatefulSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructStatefulSet() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_getStorageRequests(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *resource.Quantity
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			got, err := r.getStorageRequests(tt.args.cluster, tt.args.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("getStorageRequests() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getStorageRequests() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileClusterStatus(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileClusterStatus(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileClusterStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileClusters(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileClusters(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileClusters() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileConfigMap(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileConfigMap(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileConfigMap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileDatanode(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileDatanode(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileDatanode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileDatanodeHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileDatanodeHeadlessService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileDatanodeHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileDatanodeService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileDatanodeService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileDatanodeService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHdfsHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx        context.Context
		cluster    *hdfsv1.HdfsCluster
		desiredSvc *corev1.Service
		role       string
		logger     logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHdfsHeadlessService(tt.args.ctx, tt.args.cluster, tt.args.desiredSvc, tt.args.role, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHdfsHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHdfsRole(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx        context.Context
		cluster    *hdfsv1.HdfsCluster
		desiredSts *appsv1.StatefulSet
		role       string
		logger     logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHdfsRole(tt.args.ctx, tt.args.cluster, tt.args.desiredSts, tt.args.role, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHdfsRole() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHdfsService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx        context.Context
		cluster    *hdfsv1.HdfsCluster
		desiredSvc *corev1.Service
		role       string
		logger     logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHdfsService(tt.args.ctx, tt.args.cluster, tt.args.desiredSvc, tt.args.role, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHdfsService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHeadlessService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHttpFS(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHttpFS(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHttpFS() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileHttpFSService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileHttpFSService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileHttpFSService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileJournalnode(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileJournalnode(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileJournalnode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileJournalnodeHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileJournalnodeHeadlessService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileJournalnodeHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileJournalnodeService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileJournalnodeService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileJournalnodeService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileNamenode(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileNamenode(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileNamenode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileNamenodeHeadlessService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileNamenodeHeadlessService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileNamenodeHeadlessService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileNamenodeService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileNamenodeService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileNamenodeService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileService(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileService(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileService() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHdfsClusterReconciler_reconcileWorkload(t *testing.T) {
	type fields struct {
		Client client.Client
		Scheme *runtime.Scheme
	}
	type args struct {
		ctx     context.Context
		cluster *hdfsv1.HdfsCluster
		logger  logr.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &HdfsClusterReconciler{
				Client: tt.fields.Client,
				Scheme: tt.fields.Scheme,
			}
			if err := r.reconcileWorkload(tt.args.ctx, tt.args.cluster, tt.args.logger); (err != nil) != tt.wantErr {
				t.Errorf("reconcileWorkload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogInfoInterval(t *testing.T) {
	type args struct {
		ctx      context.Context
		interval int
		msg      string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LogInfoInterval(tt.args.ctx, tt.args.interval, tt.args.msg)
		})
	}
}

func Test_capacityPerVolume(t *testing.T) {
	type args struct {
		capacity string
	}
	tests := []struct {
		name    string
		args    args
		want    *resource.Quantity
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := capacityPerVolume(tt.args.capacity)
			if (err != nil) != tt.wantErr {
				t.Errorf("capacityPerVolume() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("capacityPerVolume() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_constructConfig(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		want2   string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, err := constructConfig(tt.args.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("constructConfig() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("constructConfig() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("constructConfig() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_getConfigValue(t *testing.T) {
	type args struct {
		conf                  map[string]string
		coreSiteDefaultConf   map[string]string
		hdfsSiteDefaultConf   map[string]string
		httpfsSiteDefaultConf map[string]string
		coreSite              map[string]string
		hdfsSite              map[string]string
		httpfsSite            map[string]string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getConfigValue(tt.args.conf, tt.args.coreSiteDefaultConf, tt.args.hdfsSiteDefaultConf, tt.args.httpfsSiteDefaultConf, tt.args.coreSite, tt.args.hdfsSite, tt.args.httpfsSite)
		})
	}
}

func Test_getDiskNum(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDiskNum(tt.args.cluster, tt.args.role); got != tt.want {
				t.Errorf("getDiskNum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getHttpPortByRole(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		role    string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHttpPortByRole(tt.args.cluster, tt.args.role); got != tt.want {
				t.Errorf("getHttpPortByRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getImageConfig(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
	}
	tests := []struct {
		name string
		args args
		want hdfsv1.ImageConfig
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getImageConfig(tt.args.cluster); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getImageConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getPortByName(t *testing.T) {
	type args struct {
		cluster *hdfsv1.HdfsCluster
		name    string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getPortByName(tt.args.cluster, tt.args.name); got != tt.want {
				t.Errorf("getPortByName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_int32Ptr(t *testing.T) {
	type args struct {
		i int32
	}
	tests := []struct {
		name string
		args args
		want *int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int32Ptr(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("int32Ptr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeCoreSite(t *testing.T) {
	type args struct {
		cluster  *hdfsv1.HdfsCluster
		coreSite map[string]string
		ha       bool
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			makeCoreSite(tt.args.cluster, tt.args.coreSite, tt.args.ha)
		})
	}
}

func Test_makeHdfsSite(t *testing.T) {
	type args struct {
		cluster  *hdfsv1.HdfsCluster
		hdfsSite map[string]string
		ha       bool
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			makeHdfsSite(tt.args.cluster, tt.args.hdfsSite, tt.args.ha)
		})
	}
}

func Test_map2String(t *testing.T) {
	type args struct {
		kv map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := map2String(tt.args.kv); got != tt.want {
				t.Errorf("map2String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_map2Xml(t *testing.T) {
	type args struct {
		properties map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := map2Xml(tt.args.properties); got != tt.want {
				t.Errorf("map2Xml() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volumeRequest(t *testing.T) {
	type args struct {
		q resource.Quantity
	}
	tests := []struct {
		name string
		args args
		want corev1.ResourceList
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := volumeRequest(tt.args.q); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("volumeRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
