package controller

import (
	"fmt"
	hdfsv1 "github.com/nineinfra/hdfs-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"net/url"
	ctrl "sigs.k8s.io/controller-runtime"
	"strconv"
	"strings"
)

func volumeRequest(q resource.Quantity) corev1.ResourceList {
	m := make(corev1.ResourceList, 1)
	m[corev1.ResourceStorage] = q
	return m
}

func capacityPerVolume(capacity string) (*resource.Quantity, error) {
	totalQuantity, err := resource.ParseQuantity(capacity)
	if err != nil {
		return nil, err
	}
	return resource.NewQuantity(totalQuantity.Value(), totalQuantity.Format), nil
}

func getReplicas(cluster *hdfsv1.HdfsCluster, role string) int32 {
	if cluster.Spec.Clusters != nil {
		for _, v := range cluster.Spec.Clusters {
			if v.Type == hdfsv1.ClusterType(role) {
				if v.Resource.Replicas != 0 {
					return v.Resource.Replicas
				}
			}
		}
	}
	switch role {
	case HdfsRoleDataNode:
		if cluster.Spec.Resource.Replicas != 0 {
			return cluster.Spec.Resource.Replicas
		}
	case HdfsRoleNameNode:
		return DefaultReplicas
	case HdfsRoleHttpFS:
		return DefaultReplicas
	}
	return DefaultQuorumReplicas
}

func getDiskNum(cluster *hdfsv1.HdfsCluster, role string) int32 {
	if role == HdfsRoleDataNode {
		if cluster.Spec.Clusters != nil {
			for _, v := range cluster.Spec.Clusters {
				if v.Type == hdfsv1.ClusterType(role) {
					if v.Resource.Disks != 0 {
						return v.Resource.Disks
					}
				}
			}
		}
	}

	return DefaultDiskNum
}

func getConfigValue(cluster *hdfsv1.HdfsCluster, key string, value string) string {
	if cluster.Spec.Conf != nil {
		if value, ok := cluster.Spec.Conf[key]; ok {
			return value
		}
	}
	return value
}

func getPortByName(cluster *hdfsv1.HdfsCluster, name string) int32 {
	var namedPort int32
	for k, v := range DefaultNamedPort {
		if strings.EqualFold(name, k) {
			namedPort = v
			if cluster.Spec.Conf != nil {
				if value, ok := cluster.Spec.Conf[DefaultNamedPortConfKey[name]]; ok {
					u, err := url.Parse("//" + value)
					if err == nil {
						tmpPort, _ := strconv.Atoi(u.Port())
						namedPort = int32(tmpPort)
					}
				}
			}
		}
	}
	return namedPort
}

func getHttpPortByRole(cluster *hdfsv1.HdfsCluster, role string) int32 {
	rolePrefix := HDFSRole2Prefix[role]
	name := fmt.Sprintf("%s%s", rolePrefix, "-http")
	return getPortByName(cluster, name)
}

func makeCoreSite(cluster *hdfsv1.HdfsCluster, coreSite map[string]string, ha bool) {
	if ha {
		quorumZookeepers := make([]string, 0)
		for i := 0; i < DefaultQuorumReplicas; i++ {
			quorumZookeeper := fmt.Sprintf("%s-zookeeper-%d.%s-zookeeper.%s.svc.%s:%d",
				cluster.Name,
				i,
				cluster.Name,
				cluster.Namespace,
				GetClusterDomain(cluster),
				2181)
			quorumZookeepers = append(quorumZookeepers, quorumZookeeper)
		}
		qZookeeper := strings.Join(quorumZookeepers, ",")
		coreSite["fs.defaultFS"] = fmt.Sprintf("hdfs://%s", DefaultNameService)
		coreSite["ha.zookeeper.quorum"] = qZookeeper
	} else {
		coreSite["fs.defaultFS"] = fmt.Sprintf(
			"hdfs://%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
			cluster.Name,
			0,
			cluster.Name,
			cluster.Namespace,
			GetClusterDomain(cluster),
			getPortByName(cluster, "nn-rpc"))
	}

}

func makeHdfsSite(cluster *hdfsv1.HdfsCluster, hdfsSite map[string]string, ha bool) {
	if ha {
		quorumJournals := make([]string, 0)
		for i := 0; i < DefaultQuorumReplicas; i++ {
			quorumJournal := fmt.Sprintf("%s-journalnode-%d.%s-journalnode.%s.svc.%s:%d",
				cluster.Name,
				i,
				cluster.Name,
				cluster.Namespace,
				GetClusterDomain(cluster),
				getPortByName(cluster, "jn-rpc"))
			quorumJournals = append(quorumJournals, quorumJournal)
		}
		qjournal := strings.Join(quorumJournals, ";")

		hdfsSite["dfs.nameservices"] = DefaultNameService
		hdfsSite[fmt.Sprintf("dfs.ha.namenodes.%s", DefaultNameService)] = "nn0,nn1"
		for i := 0; i < DefaultHaReplicas; i++ {
			hdfsSite[fmt.Sprintf("dfs.namenode.rpc-address.%s.nn%d", DefaultNameService, i)] = fmt.Sprintf(
				"%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
				cluster.Name,
				i,
				cluster.Name,
				cluster.Namespace,
				GetClusterDomain(cluster),
				getPortByName(cluster, "nn-rpc"))
		}
		hdfsSite["dfs.namenode.shared.edits.dir"] = fmt.Sprintf("qjournal://%s/%s", qjournal, DefaultNameService)
		hdfsSite[fmt.Sprintf("dfs.client.failover.proxy.provider.%s", DefaultNameService)] =
			"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
		hdfsSite["dfs.journalnode.edits.dir"] = fmt.Sprintf("%s%s",
			HdfsDataPath,
			fmt.Sprintf("%s%d", HdfsDiskPathPrefix, 0))
		hdfsSite["dfs.ha.fencing.methods"] = "shell(/bin/true)"
		hdfsSite["dfs.ha.automatic-failover.enabled"] = "true"
	} else {
		hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
	}
	hdfsSite["dfs.permissions.enabled"] = "true"
	hdfsSite["dfs.replication"] = "3"
	hdfsSite["dfs.namenode.name.dir"] = fmt.Sprintf("%s%s",
		HdfsDataPath,
		fmt.Sprintf("%s%d", HdfsDiskPathPrefix, 0))

	num := getDiskNum(cluster, HdfsRoleDataNode)
	volumeNames := make([]string, 0)
	for i := 0; i < int(num); i++ {
		volumeNames := append(volumeNames, fmt.Sprintf("%s%s", HdfsDataPath, fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i)))
		hdfsSite["dfs.datanode.data.dir"] = strings.Join(volumeNames, ",")
	}
}

func constructConfig(cluster *hdfsv1.HdfsCluster, role string) (string, string, error) {
	coresite := make(map[string]string)
	hdfssite := make(map[string]string)
	// add custom conf to core site or hdfs site
	coreSiteDefaultConf, hdfsSiteDefaultConf, err := DefaultXml2Map()
	if err != nil {
		return "", "", err
	}
	for k, v := range coreSiteDefaultConf {
		coresite[k] = getConfigValue(cluster, k, v)
	}
	for k, v := range hdfsSiteDefaultConf {
		hdfssite[k] = getConfigValue(cluster, k, v)
	}

	if CheckHdfsHA(cluster) {
		makeHdfsSite(cluster, hdfssite, true)
		makeCoreSite(cluster, coresite, true)
	} else {
		makeHdfsSite(cluster, hdfssite, false)
		makeCoreSite(cluster, coresite, false)
	}

	return map2String(coresite), map2String(hdfssite), nil
}

func getImageConfig(cluster *hdfsv1.HdfsCluster) hdfsv1.ImageConfig {
	ic := hdfsv1.ImageConfig{
		Repository:  cluster.Spec.Image.Repository,
		PullSecrets: cluster.Spec.Image.PullSecrets,
	}
	ic.Tag = cluster.Spec.Image.Tag
	if ic.Tag == "" {
		ic.Tag = cluster.Spec.Version
	}
	ic.PullPolicy = cluster.Spec.Image.PullPolicy
	if ic.PullPolicy == "" {
		ic.PullPolicy = string(corev1.PullIfNotPresent)
	}
	return ic
}

func (r *HdfsClusterReconciler) constructServicePorts(cluster *hdfsv1.HdfsCluster, role string) []corev1.ServicePort {
	servicePorts := make([]corev1.ServicePort, 0)
	rolePrefix := HDFSRole2Prefix[role]
	for name, port := range DefaultNamedPort {
		if strings.HasPrefix(name, rolePrefix) {
			namedPort := port
			if cluster.Spec.Conf != nil {
				if value, ok := cluster.Spec.Conf[DefaultNamedPortConfKey[name]]; ok {
					u, err := url.Parse("//" + value)
					if err == nil {
						tmpPort, _ := strconv.Atoi(u.Port())
						namedPort = int32(tmpPort)
					}
				}
			}
			servicePorts = append(servicePorts, corev1.ServicePort{
				Name: name,
				Port: namedPort,
			})
		}
	}
	return servicePorts
}

func (r *HdfsClusterReconciler) constructHeadlessService(cluster *hdfsv1.HdfsCluster, role string) (*corev1.Service, error) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, role, DefaultHeadlessSvcNameSuffix),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster),
		},
		Spec: corev1.ServiceSpec{
			Ports:     r.constructServicePorts(cluster, role),
			Selector:  ClusterResourceLabels(cluster),
			ClusterIP: corev1.ClusterIPNone,
		},
	}
	if err := ctrl.SetControllerReference(cluster, svc, r.Scheme); err != nil {
		return svc, err
	}
	return svc, nil
}

func (r *HdfsClusterReconciler) constructService(cluster *hdfsv1.HdfsCluster, role string) (*corev1.Service, error) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, role),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster),
		},
		Spec: corev1.ServiceSpec{
			Ports:    r.constructServicePorts(cluster, role),
			Selector: ClusterResourceLabels(cluster),
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := ctrl.SetControllerReference(cluster, svc, r.Scheme); err != nil {
		return svc, err
	}
	return svc, nil
}

func (r *HdfsClusterReconciler) constructConfigMap(cluster *hdfsv1.HdfsCluster) (*corev1.ConfigMap, error) {
	coresite, hdfssite, err := constructConfig(cluster, HDFS_ROLE)
	if err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, DefaultConfigNameSuffix),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster),
		},
		Data: map[string]string{
			DefaultCoreSiteFile: coresite,
			DefaultHdfsSiteFile: hdfssite,
		},
	}
	if err := ctrl.SetControllerReference(cluster, cm, r.Scheme); err != nil {
		return cm, err
	}
	return cm, nil
}

func (r *HdfsClusterReconciler) getStorageRequests(cluster *hdfsv1.HdfsCluster) (*resource.Quantity, error) {
	if cluster.Spec.Clusters != nil {
		for _, v := range cluster.Spec.Clusters {
			if v.Type == hdfsv1.ClusterType(HDFS_ROLE) {
				if value, ok := v.Resource.ResourceRequirements.Requests["storage"]; ok {
					return &value, nil
				}
			}
		}
	}

	if cluster.Spec.Resource.ResourceRequirements.Requests != nil {
		if value, ok := cluster.Spec.Resource.ResourceRequirements.Requests["storage"]; ok {
			return &value, nil
		}
	}

	return capacityPerVolume(DefaultHdfsVolumeSize)
}

func (r *HdfsClusterReconciler) constructContainerPorts(cluster *hdfsv1.HdfsCluster, role string) []corev1.ContainerPort {
	containerPorts := make([]corev1.ContainerPort, 0)
	rolePrefix := HDFSRole2Prefix[role]
	for name, port := range DefaultNamedPort {
		if strings.HasPrefix(name, rolePrefix) {
			namedPort := port
			if cluster.Spec.Conf != nil {
				if value, ok := cluster.Spec.Conf[DefaultNamedPortConfKey[name]]; ok {
					u, err := url.Parse("//" + value)
					if err == nil {
						tmpPort, _ := strconv.Atoi(u.Port())
						namedPort = int32(tmpPort)
					}
				}
			}
			containerPorts = append(containerPorts, corev1.ContainerPort{
				Name:          name,
				ContainerPort: namedPort,
			})
		}
	}
	return containerPorts
}

func (r *HdfsClusterReconciler) constructVolumeMounts(cluster *hdfsv1.HdfsCluster) []corev1.VolumeMount {
	num := int(getDiskNum(cluster, HDFS_ROLE))
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      ClusterResourceName(cluster, HDFS_ROLE, DefaultConfigNameSuffix),
			MountPath: fmt.Sprintf("%s/%s", HdfsConfDir, DefaultCoreSiteFile),
			SubPath:   DefaultCoreSiteFile,
		},
		{
			Name:      ClusterResourceName(cluster, HDFS_ROLE, DefaultConfigNameSuffix),
			MountPath: fmt.Sprintf("%s/%s", HdfsConfDir, DefaultHdfsSiteFile),
			SubPath:   DefaultHdfsSiteFile,
		},
		{
			Name:      DefaultLogVolumeName,
			MountPath: HdfsLogsDir,
		},
	}
	for i := 0; i < num; i++ {
		volumeName := fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: fmt.Sprintf("%s%s", HdfsDataPath, volumeName),
		})
	}
	return volumeMounts
}

func (r *HdfsClusterReconciler) constructVolumes(cluster *hdfsv1.HdfsCluster) []corev1.Volume {
	num := int(getDiskNum(cluster, HDFS_ROLE))
	volumes := []corev1.Volume{
		{
			Name: ClusterResourceName(cluster, HDFS_ROLE, DefaultConfigNameSuffix),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: ClusterResourceName(cluster, HDFS_ROLE, DefaultConfigNameSuffix),
					},
					Items: []corev1.KeyToPath{
						{
							Key:  DefaultCoreSiteFile,
							Path: DefaultCoreSiteFile,
						},
						{
							Key:  DefaultHdfsSiteFile,
							Path: DefaultHdfsSiteFile,
						},
					},
				},
			},
		},
		{
			Name: DefaultLogVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: DefaultLogVolumeName,
					ReadOnly:  false,
				},
			},
		},
	}
	for i := 0; i < num; i++ {
		volumes = append(volumes, corev1.Volume{
			Name: fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i),
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i),
					ReadOnly:  false,
				},
			}},
		)
	}
	return volumes
}

func (r *HdfsClusterReconciler) constructReadinessProbe(cluster *hdfsv1.HdfsCluster, role string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Port: intstr.FromInt32(getHttpPortByRole(cluster, role)),
			},
		},
		InitialDelaySeconds: DefaultReadinessProbeInitialDelaySeconds,
		PeriodSeconds:       DefaultReadinessProbePeriodSeconds,
		TimeoutSeconds:      DefaultReadinessProbeTimeoutSeconds,
		FailureThreshold:    DefaultReadinessProbeFailureThreshold,
		SuccessThreshold:    DefaultReadinessProbeSuccessThreshold,
	}
}

func (r *HdfsClusterReconciler) constructLivenessProbe(cluster *hdfsv1.HdfsCluster, role string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Port: intstr.FromInt32(getHttpPortByRole(cluster, role)),
			},
		},
		InitialDelaySeconds: DefaultLivenessProbeInitialDelaySeconds,
		PeriodSeconds:       DefaultLivenessProbePeriodSeconds,
		TimeoutSeconds:      DefaultLivenessProbeTimeoutSeconds,
		FailureThreshold:    DefaultLivenessProbeFailureThreshold,
		SuccessThreshold:    DefaultLivenessProbeSuccessThreshold,
	}
}

func (r *HdfsClusterReconciler) constructPodSpec(cluster *hdfsv1.HdfsCluster) corev1.PodSpec {
	tgp := int64(DefaultTerminationGracePeriod)
	ic := getImageConfig(cluster)
	var tmpPullSecrets []corev1.LocalObjectReference
	if ic.PullSecrets != "" {
		tmpPullSecrets = make([]corev1.LocalObjectReference, 0)
		tmpPullSecrets = append(tmpPullSecrets, corev1.LocalObjectReference{Name: ic.PullSecrets})
	}
	return corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:            cluster.Name,
				Image:           ic.Repository + ":" + ic.Tag,
				ImagePullPolicy: corev1.PullPolicy(ic.PullPolicy),
				Ports:           r.constructContainerPorts(cluster, HDFS_ROLE),
				Env:             DefaultEnvVars(),
				ReadinessProbe:  r.constructReadinessProbe(cluster, HDFS_ROLE),
				LivenessProbe:   r.constructLivenessProbe(cluster, HDFS_ROLE),
				VolumeMounts:    r.constructVolumeMounts(cluster),
			},
		},
		ImagePullSecrets:              tmpPullSecrets,
		RestartPolicy:                 corev1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: &tgp,
		Volumes:                       r.constructVolumes(cluster),
		Affinity: &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						TopologyKey: "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: ClusterResourceLabels(cluster),
						},
					},
				},
			},
		},
	}
}

func (r *HdfsClusterReconciler) constructPVCs(cluster *hdfsv1.HdfsCluster) ([]corev1.PersistentVolumeClaim, error) {
	q, err := r.getStorageRequests(cluster)
	if err != nil {
		return nil, err
	}
	logq, err := capacityPerVolume(DefaultHdfsLogVolumeSize)
	if err != nil {
		return nil, err
	}
	sc := GetStorageClassName(cluster)

	num := int(getDiskNum(cluster, HDFS_ROLE))
	pvcs := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultLogVolumeName,
				Namespace: cluster.Namespace,
				Labels:    ClusterResourceLabels(cluster),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &sc,
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.ResourceRequirements{
					Requests: volumeRequest(*logq),
				},
			},
		},
	}
	for i := 0; i < num; i++ {
		pvcs = append(pvcs, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i),
				Namespace: cluster.Namespace,
				Labels:    ClusterResourceLabels(cluster),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &sc,
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.ResourceRequirements{
					Requests: volumeRequest(*q),
				},
			},
		})
	}
	return pvcs, nil
}

func (r *HdfsClusterReconciler) constructWorkload(cluster *hdfsv1.HdfsCluster) (*appsv1.StatefulSet, error) {
	pvcs, err := r.constructPVCs(cluster)
	if err != nil {
		return nil, nil
	}
	stsDesired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, HDFS_ROLE),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster),
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: ClusterResourceLabels(cluster),
			},
			ServiceName: ClusterResourceName(cluster, HDFS_ROLE),
			Replicas:    int32Ptr(getReplicas(cluster, HDFS_ROLE)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ClusterResourceLabels(cluster),
				},
				Spec: r.constructPodSpec(cluster),
			},
			VolumeClaimTemplates: pvcs,
		},
	}

	if err := ctrl.SetControllerReference(cluster, stsDesired, r.Scheme); err != nil {
		return stsDesired, err
	}
	return stsDesired, nil
}
