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

	//httpfs do not need a pvc by default
	if role == HdfsRoleHttpFS {
		return 0
	}

	return DefaultDiskNum
}

func makeHttpFSSite(cluster *hdfsv1.HdfsCluster, httpfsSite map[string]string, ha bool) {
	if _, ok := httpfsSite["httpfs.http.port"]; !ok {
		httpfsSite["httpfs.http.port"] = fmt.Sprintf("%d", getPortByName(cluster, "hf-http"))
	}

	if _, ok := httpfsSite["httpfs.buffer.size"]; !ok {
		httpfsSite["httpfs.buffer.size"] = DefaultIOBufferSize
	}

	if _, ok := httpfsSite["hadoop.http.temp.dir"]; !ok {
		httpfsSite["hadoop.http.temp.dir"] = HttpFSTempDir
	}

}

func makeCoreSite(cluster *hdfsv1.HdfsCluster, coreSite map[string]string, ha bool) {
	if ha {
		_, endpoints, _ := GetRefZookeeperInfo(cluster)
		coreSite["ha.zookeeper.parent-znode"] = fmt.Sprintf("%s%s", DefaultHdfsParentZnodePrefix, ClusterResourceName(cluster))
		coreSite["ha.zookeeper.quorum"] = endpoints
		if _, ok := coreSite["fs.defaultFS"]; !ok {
			coreSite["fs.defaultFS"] = fmt.Sprintf("hdfs://%s", DefaultNameService)
		}
	} else {
		coreSite["fs.defaultFS"] = fmt.Sprintf(
			"hdfs://%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
			ClusterResourceName(cluster),
			0,
			ClusterResourceName(cluster),
			cluster.Namespace,
			GetClusterDomain(cluster),
			getPortByName(cluster, "nn-rpc"))
	}

	if _, ok := coreSite["hadoop.tmp.dir"]; !ok {
		coreSite["hadoop.tmp.dir"] = fmt.Sprintf("%s/%s/tmp",
			HdfsDataPath,
			fmt.Sprintf("%s%d", HdfsDiskPathPrefix, 0))
	}

	if _, ok := coreSite["io.file.buffer.size"]; !ok {
		coreSite["io.file.buffer.size"] = DefaultIOBufferSize
	}

	if _, ok := coreSite["hadoop.http.staticuser.user"]; !ok {
		coreSite["hadoop.http.staticuser.user"] = DefaultWebUIUser
		coreSite["hadoop.proxyuser.root.groups"] = "*"
		coreSite["hadoop.proxyuser.root.users"] = "*"
	}

}

func makeHdfsSite(cluster *hdfsv1.HdfsCluster, hdfsSite map[string]string, ha bool) {
	nameServicesCustomed := false
	if _, ok := hdfsSite["dfs.nameservices"]; !ok {
		hdfsSite["dfs.nameservices"] = DefaultNameService

	} else {
		nameServicesCustomed = true
	}

	if ha {
		if !nameServicesCustomed {
			hdfsSite[fmt.Sprintf("dfs.ha.namenodes.%s", DefaultNameService)] = "nn0,nn1"
			for i := 0; i < DefaultHaReplicas; i++ {
				hdfsSite[fmt.Sprintf("dfs.namenode.rpc-address.%s.nn%d", DefaultNameService, i)] = fmt.Sprintf(
					"%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
					ClusterResourceName(cluster),
					i,
					ClusterResourceName(cluster),
					cluster.Namespace,
					GetClusterDomain(cluster),
					getPortByName(cluster, "nn-rpc"))
				hdfsSite[fmt.Sprintf("dfs.namenode.http-address.%s.nn%d", DefaultNameService, i)] = fmt.Sprintf(
					"%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
					ClusterResourceName(cluster),
					i,
					ClusterResourceName(cluster),
					cluster.Namespace,
					GetClusterDomain(cluster),
					getPortByName(cluster, "nn-http"))
			}

			quorumJournals := make([]string, 0)
			for i := 0; i < DefaultQuorumReplicas; i++ {
				quorumJournal := fmt.Sprintf("%s-journalnode-%d.%s-journalnode.%s.svc.%s:%d",
					ClusterResourceName(cluster),
					i,
					ClusterResourceName(cluster),
					cluster.Namespace,
					GetClusterDomain(cluster),
					getPortByName(cluster, "jn-rpc"))
				quorumJournals = append(quorumJournals, quorumJournal)
			}
			qjournal := strings.Join(quorumJournals, ";")
			hdfsSite["dfs.namenode.shared.edits.dir"] = fmt.Sprintf("qjournal://%s/%s", qjournal, DefaultNameService)

			hdfsSite[fmt.Sprintf("dfs.client.failover.proxy.provider.%s", DefaultNameService)] =
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		hdfsSite["dfs.journalnode.edits.dir"] = fmt.Sprintf("%s/%s",
			HdfsDataPath,
			fmt.Sprintf("%s%d", HdfsDiskPathPrefix, 0))
		if _, ok := hdfsSite["dfs.ha.fencing.methods"]; !ok {
			hdfsSite["dfs.ha.fencing.methods"] = "shell(/bin/true)"
		}
		if _, ok := hdfsSite["dfs.ha.automatic-failover.enabled"]; !ok {
			hdfsSite["dfs.ha.automatic-failover.enabled"] = "true"
		}
		FillJNEnvs(hdfsSite["dfs.namenode.shared.edits.dir"])
		FillNNEnvs(hdfsSite)
	} else {
		if !nameServicesCustomed {
			hdfsSite[fmt.Sprintf("dfs.namenode.rpc-address.%s", DefaultNameService)] = fmt.Sprintf(
				"%s-namenode-%d.%s-namenode.%s.svc.%s:%d",
				ClusterResourceName(cluster),
				0,
				ClusterResourceName(cluster),
				cluster.Namespace,
				GetClusterDomain(cluster),
				getPortByName(cluster, "nn-rpc"))
		}
	}

	if _, ok := hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"]; !ok {
		hdfsSite["dfs.namenode.datanode.registration.ip-hostname-check"] = "false"
	}
	if _, ok := hdfsSite["dfs.datanode.fsdataset.volume.choosing.policy"]; !ok {
		hdfsSite["dfs.datanode.fsdataset.volume.choosing.policy"] = "org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy"
	}

	hdfsSite["dfs.namenode.name.dir"] = fmt.Sprintf("file://%s/%s",
		HdfsDataPath,
		fmt.Sprintf("%s%d", HdfsDiskPathPrefix, 0))

	num := getDiskNum(cluster, HdfsRoleDataNode)
	volumeNames := make([]string, 0)
	for i := 0; i < int(num); i++ {
		volumeNames = append(volumeNames, fmt.Sprintf("%s/%s", HdfsDataPath, fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i)))
	}
	hdfsSite["dfs.datanode.data.dir"] = strings.Join(volumeNames, ",")
}

func getConfigValue(conf, coreSiteDefaultConf, hdfsSiteDefaultConf, httpfsSiteDefaultConf, coreSite, hdfsSite, httpfsSite map[string]string) {
	for k, v := range conf {
		if _, ok := coreSiteDefaultConf[k]; ok {
			coreSite[k] = v
		} else if _, ok = hdfsSiteDefaultConf[k]; ok {
			hdfsSite[k] = v
		} else if _, ok = httpfsSiteDefaultConf[k]; ok {
			httpfsSite[k] = v
		} else {
			//special k/v config, the key is not fixed,such as dfs.ha.namenodes.xxx,dfs.namenode.rpc-address.xxx.xxx
			//dfs.client.failover.proxy.provider.xxx,hadoop.security.crypto.codec.classes.xxx
			for _, prefix := range CustomizableHdfsSiteConfKeyPrefixs {
				if strings.Contains(k, prefix) {
					hdfsSite[k] = v
				}
			}
			for _, prefix := range CustomizableCoreSiteConfKeyPrefixs {
				if strings.Contains(k, prefix) {
					coreSite[k] = v
				}
			}
		}
	}
}

func constructConfig(cluster *hdfsv1.HdfsCluster) (string, string, string, error) {
	coreSite := make(map[string]string)
	hdfsSite := make(map[string]string)
	httpfsSite := make(map[string]string)
	// add custom conf to core site or hdfs site
	coreSiteDefaultConf, hdfsSiteDefaultConf, httpfsSiteDefaultConf, err := DefaultXml2Map()
	if err != nil {
		return "", "", "", err
	}

	if cluster.Spec.Conf != nil {
		getConfigValue(cluster.Spec.Conf, coreSiteDefaultConf, hdfsSiteDefaultConf, httpfsSiteDefaultConf, coreSite, hdfsSite, httpfsSite)
	}

	if cluster.Spec.Clusters != nil {
		for _, c := range cluster.Spec.Clusters {
			if c.Conf != nil {
				getConfigValue(c.Conf, coreSiteDefaultConf, hdfsSiteDefaultConf, httpfsSiteDefaultConf, coreSite, hdfsSite, httpfsSite)
			}
		}
	}

	makeHdfsSite(cluster, hdfsSite, CheckHdfsHA(cluster))
	makeCoreSite(cluster, coreSite, CheckHdfsHA(cluster))
	makeHttpFSSite(cluster, httpfsSite, CheckHdfsHA(cluster))

	return map2Xml(coreSite), map2Xml(hdfsSite), map2Xml(httpfsSite), nil
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

func getPortByName(cluster *hdfsv1.HdfsCluster, name string) int32 {
	var namedPort int32
	for k, v := range DefaultNamedPort {
		if strings.EqualFold(name, k) {
			namedPort = parsePortFromConf(cluster, name)
			if namedPort == 0 {
				namedPort = v
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

func getRpcPortByRole(cluster *hdfsv1.HdfsCluster, role string) int32 {
	rolePrefix := HDFSRole2Prefix[role]
	name := fmt.Sprintf("%s%s", rolePrefix, "-rpc")
	return getPortByName(cluster, name)
}

func parsePortFromConf(cluster *hdfsv1.HdfsCluster, name string) int32 {
	var namedPort int32
	if cluster.Spec.Conf != nil {
		if value, ok := cluster.Spec.Conf[DefaultNamedPortConfKey[name]]; ok {
			u, err := url.Parse("//" + value)
			if err == nil {
				tmpPort, _ := strconv.Atoi(u.Port())
				namedPort = int32(tmpPort)
			} else {
				tmpPort, err := strconv.Atoi(value)
				if err != nil {
					namedPort = int32(tmpPort)
				}
			}
		}
	}
	return namedPort
}

func (r *HdfsClusterReconciler) constructServicePorts(cluster *hdfsv1.HdfsCluster, role string) []corev1.ServicePort {
	servicePorts := make([]corev1.ServicePort, 0)
	rolePrefix := HDFSRole2Prefix[role]
	for name, port := range DefaultNamedPort {
		if strings.HasPrefix(name, rolePrefix) {
			namedPort := parsePortFromConf(cluster, name)
			if namedPort == 0 {
				namedPort = port
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
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", role), DefaultHeadlessSvcNameSuffix),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster, role),
		},
		Spec: corev1.ServiceSpec{
			Ports:     r.constructServicePorts(cluster, role),
			Selector:  ClusterResourceLabels(cluster, role),
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
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", role)),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster, role),
		},
		Spec: corev1.ServiceSpec{
			Ports:    r.constructServicePorts(cluster, role),
			Selector: ClusterResourceLabels(cluster, role),
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := ctrl.SetControllerReference(cluster, svc, r.Scheme); err != nil {
		return svc, err
	}
	return svc, nil
}

func (r *HdfsClusterReconciler) constructConfigMap(cluster *hdfsv1.HdfsCluster) (*corev1.ConfigMap, error) {
	coresite, hdfssite, httpfssite, err := constructConfig(cluster)
	if err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster, HdfsRoleAll),
		},
		Data: map[string]string{
			DefaultCoreSiteFile:   coresite,
			DefaultHdfsSiteFile:   hdfssite,
			DefaultHttpFSSiteFile: httpfssite,
		},
	}
	if err := ctrl.SetControllerReference(cluster, cm, r.Scheme); err != nil {
		return cm, err
	}
	return cm, nil
}

func (r *HdfsClusterReconciler) getStorageRequests(cluster *hdfsv1.HdfsCluster, role string) (*resource.Quantity, error) {
	if cluster.Spec.Clusters != nil {
		for _, v := range cluster.Spec.Clusters {
			if v.Type == hdfsv1.ClusterType(role) {
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
			namedPort := parsePortFromConf(cluster, name)
			if namedPort == 0 {
				namedPort = port
			}
			containerPorts = append(containerPorts, corev1.ContainerPort{
				Name:          name,
				ContainerPort: namedPort,
			})
		}
	}
	return containerPorts
}

func (r *HdfsClusterReconciler) constructVolumeMounts(cluster *hdfsv1.HdfsCluster, role string) []corev1.VolumeMount {
	num := int(getDiskNum(cluster, role))
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
			MountPath: fmt.Sprintf("%s/%s", HdfsConfDir, DefaultCoreSiteFile),
			SubPath:   DefaultCoreSiteFile,
		},
		{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
			MountPath: fmt.Sprintf("%s/%s", HdfsConfDir, DefaultHdfsSiteFile),
			SubPath:   DefaultHdfsSiteFile,
		},
		{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
			MountPath: fmt.Sprintf("%s/%s", HdfsConfDir, DefaultHttpFSSiteFile),
			SubPath:   DefaultHttpFSSiteFile,
		},
	}
	if role != HdfsRoleHttpFS {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      DefaultLogVolumeName,
			MountPath: HdfsLogsDir,
		})
	}
	for i := 0; i < num; i++ {
		volumeName := fmt.Sprintf("%s%d", HdfsDiskPathPrefix, i)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: fmt.Sprintf("%s/%s", HdfsDataPath, volumeName),
		})
	}
	return volumeMounts
}

func (r *HdfsClusterReconciler) constructVolumes(cluster *hdfsv1.HdfsCluster, role string) []corev1.Volume {
	num := int(getDiskNum(cluster, role))
	volumes := []corev1.Volume{
		{
			Name: ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: ClusterResourceName(cluster, fmt.Sprintf("-%s", DefaultConfigNameSuffix)),
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
						{
							Key:  DefaultHttpFSSiteFile,
							Path: DefaultHttpFSSiteFile,
						},
					},
				},
			},
		},
	}

	if role != HdfsRoleHttpFS {
		volumes = append(volumes, corev1.Volume{
			Name: DefaultLogVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: DefaultLogVolumeName,
					ReadOnly:  false,
				},
			},
		})
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

func (r *HdfsClusterReconciler) getProbeHandler(cluster *hdfsv1.HdfsCluster, role string, pType string) corev1.ProbeHandler {
	ph := corev1.ProbeHandler{}
	if role == HdfsRoleHttpFS {
		ph = corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(getHttpPortByRole(cluster, role)),
			},
		}
	} else {
		if pType == HdfsProbeTypeReadiness {
			ph = corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Port: intstr.FromInt32(getHttpPortByRole(cluster, role)),
				},
			}
		} else {
			ph = corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(getRpcPortByRole(cluster, role)),
				},
			}
		}

	}
	return ph
}

func (r *HdfsClusterReconciler) constructReadinessProbe(cluster *hdfsv1.HdfsCluster, role string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        r.getProbeHandler(cluster, role, HdfsProbeTypeReadiness),
		InitialDelaySeconds: DefaultReadinessProbeInitialDelaySeconds,
		PeriodSeconds:       DefaultReadinessProbePeriodSeconds,
		TimeoutSeconds:      DefaultReadinessProbeTimeoutSeconds,
		FailureThreshold:    DefaultReadinessProbeFailureThreshold,
		SuccessThreshold:    DefaultReadinessProbeSuccessThreshold,
	}
}

func (r *HdfsClusterReconciler) constructLivenessProbe(cluster *hdfsv1.HdfsCluster, role string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        r.getProbeHandler(cluster, role, HdfsProbeTypeLiveness),
		InitialDelaySeconds: DefaultLivenessProbeInitialDelaySeconds,
		PeriodSeconds:       DefaultLivenessProbePeriodSeconds,
		TimeoutSeconds:      DefaultLivenessProbeTimeoutSeconds,
		FailureThreshold:    DefaultLivenessProbeFailureThreshold,
		SuccessThreshold:    DefaultLivenessProbeSuccessThreshold,
	}
}

func (r *HdfsClusterReconciler) constructPodSpec(cluster *hdfsv1.HdfsCluster, role string) corev1.PodSpec {
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
				Ports:           r.constructContainerPorts(cluster, role),
				Env:             DefaultEnvVars(role),
				ReadinessProbe:  r.constructReadinessProbe(cluster, role),
				LivenessProbe:   r.constructLivenessProbe(cluster, role),
				VolumeMounts:    r.constructVolumeMounts(cluster, role),
			},
		},
		ImagePullSecrets:              tmpPullSecrets,
		RestartPolicy:                 corev1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: &tgp,
		Volumes:                       r.constructVolumes(cluster, role),
		Affinity: &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						TopologyKey: "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: ClusterResourceLabels(cluster, role),
						},
					},
				},
			},
		},
	}
}

func (r *HdfsClusterReconciler) constructPVCs(cluster *hdfsv1.HdfsCluster, role string) ([]corev1.PersistentVolumeClaim, error) {
	q, err := r.getStorageRequests(cluster, role)
	if err != nil {
		return nil, err
	}
	logq, err := capacityPerVolume(DefaultHdfsLogVolumeSize)
	if err != nil {
		return nil, err
	}
	sc := GetStorageClassName(cluster)

	num := int(getDiskNum(cluster, role))
	pvcs := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultLogVolumeName,
				Namespace: cluster.Namespace,
				Labels:    ClusterResourceLabels(cluster, role),
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
				Labels:    ClusterResourceLabels(cluster, role),
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

func (r *HdfsClusterReconciler) constructStatefulSet(cluster *hdfsv1.HdfsCluster, role string) (*appsv1.StatefulSet, error) {
	pvcs, err := r.constructPVCs(cluster, role)
	if err != nil {
		return nil, nil
	}
	stsDesired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", role)),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster, role),
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: ClusterResourceLabels(cluster, role),
			},
			ServiceName: ClusterResourceName(cluster, fmt.Sprintf("-%s", role)),
			Replicas:    int32Ptr(GetReplicas(cluster, role)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ClusterResourceLabels(cluster, role),
				},
				Spec: r.constructPodSpec(cluster, role),
			},
			VolumeClaimTemplates: pvcs,
		},
	}

	if err := ctrl.SetControllerReference(cluster, stsDesired, r.Scheme); err != nil {
		return stsDesired, err
	}
	return stsDesired, nil
}

func (r *HdfsClusterReconciler) constructDeployment(cluster *hdfsv1.HdfsCluster, role string) (*appsv1.Deployment, error) {
	deployDesired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClusterResourceName(cluster, fmt.Sprintf("-%s", role)),
			Namespace: cluster.Namespace,
			Labels:    ClusterResourceLabels(cluster, role),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: ClusterResourceLabels(cluster, role),
			},
			Replicas: int32Ptr(GetReplicas(cluster, role)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ClusterResourceLabels(cluster, role),
				},
				Spec: r.constructPodSpec(cluster, role),
			},
		},
	}

	if err := ctrl.SetControllerReference(cluster, deployDesired, r.Scheme); err != nil {
		return deployDesired, err
	}
	return deployDesired, nil
}
