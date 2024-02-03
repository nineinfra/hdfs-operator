package controller

import (
	"encoding/xml"
	"fmt"
	hdfsv1 "github.com/nineinfra/hdfs-operator/api/v1"
	githuberrors "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type XmlProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type XmlConfiguration struct {
	XmlName    xml.Name      `xml:"configuration"`
	Properties []XmlProperty `xml:"property"`
}

func GetReplicas(cluster *hdfsv1.HdfsCluster, role string) int32 {
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

func ClusterResourceName(cluster *hdfsv1.HdfsCluster, suffixs ...string) string {
	return cluster.Name + DefaultNameSuffix + strings.Join(suffixs, "-")
}

func ClusterResourceLabels(cluster *hdfsv1.HdfsCluster, role string) map[string]string {
	return map[string]string{
		"cluster": cluster.Name,
		"role":    role,
		"app":     DefaultClusterSign,
	}
}

func GetStorageClassName(cluster *hdfsv1.HdfsCluster) string {
	if cluster.Spec.Resource.StorageClass != "" {
		return cluster.Spec.Resource.StorageClass
	}
	return DefaultStorageClass
}

func GetClusterDomain(cluster *hdfsv1.HdfsCluster) string {
	if cluster.Spec.K8sConf != nil {
		if value, ok := cluster.Spec.K8sConf[DefaultClusterDomainName]; ok {
			return value
		}
	}
	return DefaultClusterDomain
}

func CheckHdfsHA(cluster *hdfsv1.HdfsCluster) bool {
	if cluster.Spec.Clusters != nil {
		for _, v := range cluster.Spec.Clusters {
			if v.Type == hdfsv1.NameNodeClusterType {
				return v.Resource.Replicas == DefaultHaReplicas
			}
		}
	}
	return false
}

func FillZKEnvs(zkEndpoints string, zkReplicas int) {
	if zkEndpoints != "" && zkReplicas != 0 {
		zkNodes := strings.Split(zkEndpoints, ",")
		zkHosts := make([]string, 0)
		for _, v := range zkNodes {
			zkHP := strings.Split(v, ":")
			zkHosts = append(zkHosts, zkHP[0])
			ZK_CLIENT_PORT = zkHP[1]
		}
		ZK_NODES = strings.Join(zkHosts, ",")
	}
}

func FillJNEnvs(qjournal string) {
	if qjournal != "" {
		re := regexp.MustCompile(`qjournal://(.+?)/`)
		matches := re.FindStringSubmatch(qjournal)
		if len(matches) > 1 {
			jnNodes := strings.Split(matches[1], ";")
			jnHosts := make([]string, 0)
			for _, v := range jnNodes {
				jnHP := strings.Split(v, ":")
				jnHosts = append(jnHosts, jnHP[0])
				JN_RPC_PORT = jnHP[1]
			}
			JN_NODES = strings.Join(jnHosts, ",")
		}
	}
}

func FillNNEnvs(hdfsSite map[string]string) {
	if hdfsSite != nil {
		ns := hdfsSite["dfs.nameservices"]
		nns := hdfsSite[fmt.Sprintf("dfs.ha.namenodes.%s", ns)]
		nnlist := strings.Split(nns, ",")
		nn0 := hdfsSite[fmt.Sprintf("dfs.namenode.rpc-address.%s.%s", ns, nnlist[0])]
		nn1 := hdfsSite[fmt.Sprintf("dfs.namenode.rpc-address.%s.%s", ns, nnlist[1])]
		nn0NodeAndPort := strings.Split(nn0, ":")
		nn1NodeAndPort := strings.Split(nn1, ":")
		if len(nn0NodeAndPort) == 2 && len(nn1NodeAndPort) == 2 {
			NN0_NODE = nn0NodeAndPort[0]
			NN1_NODE = nn1NodeAndPort[0]
			NN_RPC_PORT = nn0NodeAndPort[1]
		}
	}
}

func GetRefZookeeperInfo(cluster *hdfsv1.HdfsCluster) (int, string, error) {
	zkReplicas := 0
	zkEndpoints := ""
	if cluster.Spec.ClusterRefs != nil {
		for _, v := range cluster.Spec.ClusterRefs {
			if v.Type == hdfsv1.ZookeeperClusterType {
				if v.Conf != nil {
					if value, ok := v.Conf[hdfsv1.RefClusterZKReplicasKey]; ok {
						replicas, err := strconv.ParseInt(value, 0, 0)
						if err != nil {
							return 0, "", err
						}
						if replicas < 3 || replicas%2 == 0 {
							return 0, "", githuberrors.New("invalid zookeeper replicas for hdfs ha,should be odd num and larger than 3")
						}
						zkReplicas = int(replicas)
					}
					if endpoints, ok := v.Conf[hdfsv1.RefClusterZKEndpointsKey]; ok {
						listValue := strings.Split(endpoints, ",")
						if len(listValue) != zkReplicas {
							return 0, "", githuberrors.New("invalid zookeeper endpoints for hdfs ha")
						}
						zkEndpoints = endpoints
					}
				}
			}
		}
	}
	FillZKEnvs(zkEndpoints, zkReplicas)
	return zkReplicas, zkEndpoints, nil
}

func DefaultEnvVars(role string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HDFS_ROLE",
			Value: role,
		},
		{
			Name:  "HDFS_HA",
			Value: HDFS_HA,
		},
		{
			Name:  "JN_NODES",
			Value: JN_NODES,
		},
		{
			Name:  "JN_RPC_PORT",
			Value: JN_RPC_PORT,
		},
		{
			Name:  "NN0_NODE",
			Value: NN0_NODE,
		},
		{
			Name:  "NN1_NODE",
			Value: NN1_NODE,
		},
		{
			Name:  "NN_RPC_PORT",
			Value: NN_RPC_PORT,
		},
		{
			Name:  "ZK_NODES",
			Value: ZK_NODES,
		},
		{
			Name:  "ZK_CLIENT_PORT",
			Value: ZK_CLIENT_PORT,
		},
		{
			Name:  "HDFS_DATA_PATH",
			Value: fmt.Sprintf("%s/%s%d", HdfsDataPath, HdfsDiskPathPrefix, 0),
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name: "POD_UID",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.uid",
				},
			},
		},
		{
			Name: "HOST_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		},
	}
}

func DefaultXml2Map() (map[string]string, map[string]string, map[string]string, error) {
	// 解析XML文件
	xmlFile, err := os.ReadFile(CoreSiteDefaultConfFile)
	if err != nil {
		return nil, nil, nil, err
	}

	var conf XmlConfiguration
	err = xml.Unmarshal(xmlFile, &conf)
	if err != nil {
		return nil, nil, nil, err
	}

	// 转换为map[string]string
	coreSite := make(map[string]string)
	for _, prop := range conf.Properties {
		coreSite[prop.Name] = prop.Value
	}
	// 解析XML文件
	xmlFile, err = os.ReadFile(HdfsSiteDefaultConfFile)
	if err != nil {
		return nil, nil, nil, err
	}

	err = xml.Unmarshal(xmlFile, &conf)
	if err != nil {
		return nil, nil, nil, err
	}

	// 转换为map[string]string
	hdfsSite := make(map[string]string)
	for _, prop := range conf.Properties {
		coreSite[prop.Name] = prop.Value
	}

	// 解析XML文件
	xmlFile, err = os.ReadFile(HttpFSSiteDefaultConfFile)
	if err != nil {
		return nil, nil, nil, err
	}

	err = xml.Unmarshal(xmlFile, &conf)
	if err != nil {
		return nil, nil, nil, err
	}

	// 转换为map[string]string
	httpfsSite := make(map[string]string)
	for _, prop := range conf.Properties {
		coreSite[prop.Name] = prop.Value
	}
	return coreSite, hdfsSite, httpfsSite, nil
}
