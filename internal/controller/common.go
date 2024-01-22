package controller

import (
	"encoding/xml"
	hdfsv1 "github.com/nineinfra/hdfs-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"os"
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

func ClusterResourceName(cluster *hdfsv1.HdfsCluster, suffixs ...string) string {
	return cluster.Name + DefaultNameSuffix + strings.Join(suffixs, "-")
}

func ClusterResourceLabels(cluster *hdfsv1.HdfsCluster) map[string]string {
	return map[string]string{
		"cluster": cluster.Name,
		"role":    HDFS_ROLE,
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

func DefaultEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HDFS_ROLE",
			Value: HDFS_ROLE,
		},
		{
			Name:  "HDFS_HA",
			Value: HDFS_HA,
		},
		{
			Name:  "HDFS_DATA_PATH",
			Value: HdfsDataPath,
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

func DefaultXml2Map() (map[string]string, map[string]string, error) {
	// 解析XML文件
	xmlFile, err := os.ReadFile(CoreSiteDefaultConfFile)
	if err != nil {
		return nil, nil, err
	}

	var conf XmlConfiguration
	err = xml.Unmarshal(xmlFile, &conf)
	if err != nil {
		return nil, nil, err
	}

	// 转换为map[string]string
	coreSite := make(map[string]string)
	for _, prop := range conf.Properties {
		coreSite[prop.Name] = prop.Value
	}
	// 解析XML文件
	xmlFile, err = os.ReadFile(HdfsSiteDefaultConfFile)
	if err != nil {
		return nil, nil, err
	}

	err = xml.Unmarshal(xmlFile, &conf)
	if err != nil {
		return nil, nil, err
	}

	// 转换为map[string]string
	hdfsSite := make(map[string]string)
	for _, prop := range conf.Properties {
		coreSite[prop.Name] = prop.Value
	}
	return coreSite, hdfsSite, nil
}
