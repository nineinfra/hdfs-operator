package controller

const (
	// CurrentHdfsVersion
	// hdfs-site k/v list.https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
	// core-site k/v list.https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/core-default.xml
	CurrentHdfsVersion = "3.3.6"
	// DefaultNameSuffix is the default name suffix of the resources of the zookeeper
	DefaultNameSuffix = "-hdfs"

	// DefaultClusterSign is the default cluster sign of the zookeeper
	DefaultClusterSign = "hdfs"

	// DefaultStorageClass is the default storage class of the zookeeper
	DefaultStorageClass = "nineinfra-default"

	DefaultNameService = "nineinfra"

	DefaultQuorumReplicas = 3
	DefaultHaReplicas     = 2
	DefaultReplicas       = 1
	DefaultDiskNum        = 1

	DefaultClusterDomainName = "clusterDomain"
	DefaultClusterDomain     = "cluster.local"

	DefaultLogVolumeName = "log"

	DefaultConfigNameSuffix      = "-config"
	DefaultHeadlessSvcNameSuffix = "-headless"

	DefaultCoreSiteFile = "core-site.xml"
	DefaultHdfsSiteFile = "hdfs-site.xml"

	// DefaultTerminationGracePeriod is the default time given before the
	// container is stopped. This gives clients time to disconnect from a
	// specific node gracefully.
	DefaultTerminationGracePeriod = 30

	// DefaultHdfsVolumeSize is the default volume size for the
	// Hdfs data volume
	DefaultHdfsVolumeSize    = "100Gi"
	DefaultHdfsLogVolumeSize = "5Gi"

	// DefaultReadinessProbeInitialDelaySeconds is the default initial delay (in seconds)
	// for the readiness probe
	DefaultReadinessProbeInitialDelaySeconds = 40

	// DefaultReadinessProbePeriodSeconds is the default probe period (in seconds)
	// for the readiness probe
	DefaultReadinessProbePeriodSeconds = 10

	// DefaultReadinessProbeFailureThreshold is the default probe failure threshold
	// for the readiness probe
	DefaultReadinessProbeFailureThreshold = 10

	// DefaultReadinessProbeSuccessThreshold is the default probe success threshold
	// for the readiness probe
	DefaultReadinessProbeSuccessThreshold = 1

	// DefaultReadinessProbeTimeoutSeconds is the default probe timeout (in seconds)
	// for the readiness probe
	DefaultReadinessProbeTimeoutSeconds = 10

	// DefaultLivenessProbeInitialDelaySeconds is the default initial delay (in seconds)
	// for the liveness probe
	DefaultLivenessProbeInitialDelaySeconds = 40

	// DefaultLivenessProbePeriodSeconds is the default probe period (in seconds)
	// for the liveness probe
	DefaultLivenessProbePeriodSeconds = 10

	// DefaultLivenessProbeFailureThreshold is the default probe failure threshold
	// for the liveness probe
	DefaultLivenessProbeFailureThreshold = 10

	// DefaultLivenessProbeSuccessThreshold is the default probe success threshold
	// for the readiness probe
	DefaultLivenessProbeSuccessThreshold = 1

	// DefaultLivenessProbeTimeoutSeconds is the default probe timeout (in seconds)
	// for the liveness probe
	DefaultLivenessProbeTimeoutSeconds = 10
)

const (
	HdfsRoleNameNode        = "namenode"
	HdfsRoleDataNode        = "datanode"
	HdfsRoleJournalNode     = "journalnode"
	HdfsRoleHttpFS          = "httpfs"
	HdfsHomeDir             = "/opt/hadoop"
	HdfsConfDir             = HdfsHomeDir + "/conf"
	HdfsDataPath            = HdfsHomeDir + "/data"
	HdfsLogsDir             = HdfsHomeDir + "/logs"
	HdfsDiskPathPrefix      = "disk"
	CoreSiteDefaultConfFile = HdfsConfDir + "/" + "core-site.xml.default"
	HdfsSiteDefaultConfFile = HdfsConfDir + "/" + "hdfs-site.xml.default"
)

var (
	HDFS_ROLE = "namenode"
	HDFS_HA   = "false"
)

var HDFSRole2Prefix = map[string]string{
	"namenode":    "nn",
	"datanode":    "dn",
	"journalnode": "jn",
}

var DefaultNamedPortConfKey = map[string]string{
	"jn-rpc":   "dfs.journalnode.rpc-address",
	"jn-http":  "dfs.journalnode.http-address",
	"jn-https": "dfs.journalnode.https-address",
	"nn-rpc":   "dfs.namenode.rpc-address",
	"nn-http":  "dfs.namenode.http-address",
	"nn-https": "dfs.namenode.https-address",
	//"nn-backup-addr": "dfs.namenode.backup.address",
	//"nn-backup-http": "dfs.namenode.backup.http-address",
	//"nn2-http":       "dfs.namenode.secondary.http-address",
	//"nn2-https":      "dfs.namenode.secondary.https-address",
	"dn-http":  "dfs.datanode.http.address",
	"dn-ipc":   "dfs.datanode.ipc.address",
	"dn-addr":  "dfs.datanode.address",
	"dn-https": "dfs.datanode.https.address",
}

var DefaultNamedPort = map[string]int32{
	"jn-rpc":   8485,
	"jn-http":  8480,
	"jn-https": 8481,
	"nn-rpc":   8020,
	"nn-http":  9870,
	"nn-https": 9871,
	//"nn-backup-addr": 50100,
	//"nn-backup-http": 50105,
	//"nn2-http":       9868,
	//"nn2-https":      9869,
	"dn-http":  9864,
	"dn-ipc":   9867,
	"dn-addr":  9866,
	"dn-https": 9865,
}
