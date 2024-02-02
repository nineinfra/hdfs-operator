package controller

const (
	// CurrentHdfsVersion
	// hdfs-site k/v list.https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
	// core-site k/v list.https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/core-default.xml
	CurrentHdfsVersion = "v3.3.6"
	// DefaultNameSuffix is the default name suffix of the resources of the zookeeper
	DefaultNameSuffix = "-hdfs"

	// DefaultClusterSign is the default cluster sign of the zookeeper
	DefaultClusterSign = "hdfs"

	// DefaultStorageClass is the default storage class of the zookeeper
	DefaultStorageClass = "nineinfra-default"

	// DefaultNameService is the default name service for hdfs
	DefaultNameService = "nineinfra"

	// DefaultWebUIUser is the default web ui user for hdfs
	DefaultWebUIUser = "root"

	// DefaultQuorumReplicas is the default quorum replicas
	DefaultQuorumReplicas = 3

	// DefaultHaReplicas is the default ha replicas for namenode
	DefaultHaReplicas = 2

	// DefaultReplicas is the default replicas for namenode and httpfs
	DefaultReplicas = 1

	// DefaultDiskNum is the default disk num
	DefaultDiskNum = 1

	// DefaultMaxWaitSeconds is the default disk num
	DefaultMaxWaitSeconds = 600

	// DefaultClusterDomainName is the default domain name key for the k8s cluster
	DefaultClusterDomainName = "clusterDomain"

	// DefaultClusterDomain is the default domain name value for the k8s cluster
	DefaultClusterDomain = "cluster.local"

	// DefaultLogVolumeName is the default log volume name
	DefaultLogVolumeName = "log"

	// DefaultConfigNameSuffix is the default config name suffix
	DefaultConfigNameSuffix = "config"

	// DefaultHeadlessSvcNameSuffix is the default headless service suffix
	DefaultHeadlessSvcNameSuffix = "headless"

	// DefaultCoreSiteFile is the default core site file name
	DefaultCoreSiteFile = "core-site.xml"

	// DefaultHdfsSiteFile is the default hdfs site file name
	DefaultHdfsSiteFile = "hdfs-site.xml"

	// DefaultHttpFSSiteFile is the default httpfs site file name
	DefaultHttpFSSiteFile = "httpfs-site.xml"

	// DefaultTerminationGracePeriod is the default time given before the
	// container is stopped. This gives clients time to disconnect from a
	// specific node gracefully.
	DefaultTerminationGracePeriod = 30

	// DefaultHdfsVolumeSize is the default volume size for the
	// Hdfs data volume
	DefaultHdfsVolumeSize = "100Gi"

	// DefaultHdfsLogVolumeSize is the default volume size for the
	// Hdfs log volume
	DefaultHdfsLogVolumeSize = "5Gi"

	// DefaultIOBufferSize is the default size for the
	// io file buffer
	DefaultIOBufferSize = "131072"

	// DefaultReadinessProbeInitialDelaySeconds is the default initial delay (in seconds)
	// for the readiness probe
	DefaultReadinessProbeInitialDelaySeconds = 40

	// DefaultReadinessProbePeriodSeconds is the default probe period (in seconds)
	// for the readiness probe
	DefaultReadinessProbePeriodSeconds = 10

	// DefaultReadinessProbeFailureThreshold is the default probe failure threshold
	// for the readiness probe
	DefaultReadinessProbeFailureThreshold = 60

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
	DefaultLivenessProbeFailureThreshold = 60

	// DefaultLivenessProbeSuccessThreshold is the default probe success threshold
	// for the readiness probe
	DefaultLivenessProbeSuccessThreshold = 1

	// DefaultLivenessProbeTimeoutSeconds is the default probe timeout (in seconds)
	// for the liveness probe
	DefaultLivenessProbeTimeoutSeconds = 10

	//HdfsProbeTypeLiveness liveness type probe
	HdfsProbeTypeLiveness = "liveness"

	//HdfsProbeTypeReadiness readiness type probe
	HdfsProbeTypeReadiness = "readiness"
)

const (
	HdfsRoleNameNode    = "namenode"
	HdfsRoleDataNode    = "datanode"
	HdfsRoleJournalNode = "journalnode"
	HdfsRoleHttpFS      = "httpfs"
	HdfsRoleAll         = "hdfs"
	HdfsHomeDir         = "/opt/hadoop"
	//HdfsConfDir               = HdfsHomeDir + "/conf"
	HdfsConfDir               = HdfsHomeDir + "/etc/hadoop"
	HdfsDataPath              = HdfsHomeDir + "/data"
	HdfsLogsDir               = HdfsHomeDir + "/logs"
	HttpFSTempDir             = HdfsHomeDir + "/temp"
	HdfsDiskPathPrefix        = "disk"
	CoreSiteDefaultConfFile   = "/hdfs/" + CurrentHdfsVersion + "/" + "core-site.xml.default"
	HdfsSiteDefaultConfFile   = "/hdfs/" + CurrentHdfsVersion + "/" + "hdfs-site.xml.default"
	HttpFSSiteDefaultConfFile = "/hdfs/" + CurrentHdfsVersion + "/" + "httpfs-site.xml.default"
)

var (
	HDFS_HA        = "false"
	JN_NODES       = ""
	JN_RPC_PORT    = "8485"
	ZK_NODES       = ""
	ZK_CLIENT_PORT = "2181"
)

var HDFSRole2Prefix = map[string]string{
	"namenode":    "nn",
	"datanode":    "dn",
	"journalnode": "jn",
	"httpfs":      "hf",
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
	"dn-rpc":   "dfs.datanode.ipc.address",
	"dn-addr":  "dfs.datanode.address",
	"dn-https": "dfs.datanode.https.address",
	"hf-http":  "httpfs.http.port",
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
	"dn-rpc":   9867,
	"dn-addr":  9866,
	"dn-https": 9865,
	"hf-http":  14000,
}

var CustomizableHdfsSiteConfKeyPrefixs = []string{
	"dfs.ha.namenodes",
	"dfs.namenode.rpc-address",
	"dfs.client.failover.proxy.provider",
}

var CustomizableCoreSiteConfKeyPrefixs = []string{
	"hadoop.security.crypto.codec.classes",
}
