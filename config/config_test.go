package config

import (
	"remote_storage_adpter/util/testutil"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Parse a valid file that sets a global scrape timeout. This tests whether parsing
	// an overwritten default field in the global config permanently changes the default.
	expectdConf := &Config{GlobalConfig: GlobalConfig{
		MeasurementsWhitelist: "mem,cpu,disk,container_cpu_cfs_throttled,container_cpu,container_memory,container_fs,container_fs_reads,container_fs_writes,container_network_receive,container_network_transmit",
		TagsWhitelist:         "region,kubernetes_cluster_name,host,instanceId,resource_id,disk_id,path,interface,name,container,pod_name",
	}}
	c, err := LoadFile("testdata/global_timeout.good.yml")
	testutil.Ok(t, err)
	testutil.Equals(t, c.GlobalConfig.MeasurementsWhitelist, expectdConf.GlobalConfig.MeasurementsWhitelist)
	testutil.Equals(t, c.GlobalConfig.TagsWhitelist, expectdConf.GlobalConfig.TagsWhitelist)
}
