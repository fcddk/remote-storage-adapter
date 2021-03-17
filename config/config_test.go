package config

import (
	"fmt"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Parse a valid file that sets a global scrape timeout. This tests whether parsing
	// an overwritten default field in the global config permanently changes the default.
	//expectedConf := &Config{GlobalConfig: GlobalConfig{
	//	MeasurementsWhitelist: []string{"region","kubernetes_cluster_name","host","instanceId","resource_id","disk_id","path","interface","name","container","pod_name"},
	//	TagsWhitelist:        []string{"region","kubernetes_cluster_name","host","instanceId","resource_id","disk_id","path","interface","name","container","pod_name"},
	//}}
	conf, err := LoadFile("testdata/global_good.yml")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(conf)

	//testutil.Ok(t, err)
	//testutil.Equals(t, c.GlobalConfig.MeasurementsWhitelist, expectedConf.GlobalConfig.MeasurementsWhitelist)
	//testutil.Equals(t, c.GlobalConfig.TagsWhitelist, expectedConf.GlobalConfig.TagsWhitelist)
}
