package influxdb

import "fmt"

type databasesManager struct {
	name         string
	metrics      map[string]string //key:metric_name, value: measurement_name
	measurements map[string]*measurement
}

func (d *databasesManager) String() string {
	info := "[" + "name:" + d.name + fmt.Sprintf(",metrics:%v", d.metrics) + fmt.Sprintf(",measurements:%v", d.measurements) + "]"
	return info
}
