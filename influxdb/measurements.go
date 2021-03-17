package influxdb

import (
	"fmt"
	"strings"
)

var (
	measurementsWhitelist = make(map[string]bool)
)

type measurement struct {
	Name             string
	Tags             map[string]bool
	Database         string
	Fields           map[string]bool
	IgnoreOtherLabel bool
	DropLabels       map[string]bool
}

func (m *measurement) String() string {
	return fmt.Sprintf("name:%s,database:%s,tags:%v,fields:%v,drop_label:%v", m.Name, m.Database, m.Tags, m.Fields, m.DropLabels)
}

func UpdateMeasurementsWhitelist(whitelist []string) {
	for _, measure := range whitelist {
		measurementsWhitelist[measure] = true
	}
}

func hasMeasurement(name string) string {
	depth := 0
	depthStr := ""
	for key, _ := range measurementsWhitelist {
		if strings.HasPrefix(name, key) && len(key) > depth {
			depth = len(key)
			depthStr = key
		}
	}
	return depthStr
}

func (c *Client) checkSampleBelongToMeasurement(name string) (measurementName string, fieldName string) {
	if c.databases == nil {
		return
	}
	for _, dbManager := range c.databases {
		if dbManager.metrics == nil {
			continue
		}
		measure, hasOk := dbManager.metrics[name]
		if hasOk {
			if name == measure {
				return measure, ""
			}
			field := strings.TrimPrefix(name, measure)
			if strings.HasPrefix(field, "_") {
				fieldName = strings.TrimPrefix(field, "_")
			} else {
				fieldName = field
			}
			measurementName = measure
			return
		}
	}

	return "", ""
}
