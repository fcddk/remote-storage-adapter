package influxdb

import "strings"

var (
	measurementsWhitelist = make(map[string]bool)
)

type measurement struct {
	Name       string
	Tags       map[string]bool
	Database   string
	Fields     map[string]bool
	DropLabels map[string]bool
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
	for _, measOne := range c.adapter.measurements {
		if name == measOne.Name {
			return name, ""
		}
		if strings.HasPrefix(name, measOne.Name) {
			fName := strings.TrimPrefix(name, measOne.Name+"_")
			_, hasOk := measOne.Fields[fName]
			if hasOk {
				return measOne.Name, fName
			}
		}
	}

	return "", ""
}
