package influxdb

import "strings"

var (
	measurementsWhitelist = make(map[string]bool)
)

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
