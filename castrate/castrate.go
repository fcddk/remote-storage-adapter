package castrate

import (
	"strings"
)

func CastrateMetricName(name string) (string, string) {
	//measurementName :=
	strings.Trim(name, " ")
	if name == "" {
		return "", ""
	}

	if strings.Contains(name, "_") {
		point := findCutPoint(name)
		if point <= 0 {
			return name, ""
		}
		return name[:point], name[point+1:]
	}
	return name, ""
}

func findCutPoint(name string) int {
	var point int = 0
	for i := len(name) - 1; i > 0; i-- {
		if name[i] == '_' {
			point = i
			break
		}
	}

	return point
}
