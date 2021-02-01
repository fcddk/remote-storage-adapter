package castrate

import (
	"fmt"
	"testing"
)

func TestCastrateMetricName(t *testing.T) {
	name0 := "cpu_busy"
	measurement0, field0 := CastrateMetricName(name0)
	fmt.Printf("measurement:%s, field:%s", measurement0, field0)
}
