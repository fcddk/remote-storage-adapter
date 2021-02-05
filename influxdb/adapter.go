package influxdb

import (
	"github.com/fcddk/remote-storage-adapter/config"
)

type adapterManager struct {
	measurements map[string]*measurement
}

func (ada *adapterManager) String() string {
	var info string
	info = "["
	for _, meaOne := range ada.measurements {
		info = info + meaOne.String()
	}
	info = info + "]"
	return info
}

type adapterConfig struct {
	conf *config.Config
}
