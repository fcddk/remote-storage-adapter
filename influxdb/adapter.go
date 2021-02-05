package influxdb

import "github.com/fcddk/remote-storage-adapter/config"

type adapterManager struct {
	measurements map[string]*measurement
}

type adapterConfig struct {
	conf *config.Config
}
