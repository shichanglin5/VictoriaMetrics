package config

import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"
	"sync/atomic"
)

var (
	VMStorageConfigVar = atomic.Pointer[VMStorageConfig]{}
)

type VMStorageConfig struct {
	TenantRetentionPeriod map[string]model.Duration `yaml:"tenantRetentionPeriod,omitempty"`
	TenantTimeSeriesLimit map[string]struct {
		MaxDailySeries  int `yaml:"maxDailySeries,omitempty"`
		MaxHourlySeries int `yaml:"maxHourlySeries,omitempty"`
	} `yaml:"tenantTimeSeriesLimit,omitempty"`
}

func InitVMStorageConfig() (context.CancelFunc, error) {
	return LoadConfig(func(data []byte) error {
		var c VMStorageConfig
		if err := yaml.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("cannot unmarshal vmselect config: %w", err)
		}
		VMStorageConfigVar.Store(&c)
		return nil
	})
}
