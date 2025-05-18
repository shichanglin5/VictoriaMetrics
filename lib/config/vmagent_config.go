package config

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v3"
	"sync/atomic"
)

var (
	VMAgentConfigVar = atomic.Pointer[VmAgentConfig]{}
)

type VmAgentConfig struct {
	WriteIdcUrlMapping        map[string]string   `yaml:"writeIdcUrlMapping"`
	TenantAuthTokenMapping    map[string]string   `yaml:"tenantAuthTokenMapping"`
	TenantWriteIdcListMapping map[string][]string `yaml:"tenantWriteIdcListMapping"`
}

func InitVMAgentConfig(configHandler func(*VmAgentConfig) error) (context.CancelFunc, error) {
	return LoadConfig(func(data []byte) error {
		var c VmAgentConfig
		if err := yaml.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("cannot unmarshal vmagent config: %w", err)
		}
		VMAgentConfigVar.Store(&c)

		if configHandler != nil {
			err := configHandler(&c)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
