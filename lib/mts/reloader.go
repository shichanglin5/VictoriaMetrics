package mts

import (
	"errors"
	"path"
)

type VmAgentConfig struct {
	WriteIdcUrlMapping        map[string]string `yaml:"writeIdcUrlMapping"`
	TenantAuthTokenMapping    map[string]string `yaml:"tenantAuthTokenMapping"`
	TenantWriteIdcListMapping map[string]string `yaml:"tenantWriteIdcListMapping"`
}

func LoadVmAgentConfig(configHandler func(configData *VmAgentConfig)) error {
	reqUrl := path.Join(MtsUrl, "config/vm/vm_agent_remotewrite.yaml")
	return GetRequest(Cli, reqUrl, nil, func(respData *[]byte, mtsResult *MtsResponse[*VmAgentConfig]) error {
		switch mtsResult.Code {
		case 0:
			// targets 发生变化，需要解析
			result := mtsResult.Result
			configHandler(result)
			return nil
		default:
			MtsWarningMetrics.Inc()
			return errors.New(mtsResult.Message)
		}
	})
}
