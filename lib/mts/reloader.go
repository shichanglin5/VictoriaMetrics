package mts

import (
	"net/http"
)

const (
	VmTenantToAuthTokens = "vmclusters_tenantToAuthTokens"
	VmClusterUrls        = "vmclusters_urls"
)

type VmAgentConfig struct {
	ClusterUrls        map[string]string `json:"vmclusters_urls"`
	TenantToAuthTokens map[string]string `json:"vmclusters_tenantToAuthTokens"`
}

func LoadVmAgentConfig(configHandler func(configData *VmAgentConfig)) error {
	return LoadConfig([]string{VmTenantToAuthTokens, VmClusterUrls},
		func(r *http.Request) {
			query := r.URL.Query()
			query.Set("multiGet", "true")
			r.URL.RawQuery = query.Encode()
		},
		configHandler)
}
