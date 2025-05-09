package mts

import (
	"net/http"
)

const (
	VmTenantToAuthTokens = "vmclusters_tenantToAuthTokens"
	VmTenantToIdcs       = "vmclusters_tenantToIdcs"
	VmClusterUrls        = "vmclusters_urls"
)

type VmAgentConfig struct {
	ClusterUrls        map[string]string `json:"vmclusters_urls"`
	TenantToAuthTokens map[string]string `json:"vmclusters_tenantToAuthTokens"`
	TenantToIdcs       map[string]string `json:"vmclusters_tenantToIdcs"`
}

func LoadVmAgentConfig(configHandler func(configData *VmAgentConfig)) error {
	return LoadConfig([]string{VmTenantToAuthTokens, VmClusterUrls, VmTenantToIdcs},
		func(r *http.Request) {
			query := r.URL.Query()
			query.Set("multiGet", "true")
			r.URL.RawQuery = query.Encode()
		},
		configHandler)
}
