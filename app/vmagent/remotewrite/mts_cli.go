package remotewrite

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mts"
	"time"
)

func init() {
	mts.RegisterMtsClient("pushgateway", nil, StartVmAgentConfig, nil)
}

// StartVmAgentConfig ：agent 和 pushgateway 都需要从 mts 加载 remote write 配置
func StartVmAgentConfig() error {
	// load config
	reloadMtsConfig()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		go func() {
			for {
				select {
				case <-mts.Cli.PreStopCh():
					logger.Warnf("mts stop load config.")
					return
				case <-ticker.C:
					reloadMtsConfig()
				}
			}
		}()
	}()
	return nil
}

func reloadMtsConfig() {
	_ = mts.LoadVmAgentConfig(func(vmConfigs *mts.VmAgentConfig) {
		if vmConfigs == nil {
			mts.MtsWarningMetrics.Inc()
			logger.Warnf("cannot reload vmagent config is empty, skip reload")
			return
		}

		// auth tokens
		newClusterUrls := vmConfigs.ClusterUrls
		if newClusterUrls == nil || len(newClusterUrls) == 0 {
			mts.MtsWarningMetrics.Inc()
			logger.Warnf("cannot reload vmagent config, vmClusterUrls is empty")
		}

		newTenantToAuthTokens := vmConfigs.TenantToAuthTokens
		if newTenantToAuthTokens == nil || len(newTenantToAuthTokens) == 0 {
			mts.MtsWarningMetrics.Inc()
			logger.Warnf("cannot reload vmagent config, VmTenantToAuthTokens is empty")
		}

		validTenantToAuthTokens := make(map[string]string, len(newTenantToAuthTokens))
		for k, v := range newTenantToAuthTokens {
			_, err := auth.NewToken(v)
			if err != nil {
				logger.Warnf("invalid token %s: %v", v, err)
				continue
			}
			validTenantToAuthTokens[k] = v
		}

		vmClusterSet := make(map[string]string, len(newClusterUrls))
		for k, v := range newClusterUrls {
			if prevK, ok := vmClusterSet[v]; !ok {
				vmClusterSet[v] = k
			} else {
				logger.Warnf("found duplicate vm cluster url: %s", v)
				// 当出现重复的vm cluster url，根据字典排序选择最小的idc
				if k < prevK {
					vmClusterSet[v] = k
				}
			}
		}
		validClusterUrls := make(map[string]string, len(vmClusterSet))
		for k, v := range vmClusterSet {
			validClusterUrls[v] = k
		}

		ReloadRemoteWriteCtxs(validTenantToAuthTokens, validClusterUrls)
	})
}
