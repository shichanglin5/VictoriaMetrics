package remotewrite

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/config"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mts"
	"github.com/VictoriaMetrics/metrics"
	"sync/atomic"
)

var (
	FastQueueSize atomic.Int64
	_             = metrics.NewGauge(`vmagent_fast_queue_size`, func() float64 {
		return float64(FastQueueSize.Load())
	})
)

func loadRemoteWriteConfig(vmConfigs *config.VmAgentConfig) error {
	if vmConfigs == nil {
		mts.MtsWarningMetrics.Inc()
		return errors.New("cannot reload vmagent config")
	}

	WriteIdcUrlMapping := vmConfigs.WriteIdcUrlMapping
	if WriteIdcUrlMapping == nil || len(WriteIdcUrlMapping) == 0 {
		mts.MtsWarningMetrics.Inc()
		return fmt.Errorf("cannot reload vmagent config is empty, skip reload")
	}

	// TenantAuthTokenMapping
	TenantAuthTokenMapping := vmConfigs.TenantAuthTokenMapping
	if TenantAuthTokenMapping == nil || len(TenantAuthTokenMapping) == 0 {
		mts.MtsWarningMetrics.Inc()
		return fmt.Errorf("cannot reload vmagent config, VmTenantToAuthTokens is empty")
	}

	checkedTenantAuthTokenMapping := make(map[string]string, len(TenantAuthTokenMapping))
	for k, v := range TenantAuthTokenMapping {
		_, err := auth.NewToken(v)
		if err != nil {
			logger.Warnf("invalid token %s: %v", v, err)
			continue
		}
		checkedTenantAuthTokenMapping[k] = v
	}

	// WriteIdcUrlMapping
	vmClusterSet := make(map[string]string, len(WriteIdcUrlMapping))
	for k, v := range WriteIdcUrlMapping {
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
	checkedWriteIdcUrlMapping := make(map[string]string, len(vmClusterSet))
	for k, v := range vmClusterSet {
		checkedWriteIdcUrlMapping[v] = k
	}

	// TenantWriteIdcListMapping
	TenantWriteIdcListMapping := vmConfigs.TenantWriteIdcListMapping
	checkedTenantWriteIdcListMapping := make(map[string]map[string]struct{}, len(TenantWriteIdcListMapping))
	for tenant, tenantWriteIdcList := range TenantWriteIdcListMapping {
		if len(tenantWriteIdcList) == 0 {
			// 如果配置为 "",则表示默认不写数据
			continue
		} else {
			tenantIdcsSet := make(map[string]struct{})
			for _, tenantIdc := range tenantWriteIdcList {
				tenantIdcsSet[tenantIdc] = struct{}{}
			}
			checkedTenantWriteIdcListMapping[tenant] = tenantIdcsSet
		}
	}

	ReloadRemoteWriteCtxs(checkedTenantAuthTokenMapping, checkedWriteIdcUrlMapping, checkedTenantWriteIdcListMapping)
	return nil
}
