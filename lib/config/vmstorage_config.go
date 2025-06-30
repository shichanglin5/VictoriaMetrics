package config

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"
	"math"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var (
	VMStorageConfigVar = atomic.Pointer[VMStorageConfig]{}
)

func GetVmStorageConfig() *VMStorageConfig {
	return VMStorageConfigVar.Load()
}

type VMStorageConfig map[string]*TenantConfig

type TenantConfig struct {
	vmAccountId              uint32
	vmProjectId              uint32
	minDownsampleTimeAfterMs int64
	retentionPeriodMs        int64
	dedupIntervalMs          int64

	DudepInterval       *model.Duration   `yaml:"DudepInterval,omitempty"`
	RetentionPeriod     *model.Duration   `yaml:"retentionPeriod,omitempty"`
	TimeSeriesLimit     int64             `yaml:"timeSeriesLimit,omitempty"`
	DownsamplingConfigs []*DumplingConfig `yaml:"downsamplingConfigs,omitempty"`
}

func (cfg *TenantConfig) GetMinDownsampleTimeAfterMs() int64 {
	return cfg.minDownsampleTimeAfterMs
}

func (cfg *TenantConfig) GetRetentionDeadline() int64 {
	return cfg.retentionPeriodMs
}

func (cfg *TenantConfig) GetDedupInterval() int64 {
	return cfg.retentionPeriodMs
}

type DumplingConfig struct {
	timeBeforeMs int64
	intervalMs   int64
	metricIds    *uint64set.Set
	tsFilterExpr *metricsql.Expr

	TsFilter   string          `yaml:"ts_filter,omitempty"`
	TimeBefore *model.Duration `yaml:"time_before,omitempty"`
	Interval   *model.Duration `yaml:"interval,omitempty"`
	Method     string          `yaml:"method,omitempty"`
}

func (dc *DumplingConfig) Match(maxTs int64, tsId storage.TSID) bool {
	// 如何再配置规则的时间之前，则跳过（eg: 规则针对30d以前的时序生效，那么 30d内的则匹配失败）
	if maxTs > dc.timeBeforeMs {
		return false
	}

	// metricsIds 为空则表示租户下都生效，否则需要对应 metricIds 包含该 tsId
	return dc.metricIds == nil || dc.metricIds.Has(tsId.MetricID)
}

func (dc *DumplingConfig) GetDedupInterval() int64 {
	return dc.intervalMs
}

func InitVMStorageConfig() (context.CancelFunc, error) {
	go func() {
		tick := time.NewTicker(time.Hour)
		for {
			select {
			case <-tick.C:
				tenantConfigs := VMStorageConfigVar.Load()
				reloadTenantConfigs(*tenantConfigs)
			}
		}
	}()
	return LoadConfig(func(data []byte) error {
		var c VMStorageConfig
		if err := yaml.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("cannot unmarshal vmselect config: %w", err)
		}
		err := reloadTenantConfigs(c)
		if err != nil {
			return err
		}
		VMStorageConfigVar.Store(&c)
		return nil
	})
}

func reloadTenantConfigs(tenantConfigs VMStorageConfig) error {
	for tenant, tenantConfig := range tenantConfigs {
		err := reloadTenantConfig(tenant, tenantConfig)
		if err != nil {
			logger.Errorf("cannot reload tenant config for tenant %s: %v", tenant, err)
			return err
		}
	}
	VMStorageConfigVar.Store(&tenantConfigs)
	return nil
}

func refreshRuntimeData(tenantConfigs VMStorageConfig) error {
	for _, tenantConfig := range tenantConfigs {
		// 更新 retentionDeadline
		if tenantConfig.RetentionPeriod != nil {
			tenantConfig.retentionPeriodMs = time.Now().UnixMilli() - time.Duration(*tenantConfig.RetentionPeriod).Milliseconds()
		} else {
			tenantConfig.retentionPeriodMs = 0
		}

		// 更新 downsamplingTs
		for _, downsamplingConfig := range tenantConfig.DownsamplingConfigs {
			if downsamplingConfig.TimeBefore != nil {
				downsamplingConfig.timeBeforeMs = time.Now().UnixMilli() - time.Duration(*downsamplingConfig.TimeBefore).Milliseconds()
			} else {
				logger.Errorf("refresh vmstorage runtimeData: invalid downsamplingConfig.TimeBefore: %v", downsamplingConfig.TimeBefore)
			}
		}
	}
	return nil
}

func reloadTenantConfig(tenantToken string, config *TenantConfig) error {
	authToken, err := auth.NewToken(tenantToken)
	if err != nil {
		return fmt.Errorf("parse tenantToken failed: %v", err)
	}
	config.vmAccountId = authToken.AccountID
	config.vmProjectId = authToken.ProjectID

	if len(config.DownsamplingConfigs) > 0 {
		var minTimeAfterDuration model.Duration

		validDownsampleConfigs := make([]*DumplingConfig, 0, len(config.DownsamplingConfigs))
		for _, dc := range config.DownsamplingConfigs {
			if dc.TimeBefore == nil || *dc.TimeBefore <= 0 {
				return fmt.Errorf("load tenant config failed: downsample timeAfter is invalid: %v", dc.TimeBefore)
			}
			if dc.Interval == nil || *dc.Interval <= 0 {
				return fmt.Errorf("load tenant config failed: downsample interval is invalid: %v", dc.Interval)
			}

			// 校验 tsFilter
			var parsedExpr metricsql.Expr
			emptyTsFilter := true
			if len(dc.TsFilter) > 0 {
				expr, err := metricsql.Parse(dc.TsFilter)
				if err != nil {
					return fmt.Errorf("load tenant config failed: failed to parse tsfilter expression: %v", err)
				}
				me, ok := expr.(*metricsql.MetricExpr)
				if !ok {
					return fmt.Errorf("expecting metricSelector; got %q", expr.AppendString(nil))
				}
				if len(me.LabelFilterss) > 0 {
					emptyTsFilter = false
					tenantFilter := fmt.Sprintf("{vm_account_id=\"%d\",vm_project_id=\"%d\"}", config.vmAccountId, config.vmProjectId)
					parsedExpr, _ = metricsql.Parse(tenantFilter)
					metricMetricExpr := parsedExpr.(*metricsql.MetricExpr)
					for i := range metricMetricExpr.LabelFilterss {
						for _, lfs := range me.LabelFilterss {
							metricMetricExpr.LabelFilterss[i] = append(metricMetricExpr.LabelFilterss[i], lfs...)
						}
					}

				}
			}
			// 如果没有配置 filter，则清空
			if emptyTsFilter {
				dc.metricIds = nil
				dc.tsFilterExpr = nil
			}

			// 更新 timeBeforeMs
			dc.timeBeforeMs = time.Now().UnixMilli() - time.Duration(*dc.Interval).Milliseconds()
			dc.intervalMs = time.Duration(*dc.Interval).Milliseconds()
			if minTimeAfterDuration > *dc.TimeBefore {
				minTimeAfterDuration = *dc.TimeBefore
			}

			// 去掉空格
			dc.TsFilter = strings.TrimSpace(dc.TsFilter)
			validDownsampleConfigs = append(validDownsampleConfigs, dc)
		}

		if config.DudepInterval != nil && *config.DudepInterval > 0 {
			config.dedupIntervalMs = time.Duration(*config.DudepInterval).Milliseconds()
		} else {
			config.dedupIntervalMs = 0
		}

		// 排序优先级
		// 1. TsFilter 非空优先级更高，配置为空表示整个租户维度，非空则表示针对匹配成功的时序配置
		// 2. Interval 采样间隔越大优先级越高，eg: 如果有个采样间隔为5m，另一个采样间隔为1m，则优先使用5m采样
		// 3. timeAfter 越大优先级越高，eg: 如果匹配上 60d 的采样规则，那么不会再匹配 30d 的采样规则
		sort.SliceStable(validDownsampleConfigs, func(i, j int) bool {
			c1 := config.DownsamplingConfigs[i]
			c2 := config.DownsamplingConfigs[j]
			if c1.TsFilter == c2.TsFilter {
				if c1.timeBeforeMs == c2.timeBeforeMs {
					return c1.intervalMs > c2.intervalMs
				}
				return c1.timeBeforeMs > c2.timeBeforeMs
			}
			// filter对应promql越长，则假设越精确，优先级更高
			// filter对应promql为0，表示整个租户范围生效，优先级最低
			return len(c1.TsFilter) > len(c2.TsFilter)
		})

		// 刷新租户 retentionDeadline，如果为空，则设置为0
		if config.RetentionPeriod != nil {
			config.retentionPeriodMs = time.Now().UnixMilli() - time.Duration(*config.RetentionPeriod).Milliseconds()
		} else {
			config.retentionPeriodMs = 0
		}
		config.minDownsampleTimeAfterMs = time.Now().UnixMilli() - time.Duration(minTimeAfterDuration).Milliseconds()
		config.DownsamplingConfigs = validDownsampleConfigs
	}
	return nil
}
