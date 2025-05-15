package config

import (
	"context"
	"errors"
	"regexp"
	"strings"

	//"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"gopkg.in/yaml.v3"
	"sync/atomic"
)

var (
	VMSelectConfigVar           = atomic.Pointer[VMSelectConfig]{}
	VMSelectTreatDotsAsIsLabels = atomic.Pointer[Labels]{}

	queryBlockEnabled = false
	_                 = metrics.NewGauge("vmselect_query_blocked_enabled", func() float64 {
		if queryBlockEnabled {
			return 1
		} else {
			return 0
		}
	})
	MetricsQueryContainsPassTotal    = metrics.NewCounter(`vmselect_query_containsPass_total`)
	MetricsQueryContainsBlockTotal   = metrics.NewCounter(`vmselect_query_containsBlock_total`)
	MetricsQueryMatchBlockedTotal    = metrics.NewCounter(`vmselect_query_matchBlocked_total`)
	MetricsQueryNotMatchBlockedTotal = metrics.NewCounter(`vmselect_query_notMatchBlocked_total`)
)
var ErrBlockedQuery = errors.New("query is blocked! contact administator for more information")
var ShouldBlockQuery func(string) bool

type Labels map[string]struct{}

func (l Labels) Contains(label string) bool {
	_, ok := l[label]
	return ok
}

type VMSelectConfig struct {
	QueryBlockedRules   []*QueryBlockedRules `yaml:"queryBlockedRules,omitempty"`
	TreatDotsAsIsLabels []string             `yaml:"treatDotsAsIsLabels,omitempty"`
}

type QueryBlockedRules struct {
	Predication string `yaml:"prediction,omitempty"`

	IgnoreIfContains []string `yaml:"ignoreIfContains,omitempty"`
	BlockIfContains  []string `yaml:"blockIfContains,omitempty"`

	BlockIfMatch    []string `yaml:"blockIfMatch,omitempty"`
	BlockIfNotMatch []string `yaml:"blockIfNotMatch,omitempty"`
}

func InitVMSelectConfig() (context.CancelFunc, error) {
	return LoadConfig(func(data []byte) error {
		var c VMSelectConfig
		if err := yaml.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("cannot unmarshal vmselect config: %w", err)
		}

		// 设置 VMSelectTreatDotsAsIsLabels
		if len(c.TreatDotsAsIsLabels) > 0 {
			treatDotsAsIsLabelsMaps := make(Labels, len(c.TreatDotsAsIsLabels))
			for _, labelName := range c.TreatDotsAsIsLabels {
				treatDotsAsIsLabelsMaps[labelName] = struct{}{}
			}
			VMSelectTreatDotsAsIsLabels.Store(&treatDotsAsIsLabelsMaps)
		}

		// 加载 block query 规则
		loadQueryBlockRules(c.QueryBlockedRules)

		// 设置 VMSelectConfigVar
		VMSelectConfigVar.Store(&c)
		return nil
	})
}

const (
	// PredictPass 配置 ignore 规则，命中的话直接放行
	PredictPass byte = 0

	// PredictContinue 继续执行其他规则，判断是否屏蔽此次请求
	PredictContinue byte = 1

	// PredictProhibit 直接屏蔽请求
	PredictProhibit byte = 2
)

func loadQueryBlockRules(blockRules []*QueryBlockedRules) {
	if len(blockRules) == 0 {
		queryBlockEnabled = false
		logger.Infof("blockedQueries is empty, skip load")
		ShouldBlockQuery = func(string) bool {
			return false
		}
		return
	}
	BlockPredicationFuncs := make([]func(string) byte, 0, len(blockRules))
	for _, blockRule := range blockRules {
		predication := strings.TrimSpace(blockRule.Predication)
		var predicationFunc func(string) bool
		if len(predication) > 0 {
			if predication == ".*" {
				predicationFunc = func(s string) bool {
					return true
				}
			} else {
				predicationRegx, err := regexp.Compile(blockRule.Predication)
				if err != nil {
					logger.Warnf("cannot compile blockRule.Predication(%s) error: %s", predication, err)
					continue
				}
				predicationFunc = func(s string) bool {
					return predicationRegx.MatchString(s)
				}
			}
		} else {
			logger.Warnf("ignore block rule: blockRule.Predication is empty!")
			continue
		}
		blockIfMatchRules := make([]*regexp.Regexp, 0, len(blockRule.BlockIfMatch))
		for _, blockIfMatch := range blockRule.BlockIfMatch {
			if len(strings.TrimSpace(blockIfMatch)) > 0 {
				blockIfMatchRegx, err := regexp.Compile(blockIfMatch)
				if err != nil {
					logger.Warnf("cannot compile blockRule.BlockIfMatch (%s) error: %s, prediction:(%s)", blockIfMatch, err, predication)
					continue
				}
				blockIfMatchRules = append(blockIfMatchRules, blockIfMatchRegx)
			}
		}
		blockIfNotMatchRules := make([]*regexp.Regexp, 0, len(blockRule.BlockIfNotMatch))
		for _, blockIfNotMatch := range blockRule.BlockIfNotMatch {
			if len(strings.TrimSpace(blockIfNotMatch)) > 0 {
				blockIfNotMatchRegx, err := regexp.Compile(blockIfNotMatch)
				if err != nil {
					logger.Warnf("cannot compile blockRule.BlockIfNotMatch (%s) error: %s, prediction:(%s)", blockIfNotMatch, err, predication)
					continue
				}
				blockIfNotMatchRules = append(blockIfNotMatchRules, blockIfNotMatchRegx)
			}
		}
		if len(blockIfMatchRules) == 0 && len(blockIfNotMatchRules) == 0 {
			continue
		}
		// 返回 true 表示需要拒绝此次查询
		BlockPredicationFuncs = append(BlockPredicationFuncs, func(queryStr string) byte {
			if predicationFunc(queryStr) {
				if len(blockRule.IgnoreIfContains) > 0 {
					for _, ignoreCase := range blockRule.IgnoreIfContains {
						// 如果包含 ignore case，则直接跳过，不屏蔽当前查询
						if strings.Contains(queryStr, ignoreCase) {
							MetricsQueryContainsPassTotal.Inc()
							return PredictPass
						}
					}
				}
				if len(blockRule.BlockIfContains) > 0 {
					for _, blockCase := range blockRule.BlockIfContains {
						// 如果包含 block case 直接屏蔽
						if strings.Contains(queryStr, blockCase) {
							MetricsQueryContainsBlockTotal.Inc()
							return PredictProhibit
						}
					}
				}
				if len(blockIfMatchRules) > 0 {
					for _, regxRuleCompile := range blockIfMatchRules {
						if regxRuleCompile.MatchString(queryStr) {
							MetricsQueryMatchBlockedTotal.Inc()
							return PredictProhibit
						}
					}
				}
				if len(blockIfNotMatchRules) > 0 {
					for _, regxRuleCompile := range blockIfNotMatchRules {
						if !regxRuleCompile.MatchString(queryStr) {
							MetricsQueryNotMatchBlockedTotal.Inc()
							return PredictProhibit
						}
					}
				}
			}
			return PredictContinue
		})
	}

	if len(BlockPredicationFuncs) == 0 {
		queryBlockEnabled = false
		logger.Infof("blockedQueries is empty, skip load")
	} else {
		queryBlockEnabled = true
		ShouldBlockQuery = func(queryStr string) bool {
			for _, predicationFunc := range BlockPredicationFuncs {
				switch predicationFunc(queryStr) {
				case PredictPass:
					return false
				case PredictProhibit:
					return true
				default:
					continue
				}
			}
			return false
		}
	}
}
