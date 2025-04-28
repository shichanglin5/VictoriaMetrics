package config

import (
	"context"
	"errors"
	"regexp"

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
	MetricsMatchBlockedTotal    = metrics.NewCounter(`vmselect_match_query_blocked_total`)
	MetricsNotMatchBlockedTotal = metrics.NewCounter(`vmselect_notmatch_query_blocked_total`)
)
var ErrBlockedQuery = errors.New("query is blocked! contact administator for more information")
var ShouldBlockQuery func(string) bool

type Labels map[string]struct{}

func (l Labels) Contains(label string) bool {
	_, ok := l[label]
	return ok
}

type VMSelectConfig struct {
	BlockedQueries      *BlockedQueries `yaml:"blockedQueries,omitempty"`
	TreatDotsAsIsLabels []string        `yaml:"treatDotsAsIsLabels,omitempty"`
}

type BlockedQueries struct {
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
		loadQueryBlockRules(c.BlockedQueries)

		// 设置 VMSelectConfigVar
		VMSelectConfigVar.Store(&c)
		return nil
	})
}

func loadQueryBlockRules(blockRules *BlockedQueries) {
	if blockRules == nil {
		queryBlockEnabled = false
		logger.Infof("blockedQueries is empty, skip load")
		ShouldBlockQuery = func(string) bool {
			return false
		}
		return
	}
	BlockIfMatchRules := make([]*regexp.Regexp, 0)
	if blockRules.BlockIfMatch != nil {
		for _, regxRule := range blockRules.BlockIfMatch {
			compile, err := regexp.Compile(regxRule)
			if err != nil {
				logger.Warnf("cannot compile regular expression %q: %s", regxRule, err)
				continue
			}
			if compile != nil {
				BlockIfMatchRules = append(BlockIfMatchRules, compile)
			}
		}
	}
	BlockIfNotMatchRules := make([]*regexp.Regexp, 0)
	if blockRules.BlockIfNotMatch != nil {
		for _, regxRule := range blockRules.BlockIfNotMatch {
			compile, err := regexp.Compile(regxRule)
			if err != nil {
				logger.Warnf("cannot compile regular expression %q: %s", regxRule, err)
				continue
			}
			if compile != nil {
				BlockIfNotMatchRules = append(BlockIfNotMatchRules, compile)
			}
		}
	}

	if len(BlockIfMatchRules) == 0 && len(BlockIfNotMatchRules) == 0 {
		queryBlockEnabled = false
		ShouldBlockQuery = func(string) bool {
			return false
		}
	}

	queryBlockEnabled = true
	ShouldBlockQuery = func(queryStr string) bool {
		for _, matchRule := range BlockIfMatchRules {
			// 必须匹配规则，否则屏蔽（返回 true）
			if matchRule.Match([]byte(queryStr)) {
				MetricsMatchBlockedTotal.Inc()
				return true
			}
		}
		for _, blockRule := range BlockIfNotMatchRules {
			// 只要符合 blockRule，则屏蔽（返回 true)
			if !blockRule.Match([]byte(queryStr)) {
				MetricsNotMatchBlockedTotal.Inc()
				return true
			}
		}
		return false
	}
}
