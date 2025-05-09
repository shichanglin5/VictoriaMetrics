package mts

import (
	"context"
	"github.com/prometheus/common/model"
	"net/http"
	"sync/atomic"
)

type MtsClientType struct {
	InitFunc  func() error
	StartFunc func() error
	StopFunc  func() error
}

// MtsResponse TargetsResult 请求targets结果
type MtsResponse[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Result  T      `json:"result"`
}

type MtsClientConfig struct {
	HeartbeatAddrs  []string `json:"heartbeatAddrs"`
	PullTargetsAddr *string  `json:"pullTargetsAddr"` // for vmagent
}

type MtsClient struct {
	Addr       string
	httpClient *http.Client
	preStopCh  chan struct{}
	stopCh     <-chan struct{}
	cancelFunc context.CancelFunc

	// heartbeat to mts with 'stopping' flag (set ts to -1)
	stopping atomic.Bool
}

// MtsGetTargetsRequest MtsTargets mts heartbeat response
type MtsGetTargetsRequest struct {
	Ident  string `json:"ident"`
	Ts     int64  `json:"ts"`
	Sign   string `json:"sign"`
	Tenant string `json:"tenant"`
	Group  string `json:"group"`
}

// PullTargetResult Result 数据结构体
type PullTargetResult struct {
	Data []TargetGroup `json:"data"`
	Sign string        `json:"sign"`
}

// TargetGroup 目标targets
type TargetGroup struct {
	Targets      []string          `json:"targets"`
	Labels       map[string]string `json:"labels"`
	ScrapeConfig *MtsScrapeConfig  `json:"scrapeConfig,omitempty"`
}

// MtsScrapeConfig 采集配置
type MtsScrapeConfig struct {
	ScrapeIdc      string         `json:"scrapeIdc,omitempty"`
	ScrapeTimeout  model.Duration `json:"scrapeTimeout,omitempty"`
	ScrapeInterval model.Duration `json:"scrapeInterval,omitempty"`
	MaxScrapeSize  string         `json:"maxScrapeSize,omitempty"`
	Tenant         string         `json:"tenant,omitempty"`
}
