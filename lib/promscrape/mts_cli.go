package promscrape

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
	"github.com/VictoriaMetrics/metrics"
	"go.uber.org/atomic"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	apiIp        = "/stats/ip"
	apiHeartbeat = "/stats/heartbeat"
	apiGetTarget = "/stats/getTargets"
	jobLabel     = "job"
)

var (
	// metrics
	mtsHeartbeatFailedMetric  = metrics.NewCounter(`vmagent_mts_heartbeat_failed`)
	mtsHeartbeatSuccessMetric = metrics.NewCounter(`vmagent_mts_heartbeat_success`)

	mtsPullTargetsNoChangeMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_nochange`)
	mtsPullTargetsSuccessMetric  = metrics.NewCounter(`vmagent_mts_pull_targets_counter_success`)
	mtsPullTargetsFailedMetric   = metrics.NewCounter(`vmagent_mts_pull_targets_counter_failed`)
	targetSize                   atomic.Int64
	_                            = metrics.NewGauge(`vmagent_mts_pull_targets_size`, func() float64 {
		return float64(targetSize.Load())
	})

	// init from ip addr
	ident string

	// env
	tenant      string
	mtsUrl      string
	addIdentTag bool
	addUrlTag   bool

	// runtime vars
	sign = &Md5Sign{Md5: "", Timestamp: 0}
)

var globalConfig = &GlobalConfig{
	ScrapeInterval: promutils.NewDuration(15 * time.Second),
	ScrapeTimeout:  promutils.NewDuration(10 * time.Second),
	ExternalLabels: nil,
}

var tenantMap = map[string]string{
	"default":        "1:0",
	"inf-esproxy":    "1:1",
	"inf-dbproxy":    "1:2",
	"inf-redisproxy": "1:3",
	"inf-pay":        "1:4",
}

func init() {
	// init env vars
	tenant = os.Getenv("TENANT")
	if len(tenant) == 0 {
		panic("environment variable TENANT not set")
	}
	mtsUrl = os.Getenv("MTS_URL")
	if len(mtsUrl) == 0 {
		panic("environment variable MTS_URL not set")
	}
	mtsUrl = strings.TrimSuffix(mtsUrl, "/")
	// extra tags
	addIdentTag = len(os.Getenv("IDENT_TAG")) > 0
	addUrlTag = len(os.Getenv("URL_TAG")) > 0
}

// MtsGetTargetsRequest MtsTargets mts heartbeat response
type MtsGetTargetsRequest struct {
	Ident  string `json:"ident"`
	Ts     int64  `json:"ts"`
	Sign   string `json:"sign"`
	Tenant string `json:"tenant"`
}

// MtsHeartbeatRequest MtsHeartbeat response entity
type MtsHeartbeatRequest struct {
	Ident  string `json:"ident"`
	Ts     int64  `json:"ts"`
	Tenant string `json:"tenant"`
}

type Md5Sign struct {
	Timestamp int64  `json:"timestamp"`
	Md5       string `json:"md5"`
}

func (sign *Md5Sign) sign() string {
	if sign.Md5 != "" {
		return strconv.FormatInt(sign.Timestamp, 10) + "@" + sign.Md5
	}
	return ""
}

// MtsResponse TargetsResult 请求targets结果
type MtsResponse[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Result  T      `json:"result"`
}

// PullTargetResult Result 数据结构体
type PullTargetResult struct {
	Data []TargetGroup `json:"data"`
	Sign string        `json:"sign"`
}

// TargetGroup 目标targets
type TargetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

type MtsClient struct {
	httpClient   *http.Client
	globalStopCh <-chan struct{}
}

func NewMtsClient(globalStopCh <-chan struct{}) *MtsClient {
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &MtsClient{
		globalStopCh: globalStopCh,
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext:           dialer.DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: 5 * time.Second,
		},
	}
}

// StartHeartbeat period
func (c *MtsClient) StartHeartbeat() error {
	// init ip
	ip, err := c.getIp()
	if err != nil {
		return fmt.Errorf("init ip err: %w", err)
	}
	if ip == "" {
		return errors.New("ip is empty")

	}
	ident = fmt.Sprintf("%s:%s", ip, "8429")

	err = c.heartbeat()
	if err != nil {
		return err
	}
	logger.Infof("mts heartbeat started.")
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case <-c.globalStopCh:
				return
			case <-ticker.C:
				err = c.heartbeat()
				if err != nil {
					logger.Warnf("mts heartbeat failed: %s", err)
				}
			}
		}
	}()
	return nil
}

func (c *MtsClient) getIp() (string, error) {
	ip := ""
	err := requestMts(c, apiIp, struct{}{}, func(resp *MtsResponse[string]) error {
		if resp.Code == 0 {
			ip = resp.Result
		}
		return nil
	})
	return ip, err
}

func (c *MtsClient) heartbeat() error {
	entity := MtsHeartbeatRequest{Ident: ident, Ts: time.Now().UnixMilli(), Tenant: tenant}
	err := requestMts(c, apiHeartbeat, entity, func(result *MtsResponse[string]) error {
		if result.Code == 0 {
			mtsHeartbeatSuccessMetric.Inc()
			return nil
		} else {
			return errors.New(result.Message)
		}
	})
	if err != nil {
		mtsHeartbeatFailedMetric.Inc()
	}
	return err
}

// loadConfig 如果 mts 下发配置变化，则返回 config 不为空，且 err 为空
func (c *MtsClient) loadConfig(_ string) (*Config, error) {
	var cfg *Config
	req := MtsGetTargetsRequest{Ident: ident, Sign: sign.sign(), Tenant: tenant}
	err := requestMts(c, apiGetTarget, req, func(mtsResult *MtsResponse[PullTargetResult]) error {
		if mtsResult.Code == 0 {
			// targets 发生变化，需要解析
			result := mtsResult.Result
			// 获取sign判断，获取md5、timestamp
			signArray := strings.Split(result.Sign, "@")
			if len(signArray) != 2 {
				return fmt.Errorf("sign format err: len(signArr)!=2: result.sign: %s", result.Sign)
			}
			remoteMd5 := signArray[1]
			remoteTimestamp, err := strconv.ParseInt(signArray[0], 10, 64)
			if err != nil {
				return fmt.Errorf("parse sign.ts(%s) err: %v", result.Sign, err)
			}
			if remoteMd5 == sign.Md5 {
				if sign.Timestamp < remoteTimestamp {
					sign.Timestamp = remoteTimestamp
				}
				mtsPullTargetsNoChangeMetric.Inc()
				return nil
			}

			// parse scrape configs
			jobScrapConfigs := make(map[string]*ScrapeConfig, 100)
			count := 0
			for i := range result.Data {
				targetGroup := result.Data[i]
				count += len(targetGroup.Targets)
				if jobName, ok := targetGroup.Labels[jobLabel]; ok {
					labels := promutils.GetLabels()
					for k, v := range targetGroup.Labels {
						labels.Add(k, v)
					}
					//add tenant labels
					labels.Add("__tenant_id__", tenantMap[tenant])
					staticConfig := StaticConfig{
						Targets: targetGroup.Targets,
						Labels:  labels,
					}
					if jobScrapConfig, ok := jobScrapConfigs[jobName]; !ok {
						jobScrapConfig = &ScrapeConfig{
							JobName: jobName,
							StaticConfigs: []StaticConfig{
								staticConfig,
							},
						}
						sws, err := getScrapeWorkConfig(jobScrapConfig, "", globalConfig)
						if err != nil {
							logger.Warnf("getScrapeWorkConfig for %s: ", jobName, err)
							continue
						}
						jobScrapConfig.swc = sws
						jobScrapConfigs[jobName] = jobScrapConfig
					} else {
						jobScrapConfig.StaticConfigs = append(jobScrapConfig.StaticConfigs, staticConfig)
					}
				}
			}

			// build vm agent config
			scrapeConfigs := make([]*ScrapeConfig, 0, len(jobScrapConfigs))
			for _, jobScrapConfig := range jobScrapConfigs {
				scrapeConfigs = append(scrapeConfigs, jobScrapConfig)
			}

			cfg = &Config{
				Global:        *globalConfig,
				ScrapeConfigs: scrapeConfigs,
			}

			targetSize.Store(int64(count))
			mtsPullTargetsSuccessMetric.Inc()
			return nil
		} else if mtsResult.Code == -2 {
			mtsPullTargetsNoChangeMetric.Inc()
			return nil
		} else {
			// 目前 mts 当 sign 没变化时，返回 code 是 -1，所以需要根据 msg 来判断
			if strings.Contains(mtsResult.Message, "targets和服务端一致") {
				mtsPullTargetsNoChangeMetric.Inc()
				return nil
			}
			return errors.New(mtsResult.Message)
		}
	})
	if err != nil {
		mtsPullTargetsFailedMetric.Inc()
		logger.Warnf("mts pull targets err: %s", err)
	}
	return cfg, err
}

func requestMts[T any, Z any](c *MtsClient, path string, req T, handler func(result *MtsResponse[Z]) error) error {
	retryCount := 0
	var err error
retryRequest:
	// at most retry once
	if retryCount > 1 {
		return fmt.Errorf("exceed max retry count, err: %v", err)
	}
	retryCount++
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marsha json err: %w", err)
	}
	request, err := http.NewRequest("POST", mtsUrl+path, bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			goto retryRequest
		}
		if netutil.IsTrivialNetworkError(err) {
			goto retryRequest
		}
		return err
	}
	if resp.Body != nil && resp.Body != http.NoBody {
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(resp.Body)
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode/100 == 5 {
			err = fmt.Errorf("request mts statusCode=%v", resp.StatusCode)
			goto retryRequest
		}
		return fmt.Errorf("request mts statusCode=%v", resp.StatusCode)
	}
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		if netutil.IsTrivialNetworkError(err) {
			goto retryRequest
		}
		return err
	}
	result := &MtsResponse[Z]{}
	err = json.Unmarshal(respData, result)
	if err != nil {
		return err
	}
	return handler(result)
}
