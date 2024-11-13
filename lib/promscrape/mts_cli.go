package promscrape

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
	"github.com/VictoriaMetrics/metrics"
	"github.com/prometheus/common/model"
	"go.uber.org/atomic"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	apiIp        = "/stats/ip"
	apiHeartbeat = "/stats/heartbeat"
	apiGetTarget = "/stats/getTargets"
	jobLabel     = "job"
)

var TenantToAuthTokenStr = map[string]string{
	"meta":                "0:0",
	"default":             "1:0",
	"inf-esproxy":         "1:1",
	"inf-dbproxy":         "1:2",
	"inf-redisproxy":      "1:3",
	"inf-pay":             "1:4",
	"skyeye-band":         "2:0",
	"skyeye-band-large":   "2:1",
	"skyeye-band-uat":     "2:2",
	"skyeye-gateway":      "2:3",
	"skyeye-skynet":       "2:4",
	"skyeye-tlb":          "2:5",
	"skyeye-tlb-core":     "2:6",
	"skyeye-tlb-core-uat": "2:7",
	"skyeye-tlb-static":   "2:8",
	"skyeye-tlb-uat":      "2:9",
	"skynet-alert":        "2:10",
	"skyeye-apm":          "3:0",
	"skyeye-apm-uat":      "3:1",
	"skyeye-apm-topo":     "3:2",
}
var TenantToAuthToken = make(map[string]*auth.Token, len(TenantToAuthTokenStr))
var AuthTokenToTenant = make(map[auth.Token]string, len(TenantToAuthTokenStr))
var MtsPullTargetsUpdateTs atomic.Int64

func newAuthToken(t string) *auth.Token {
	token, err := auth.NewToken(t)
	if err != nil {
		panic(err)
	}
	return token
}

var (
	// metrics
	mtsHeartbeatFailedMetric  = metrics.NewCounter(`vmagent_mts_heartbeat_failed`)
	mtsHeartbeatSuccessMetric = metrics.NewCounter(`vmagent_mts_heartbeat_success`)

	mtsPullTargetsNoChangeMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_nochange`)
	mtsPullTargetsSuccessMetric  = metrics.NewCounter(`vmagent_mts_pull_targets_counter_success`)
	mtsPullTargetsUpdateTs       = metrics.NewGauge(`vmagent_mts_pull_targets_update_seconds`, func() float64 {
		return float64(MtsPullTargetsUpdateTs.Load())
	})
	mtsPullTargetsFailedMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_failed`)
	targetSize                 atomic.Int64
	_                          = metrics.NewGauge(`vmagent_mts_pull_targets_size`, func() float64 {
		return float64(targetSize.Load())
	})

	// init from ip addr
	ident string
	addr  string

	// env
	region      string
	tenants     []string
	scrapeGroup string
	MtsUrl      string
	identTag    string
	urlTag      string

	// runtime vars
	sign = &Md5Digest{Md5: "", Timestamp: 0}
)

var (
	OverrideRemoteWriteUrls    []string
	OverrideRemoteWriteHeaders []string
	GetAuthTokenByArgId        func(i int) string
	GetRwctxIdByArgId          func(i int) string
)

var globalConfig = &GlobalConfig{
	ScrapeInterval: promutils.NewDuration(15 * time.Second),
	ScrapeTimeout:  promutils.NewDuration(10 * time.Second),
	ExternalLabels: nil,
}

var wxUrl = "http://hd1.infprometheus.dss.17usoft.com/write/api/v1/write"
var thUrl = "http://hd2.infprometheus.dss.17usoft.com/write/api/v1/write"
var regionUrls = map[string]string{
	"wx": wxUrl,
	"th": thUrl,
}

var mtsConfigData atomic.Pointer[[]byte]

func WriteMtsConfigData(w io.Writer) {
	p := mtsConfigData.Load()
	if p == nil {
		// Nothing to write to w
		return
	}
	_, _ = w.Write(*p)
}

func init() {
	var err error
	ident, err = os.Hostname()
	if err != nil {
		logger.Fatalf("get hostname err: %v", err)
	}
	// tenants
	regionEnv := os.Getenv("REGION")
	if len(regionEnv) > 0 {
		region = strings.TrimSpace(regionEnv)
		if _, ok := regionUrls[region]; !ok {
			panic("region must be 'wx' or 'th'")
		}
		logger.Infof("load region from env: %s", region)
	} else {
		logger.Infof("no evn REGION set, default to multi region mode")
	}

	// tenants
	tenantsEnv := os.Getenv("TENANTS")
	if len(tenantsEnv) == 0 {
		panic("environment variable TENANTS not set")
	}
	// 如果指定为 *，则添加所有预定于的租户
	if tenantsEnv == "*" {
		tenants = make([]string, 0, len(TenantToAuthTokenStr))
		for k := range TenantToAuthTokenStr {
			tenants = append(tenants, k)
		}
	} else {
		tenantsSplit := strings.Split(tenantsEnv, ",")
		tenants = make([]string, 0, len(tenantsSplit))
		for _, tenant := range tenantsSplit {
			tenants = append(tenants, strings.TrimSpace(tenant))
		}
	}
	logger.Infof("load tenants(len=%d) from env: %v", len(tenants), tenants)

	// scrape group
	scrapeGroup = os.Getenv("SCRAPE_GROUP")
	if len(scrapeGroup) > 0 {
		scrapeGroup = strings.TrimSpace(scrapeGroup)
		logger.Infof("load mts scrape group from env: %s", MtsUrl)
	} else {
		logger.Infof("env SCRAPE_GROUP not set")
	}

	// mts url
	MtsUrl = os.Getenv("MTS_URL")
	if len(MtsUrl) > 0 {
		MtsUrl = strings.TrimSuffix(MtsUrl, "/")
		logger.Infof("load mts url from env: %s", MtsUrl)
	} else {
		logger.Infof("env MTS_URL not set")
	}

	// extra tags
	identTag = strings.TrimSpace(os.Getenv("IDENT_TAG"))
	urlTag = strings.TrimSpace(os.Getenv("URL_TAG"))

	// generate remote write urls & headers
	OverrideRemoteWriteHeaders = make([]string, 0, len(tenants)*2)
	OverrideRemoteWriteUrls = make([]string, 0, len(tenants)*2)
	for _, tenant := range tenants {
		if len(region) > 0 {
			GetAuthTokenByArgId = func(i int) string {
				return TenantToAuthToken[tenants[i]].String()
			}
			GetRwctxIdByArgId = func(i int) string {
				return fmt.Sprintf("%s_%s", region, tenants[i])
			}
			OverrideRemoteWriteUrls = append(OverrideRemoteWriteUrls, fmt.Sprintf("%s", regionUrls[region]))
			OverrideRemoteWriteHeaders = append(OverrideRemoteWriteHeaders, fmt.Sprintf("X-Scope-OrgID:%s", tenant))
		} else {
			GetAuthTokenByArgId = func(i int) string {
				return TenantToAuthToken[tenants[i/2]].String()
			}
			GetRwctxIdByArgId = func(i int) string {
				isWxRegion := i%2 == 0
				if isWxRegion {
					return fmt.Sprintf("%s_%s", "wx", tenants[i/2])
				} else {
					return fmt.Sprintf("%s_%s", "th", tenants[i/2])
				}
			}
			// wx
			OverrideRemoteWriteUrls = append(OverrideRemoteWriteUrls, fmt.Sprintf("%s", wxUrl))
			OverrideRemoteWriteHeaders = append(OverrideRemoteWriteHeaders, fmt.Sprintf("X-Scope-OrgID:%s", tenant))
			// th
			OverrideRemoteWriteUrls = append(OverrideRemoteWriteUrls, fmt.Sprintf("%s", thUrl))
			OverrideRemoteWriteHeaders = append(OverrideRemoteWriteHeaders, fmt.Sprintf("X-Scope-OrgID:%s", tenant))
		}
	}

	// init auth tokens
	for k, v := range TenantToAuthTokenStr {
		authToken := newAuthToken(v)
		TenantToAuthToken[k] = authToken
		AuthTokenToTenant[*authToken] = k
	}
}

// MtsGetTargetsRequest MtsTargets mts heartbeat response
type MtsGetTargetsRequest struct {
	Ident  string `json:"ident"`
	Ts     int64  `json:"ts"`
	Sign   string `json:"sign"`
	Tenant string `json:"tenant"`
	Group  string `json:"group"`
}

// MtsHeartbeatRequest MtsHeartbeat response entity
type MtsHeartbeatRequest struct {
	Ident    string `json:"ident"`
	Addr     string `json:"addr"`
	Ts       int64  `json:"ts"`
	Tenant   string `json:"tenant"`
	Group    string `json:"group"`
	Stopping bool   `json:"stopping"`
}

type Md5Digest struct {
	Timestamp int64  `json:"timestamp"`
	Md5       string `json:"md5"`
}

func (sign *Md5Digest) signature() string {
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

// MtsScrapeConfig 采集配置
type MtsScrapeConfig struct {
	ScrapeIdc      string         `json:"scrapeIdc,omitempty"`
	ScrapeTimeout  model.Duration `json:"scrapeTimeout,omitempty"`
	ScrapeInterval model.Duration `json:"scrapeInterval,omitempty"`
	MaxScrapeSize  string         `json:"maxScrapeSize,omitempty"`
}

// TargetGroup 目标targets
type TargetGroup struct {
	Targets      []string          `json:"targets"`
	Labels       map[string]string `json:"labels"`
	ScrapeConfig *MtsScrapeConfig  `json:"scrapeConfig,omitempty"`
}

type MtsClient struct {
	httpClient *http.Client
	stopCh     <-chan struct{}
	cancelFunc context.CancelFunc

	// heartbeat to mts with 'stopping' flag (set ts to -1)
	stopping atomic.Bool
}

func NewMtsClient() *MtsClient {
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &MtsClient{
		stopCh:     ctx.Done(),
		cancelFunc: cancelFunc,
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
	addr = fmt.Sprintf("%s:%s", ip, "8429")

	err = c.heartbeat()
	if err != nil {
		return err
	}
	logger.Infof("mts heartbeat started.")
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		scraperWG.Add(1)
		defer scraperWG.Done()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

		for {
			select {
			case sig := <-signals:
				if sig == syscall.SIGHUP {
					// Prevent from the program stop on SIGHUP
					continue
				}
				logger.Infof("mts client stopping...")
				c.stopping.Store(true)
				_ = c.heartbeat()
				signal.Stop(signals)
				//
				go func() {
					logger.Infof("mts client will exit after waiting for 10 seconds")
					time.Sleep(10 * time.Second)
					c.cancelFunc()
				}()
			case <-c.stopCh:
				logger.Infof("mts client exit gracefally")
				return
			case <-globalStopChan:
				logger.Infof("mts client exit. triggerred by globalStopCh, adjust -http.shutdownDelay to bigger value to wait for mts gracefally shutdown.")
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
	err := requestMts(c, apiIp, struct{}{}, func(_ *[]byte, resp *MtsResponse[string]) error {
		if resp.Code == 0 {
			ip = resp.Result
		}
		return nil
	})
	return ip, err
}

func (c *MtsClient) heartbeat() error {
	entity := MtsHeartbeatRequest{Ident: ident, Addr: addr, Ts: time.Now().UnixMilli(), Tenant: tenants[0], Group: scrapeGroup, Stopping: c.stopping.Load()}
	err := requestMts(c, apiHeartbeat, entity, func(_ *[]byte, result *MtsResponse[string]) error {
		if result.Code == 0 {
			mtsHeartbeatSuccessMetric.Inc()
			return nil
		} else {
			return errors.New(result.Message)
		}
	})
	if err != nil {
		mtsHeartbeatFailedMetric.Inc()
		return fmt.Errorf("mts heartbeat: %s", err)
	}
	return err
}

func IsScrapeInitErr(err error) bool {
	return err != nil && errors.Is(err, errGroupNotInitialized)
}

var errGroupNotInitialized error = errors.New("mts scrape group not initialized")

// loadConfig 如果 mts 下发配置变化，则返回 config 不为空，且 err 为空
func (c *MtsClient) loadConfig(_ string) (*Config, error) {
	var cfg *Config
	req := MtsGetTargetsRequest{Ident: ident, Sign: sign.signature(), Tenant: tenants[0], Group: scrapeGroup}
	err := requestMts(c, apiGetTarget, req, func(respData *[]byte, mtsResult *MtsResponse[PullTargetResult]) error {
		switch mtsResult.Code {
		case 0:
			// targets 发生变化，需要解析
			result := mtsResult.Result
			// 获取sign判断，获取md5、timestamp
			signArray := strings.Split(result.Sign, "@")
			if len(signArray) != 2 {
				return fmt.Errorf("mts sign format err: len(signArr)!=2: result.sign: %s", result.Sign)
			}
			remoteMd5 := signArray[1]
			remoteTimestamp, err := strconv.ParseInt(signArray[0], 10, 64)
			if err != nil {
				return fmt.Errorf("mts parse sign.ts(%s) err: %v", result.Sign, err)
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
					labels.Add("__tenant_id__", TenantToAuthTokenStr[tenants[0]])
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
						if targetGroup.ScrapeConfig != nil {
							jobScrapConfig.ScrapeTimeout = promutils.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeTimeout))
							jobScrapConfig.ScrapeInterval = promutils.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeInterval))
							jobScrapConfig.MaxScrapeSize = targetGroup.ScrapeConfig.MaxScrapeSize
						}
						sws, err := getScrapeWorkConfig(jobScrapConfig, "", globalConfig)
						if err != nil {
							logger.Warnf("getScrapeWorkConfig for %s: %v", jobName, err)
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
			configs := make([]*ScrapeConfig, 0, len(jobScrapConfigs))
			for _, jobScrapConfig := range jobScrapConfigs {
				configs = append(configs, jobScrapConfig)
			}

			cfg = &Config{
				Global:        *globalConfig,
				ScrapeConfigs: configs,
			}

			targetSize.Store(int64(count))
			mtsPullTargetsSuccessMetric.Inc()
			mtsConfigData.Store(respData)
			MtsPullTargetsUpdateTs.Store(time.Now().Unix())
			if c.stopping.Load() && len(result.Data) == 0 {
				// trigger the heartbeat goroutine to exit
				c.cancelFunc()
			}
			sign.Md5 = remoteMd5
			sign.Timestamp = remoteTimestamp
			return nil
		case 2:
			mtsPullTargetsNoChangeMetric.Inc()
			return nil
		case 3:
			return errGroupNotInitialized
		default:
			return errors.New(mtsResult.Message)
		}
	})
	if err != nil {
		mtsPullTargetsFailedMetric.Inc()
		return nil, err
	}
	return cfg, err
}

func requestMts[T any, Z any](c *MtsClient, path string, req T, handler func(respData *[]byte, result *MtsResponse[Z]) error) error {
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
	request, err := http.NewRequest("POST", MtsUrl+path, bytes.NewBuffer(reqBody))
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
			err = fmt.Errorf("resp statusCode=%v", resp.StatusCode)
			goto retryRequest
		}
		return fmt.Errorf("resp statusCode=%v", resp.StatusCode)
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
		return fmt.Errorf("unmarshal json err: %w", err)
	}
	return handler(&respData, result)
}
