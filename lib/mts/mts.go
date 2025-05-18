package mts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
	"go.uber.org/atomic"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	apiHeartbeat = "/api/heartbeat"
)

var (
	shutdownDelay = flag.Duration("mts.shutdownDelay", 0, `Optional delay before http server shutdown. During this delay, the server returns non-OK responses from /health page, so load balancers can route new requests to other servers`)
)

var (
	Ident  string
	Addr   string
	MtsUrl string

	Cli                   *MtsClient
	runtimeHeartbeatAddrs atomic.Value
)

var (
	MtsHeartbeatFailedMetric  *metrics.Counter
	MtsWarningMetrics         *metrics.Counter
	MtsHeartbeatSuccessMetric *metrics.Counter

	MtsPullTargetsNoChangeMetric *metrics.Counter
	MtsPullTargetsSuccessMetric  *metrics.Counter
	MtsPullTargetsFailedMetric   *metrics.Counter

	PullTargetSize         atomic.Int64
	PullTargetsAddr        atomic.Value
	MtsPullTargetsUpdateTs atomic.Int64
)
var (
	MainHeartbeatAddrFailedCount atomic.Uint64
	ScrapeGroup                  string
	ScrapeTenant                 string
	ScrapeJobName                = "scrape"
	GatewayJobName               = "pushgateway"
)

var (
	// mts.RemoteWriteCtxsMapping rwxKey -> *remoteWriteCtx
	RemoteWriteCtxsMapping = &sync.Map{}

	// mts.AuthTokenToRwctxs authToken(string) -> []*remoteWriteCtx
	AuthTokenToRwctxs = &sync.Map{}

	// AuthTokenToTenant authToken(string) -> tenant(string)
	AuthTokenToTenant = &sync.Map{}

	// TenantToAuthToken tenant(string) -> authToken(*auth.Token)
	TenantToAuthToken = &sync.Map{}
)

func GetPullTargetAddr() string {
	u := PullTargetsAddr.Load()
	if u == nil {
		return MtsUrl
	}
	return u.(string)
}

func IsMtsScrapeConfigDisabled() bool {
	return len(MtsUrl) == 0
}

func NewMtsClient() *MtsClient {
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &MtsClient{
		preStopCh:  make(chan struct{}, 1),
		stopCh:     ctx.Done(),
		cancelFunc: cancelFunc,
		httpClient: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: time.Second,
				DialContext:           dialer.DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: 3 * time.Second,
		},
	}
}

func (c *MtsClient) PreStopCh() <-chan struct{} {
	return c.preStopCh
}

func (c *MtsClient) StopCh() <-chan struct{} {
	return c.stopCh
}

func Init() {
	initVars()

	// mts url
	mtsUrl := os.Getenv("MTS_URL")
	if len(mtsUrl) == 0 {
		return
	}

	MtsUrl = strings.TrimSuffix(mtsUrl, "/")
	logger.Infof("load mts url from env: %s", MtsUrl)

	logger.Infof("mts client ident: %s", Ident)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	Cli = NewMtsClient()
	// 如果是 jean 容器，直接从环境变量获取 ip
	ip := os.Getenv("INSIP")
	if len(ip) == 0 {
		ip = "127.0.0.1"
	}
	Addr = fmt.Sprintf("%s:%s", ip, "8429")
	Cli.Addr = Addr

	MtsWarningMetrics = metrics.NewCounter(`vmagent_mts_warning_metrics`)
	MtsHeartbeatFailedMetric = metrics.NewCounter(`vmagent_mts_heartbeat_failed`)
	MtsHeartbeatSuccessMetric = metrics.NewCounter(`vmagent_mts_heartbeat_success`)

	MtsPullTargetsNoChangeMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_nochange`)
	MtsPullTargetsSuccessMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_success`)
	MtsPullTargetsFailedMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_failed`)
	_ = metrics.NewGauge(`vmagent_mts_pull_targets_size`, func() float64 {
		return float64(PullTargetSize.Load())
	})
	_ = metrics.NewGauge(`vmagent_mts_pull_targets_update_seconds`, func() float64 {
		return float64(MtsPullTargetsUpdateTs.Load())
	})

	err := StartHeartbeat()
	if err != nil {
		logger.Fatalf("start mts client type err: %v", err)
	}

	go func() {
		for {
			select {
			case <-signals:
				signal.Stop(signals)
				close(Cli.preStopCh)
				logger.Infof("mts receive signal, start to exit")
				Cli.stopping.Store(true)
				if *shutdownDelay > 0 {
					logger.Infof("mts waiting for shutdown delay: %s seconds", shutdownDelay.String())
					time.Sleep(*shutdownDelay)
				}
				Cli.cancelFunc()
				logger.Infof("mts client exit gracefally")
				return
			}
		}
	}()
}

func initVars() {
	// scrape group
	ScrapeGroup = os.Getenv("IDC")
	if len(ScrapeGroup) > 0 {
		ScrapeGroup = strings.TrimSpace(ScrapeGroup)
		logger.Infof("load mts scrape group from env(IDC): %s", ScrapeGroup)
	}

	// scrape group
	ScrapeTenant = os.Getenv("SCRAPE_TENANT")
	if len(ScrapeTenant) > 0 {
		ScrapeTenant = strings.TrimSpace(ScrapeTenant)
		logger.Infof("load mts scrape tenant from env: %s", ScrapeTenant)
	}

	var err error
	Ident, err = os.Hostname()
	if err != nil {
		logger.Fatalf("get hostname err: %v", err)
	}

	ScrapeJobName = fmt.Sprintf("scrape-%s-%s", ScrapeGroup, ScrapeTenant)
	GatewayJobName = fmt.Sprintf("gateway-%s", ScrapeGroup)
}

func (c *MtsClient) IsStopping() bool {
	return c.stopping.Load()
}

func GetHeartbeatAddrs() []string {
	u := runtimeHeartbeatAddrs.Load()
	if u == nil {
		return nil
	}
	return u.([]string)
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

func (sign *Md5Digest) Signature() string {
	if sign.Md5 != "" {
		return strconv.FormatInt(sign.Timestamp, 10) + "@" + sign.Md5
	}
	return ""
}

func StartHeartbeat() error {
	return startHeartbeat(
		Cli,
		func() *MtsHeartbeatRequest {
			return &MtsHeartbeatRequest{Ident: Ident, Addr: Addr, Ts: time.Now().UnixMilli(), Tenant: ScrapeTenant, Group: ScrapeGroup, Stopping: Cli.IsStopping()}
		},
		func(result *MtsClientConfig) error {
			newPullTargetsAddr := result.PullTargetsAddr
			if newPullTargetsAddr == nil {
				return nil
			}
			parsedAddr, err := ParseUrl(*newPullTargetsAddr)
			if err != nil {
				MtsWarningMetrics.Inc()
				logger.Warnf("mts parse <PullTargetsAddr> err: %v, newAddr: %s", err, *newPullTargetsAddr)
				return nil
			}
			previousAddr := GetPullTargetAddr()
			for _, heartbeatAddr := range GetHeartbeatAddrs() {
				if heartbeatAddr == parsedAddr {
					// pull targets 前提必须先上报心跳
					if previousAddr != parsedAddr {
						logger.Infof("mts pull targets addr changed from %s to %s", previousAddr, parsedAddr)
						PullTargetsAddr.Store(parsedAddr)
					}
					return nil
				}
			}
			MtsWarningMetrics.Inc()
			logger.Warnf("mts pull targets addr (%s) is not in heartbeat addrs, %v", parsedAddr, GetHeartbeatAddrs())

			return nil
		},
	)
}

// startHeartbeat periodically send heartbeat to mts server
func startHeartbeat[REQ any](c *MtsClient, reqFactory func() REQ, respHandler func(config *MtsClientConfig) error) error {
	err := sendHeartbeat(c, reqFactory, respHandler)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {
			case <-c.preStopCh:
				err = sendHeartbeat(c, reqFactory, respHandler)
				if err != nil {
					MtsWarningMetrics.Inc()
					logger.Warnf("mts final heartbeat failed: %s", err)
				}
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				_ = sendHeartbeat(c, reqFactory, respHandler)
			}
		}
	}()
	return nil
}

func sendHeartbeat[REQ any](c *MtsClient, reqFactory func() REQ, respHandler func(config *MtsClientConfig) error) error {
	req := reqFactory()
	rumtimeHeartbeatAddrs := GetHeartbeatAddrs()

	mainHeartbeatAddr := MtsUrl
	continuesFailedCount := MainHeartbeatAddrFailedCount.Load()
	var otherHeartbeatAddr []string
	if len(rumtimeHeartbeatAddrs) > 0 && continuesFailedCount < 30 {
		mainHeartbeatAddr = rumtimeHeartbeatAddrs[0]
		otherHeartbeatAddr = rumtimeHeartbeatAddrs[1:]
	}
	if continuesFailedCount >= 30 {
		logger.Warnf("mainHeartbeatAddr failed: %d, reset Heartbeat addr to MtsUrl: %s", continuesFailedCount, MtsUrl)
	}
	err := PostRequest(c, mainHeartbeatAddr+apiHeartbeat, req, nil, func(_ *[]byte, mtsResp *MtsResponse[*MtsClientConfig]) error {
		if mtsResp.Code == 0 {
			MainHeartbeatAddrFailedCount.Store(0)
			MtsHeartbeatSuccessMetric.Inc()
			if mtsResp.Result != nil {
				heartbeatAddrs := mtsResp.Result.HeartbeatAddrs
				parsedHeartbeatAddrs := make([]string, 0, len(heartbeatAddrs))
				if len(heartbeatAddrs) > 0 {
					for _, newAddr := range heartbeatAddrs {
						parsedAddr, err := ParseUrl(newAddr)
						if err != nil {
							MtsWarningMetrics.Inc()
							logger.Warnf("mts parse <HeartbeatAddrs> err: %v, newAddr: %s", err, newAddr)
							continue
						}
						parsedHeartbeatAddrs = append(parsedHeartbeatAddrs, parsedAddr)
					}
					if len(parsedHeartbeatAddrs) > 0 {
						runtimeHeartbeatAddrs.Store(parsedHeartbeatAddrs)
					} else {
						// 默认使用 mts url
						logger.Warnf("mts parse <HeartbeatAddrs> result array is emtpy, fallback to default: %v", MtsUrl)
						runtimeHeartbeatAddrs.Store([]string{MtsUrl})
					}
				}
				return respHandler(mtsResp.Result)
			}
			return nil
		} else {
			MtsWarningMetrics.Inc()
			logger.Warnf("mts heartbeat result code err: %s", mtsResp.Message)
			return errors.New(mtsResp.Message)
		}
	})

	if err != nil {
		MainHeartbeatAddrFailedCount.Inc()
		logger.Warnf("mts heartbeat request failed(%d): %s", continuesFailedCount, err)
		MtsHeartbeatFailedMetric.Inc()
	}

	if len(otherHeartbeatAddr) == 0 {
		return err
	}
	if len(otherHeartbeatAddr) == 1 {
		err := PostRequest(c, otherHeartbeatAddr[0]+apiHeartbeat, req, nil, func(_ *[]byte, mtsResp *MtsResponse[*MtsClientConfig]) error {
			if mtsResp.Code == 0 {
				MtsHeartbeatSuccessMetric.Inc()
				return nil
			} else {
				return errors.New(mtsResp.Message)
			}
		})
		if err != nil {
			MtsWarningMetrics.Inc()
			logger.Warnf("mts heartbeat to otherHeartbeatAddr[0](%s) failed: %s", otherHeartbeatAddr[0], err)
		}
	} else {
		var wg sync.WaitGroup
		wg.Add(len(otherHeartbeatAddr))
		for _, otherAddr := range otherHeartbeatAddr {
			go func() {
				defer wg.Done()
				err := PostRequest(c, otherAddr+apiHeartbeat, req, nil, func(_ *[]byte, mtsResp *MtsResponse[*MtsClientConfig]) error {
					if mtsResp.Code == 0 {
						MtsHeartbeatSuccessMetric.Inc()
						return nil
					} else {
						return errors.New(mtsResp.Message)
					}
				})
				if err != nil {
					MtsWarningMetrics.Inc()
					logger.Warnf("mts heartbeat to otherAddr(%s) failed: %s", otherAddr, err)
				}
			}()
		}
		wg.Wait()
	}
	return err
}

func ParseUrl(newAddr string) (string, error) {
	if !strings.HasPrefix(newAddr, "http") {
		newAddr = "http://" + newAddr
	}
	newAddr = strings.TrimSuffix(newAddr, "/")
	parse, err := url.Parse(newAddr)
	if err != nil {
		return "", fmt.Errorf("mts parse url err: %w, newAddr: %s", err, newAddr)
	}
	return parse.String(), nil
}

func GetRequest[Z any](c *MtsClient, url string, reqHandler func(r *http.Request), handler func(respData *[]byte, result *MtsResponse[Z]) error) error {
	return DoRequest(c, "GET", url, nil, reqHandler, handler)
}

// PostRequest post request to mts server. req 不能为空
func PostRequest[T any, Z any](c *MtsClient, url string, req T, reqHandler func(r *http.Request), handler func(respData *[]byte, result *MtsResponse[Z]) error) error {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marsha json err: %w", err)
	}
	return DoRequest(c, "POST", url, reqBody, reqHandler, handler)
}

func DoRequest[Z any](c *MtsClient, method, url string, reqBuf []byte, reqHandler func(r *http.Request), handler func(respData *[]byte, result *MtsResponse[Z]) error) error {
	retryCount := 0
	var err error
retryRequest:
	// at most retry once
	if retryCount > 1 {
		return fmt.Errorf("exceed max retry count, err: %v", err)
	}
	retryCount++
	var request *http.Request
	if len(reqBuf) > 0 {
		request, err = http.NewRequest(method, url, bytes.NewBuffer(reqBuf))
	} else {
		request, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return err
	}

	if reqHandler != nil {
		reqHandler(request)
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

// LoadConfig 封装的通用的配置获取方法
// 只需要指定 configKeys 和 configHandler 即可
func LoadConfig[T any](api string, reqHandler func(r *http.Request), configHandler func(configData T)) error {
	reqUrl := path.Join(MtsUrl, api)
	return GetRequest(Cli, reqUrl, reqHandler, func(respData *[]byte, mtsResult *MtsResponse[T]) error {
		switch mtsResult.Code {
		case 0:
			// targets 发生变化，需要解析
			result := mtsResult.Result
			configHandler(result)
			return nil
		default:
			MtsWarningMetrics.Inc()
			return errors.New(mtsResult.Message)
		}
	})
}
