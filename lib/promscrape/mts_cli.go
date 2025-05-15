package promscrape

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mts"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	apiGetTarget = "/api/getTargets"
	jobLabel     = "job"
)

var MtsConfigData atomic.Pointer[[]byte]

var globalConfig = &GlobalConfig{
	ScrapeInterval: promutil.NewDuration(15 * time.Second),
	ScrapeTimeout:  promutil.NewDuration(10 * time.Second),
	ExternalLabels: nil,
}

func WriteMtsConfigData(w io.Writer) {
	p := MtsConfigData.Load()
	logger.Infof("pullTargetsAddr: %v, otherHeartbeatAddrs: %v", GetPullTargetAddr(), mts.GetOtherHeartbeatAddrs())
	if p == nil {
		// Nothing to write to w
		return
	}
	_, _ = w.Write(*p)
}

func init() {
	mts.RegisterMtsClient(
		"vmagent",
		mts.InitCliVmAgent,
		startBGTask,
		nil,
	)
}

func GetPullTargetAddr() string {
	u := mts.PullTargetsAddr.Load()
	if u == nil {
		return mts.MtsUrl
	}
	return u.(string)
}

func IsScrapeInitErr(err error) bool {
	return err != nil && errors.Is(err, errGroupNotInitialized)
}

var errGroupNotInitialized error = errors.New("mts scrape group not initialized")

var sign = &mts.Md5Digest{Md5: "", Timestamp: 0}

func startBGTask() error {
	err := mts.StartHeartbeat(
		mts.Cli,
		func() *mts.MtsHeartbeatRequest {
			return &mts.MtsHeartbeatRequest{Ident: mts.Ident, Addr: mts.Addr, Ts: time.Now().UnixMilli(), Tenant: mts.ScrapeTenant, Group: mts.ScrapeGroup, Stopping: mts.Cli.IsStopping()}
		},
		func(result *mts.MtsClientConfig) error {
			newPullTargetsAddr := result.PullTargetsAddr
			if newPullTargetsAddr == nil {
				return nil
			}
			parsedAddr, err := mts.ParseUrl(*newPullTargetsAddr)
			if err != nil {
				mts.MtsWarningMetrics.Inc()
				logger.Warnf("mts parse <PullTargetsAddr> err: %v, newAddr: %s", err, *newPullTargetsAddr)
				return nil
			}
			previousAddr := GetPullTargetAddr()
			if parsedAddr == mts.MtsUrl {
				if parsedAddr != previousAddr {
					logger.Infof("mts pull targets addr changed from %s to %s (same to MtsUrl)", previousAddr, parsedAddr)
					mts.PullTargetsAddr.Store(parsedAddr)
				}
				return nil
			}
			if len(mts.GetOtherHeartbeatAddrs()) > 0 {
				for _, heartbeatAddr := range mts.GetOtherHeartbeatAddrs() {
					if heartbeatAddr == parsedAddr {
						// pull targets 前提必须先上报心跳
						if previousAddr != parsedAddr {
							logger.Infof("mts pull targets addr changed from %s to %s", previousAddr, parsedAddr)
							mts.PullTargetsAddr.Store(parsedAddr)
						}
						return nil
					}
				}
				mts.MtsWarningMetrics.Inc()
				logger.Warnf("mts pull targets addr (%s) is not in heartbeat addrs, %v", parsedAddr, mts.GetOtherHeartbeatAddrs())
			}
			return nil
		},
	)
	if err != nil {
		return err
	}

	return remotewrite.StartVmAgentConfig()
}

// loadConfig 如果 mts 下发配置变化，则返回 config 不为空，且 err 为空
func loadScrapeConfig(_ string) (*Config, error) {
	var cfg *Config
	req := mts.MtsGetTargetsRequest{Ident: mts.Ident, Sign: sign.Signature(), Tenant: mts.ScrapeTenant, Group: mts.ScrapeGroup}
	err := mts.PostRequest(mts.Cli, GetPullTargetAddr()+apiGetTarget, req, nil, func(respData *[]byte, mtsResult *mts.MtsResponse[*mts.PullTargetResult]) error {
		switch mtsResult.Code {
		case 0:
			// targets 发生变化，需要解析
			result := mtsResult.Result
			// 获取sign判断，获取md5、timestamp
			signArray := strings.Split(result.Sign, "@")
			if len(signArray) != 2 {
				mts.MtsWarningMetrics.Inc()
				return fmt.Errorf("mts sign format err: len(signArr)!=2: result.sign: %s", result.Sign)
			}
			remoteMd5 := signArray[1]
			remoteTimestamp, err := strconv.ParseInt(signArray[0], 10, 64)
			if err != nil {
				mts.MtsWarningMetrics.Inc()
				return fmt.Errorf("mts parse sign.ts(%s) err: %v", result.Sign, err)
			}
			// parse scrape configs
			jobScrapConfigs := make(map[string]*ScrapeConfig, 100)
			count := 0
			for i := range result.Data {
				targetGroup := result.Data[i]
				count += len(targetGroup.Targets)
				if jobName, ok := targetGroup.Labels[jobLabel]; ok {
					labels := promutil.GetLabels()
					for k, v := range targetGroup.Labels {
						labels.Add(k, v)
					}
					// add tenant labels
					// 如果 job 指定了 tenant，则优先使用
					if targetGroup.ScrapeConfig != nil && targetGroup.ScrapeConfig.Tenant != "" {
						t, tok := remotewrite.TenantToAuthToken.Load(targetGroup.ScrapeConfig.Tenant)
						if !tok {
							mts.MtsWarningMetrics.Inc()
							logger.Warnf("tenant(specified by scrape config) %s not found in remotewrite.TenantToAuthToken", mts.ScrapeTenant)
							continue
						}
						labels.Add("__tenant_id__", t.(*auth.Token).String())
					} else {
						t, tok := remotewrite.TenantToAuthToken.Load(mts.ScrapeTenant)
						if !tok {
							mts.MtsWarningMetrics.Inc()
							logger.Warnf("tenant %s not found in remotewrite.TenantToAuthToken", mts.ScrapeTenant)
							continue
						}
						labels.Add("__tenant_id__", t.(*auth.Token).String())
					}
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
							jobScrapConfig.ScrapeTimeout = promutil.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeTimeout))
							jobScrapConfig.ScrapeInterval = promutil.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeInterval))
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

			mts.PullTargetSize.Store(int64(count))
			mts.MtsPullTargetsSuccessMetric.Inc()
			MtsConfigData.Store(respData)
			mts.MtsPullTargetsUpdateTs.Store(time.Now().Unix())
			if mts.Cli.IsStopping() && len(result.Data) == 0 {
				// trigger the heartbeat goroutine to exit
				//todo
			}
			sign.Md5 = remoteMd5
			sign.Timestamp = remoteTimestamp
			return nil
		case 2:
			mts.MtsPullTargetsNoChangeMetric.Inc()
			return nil
		case 3:
			return errGroupNotInitialized
		default:
			mts.MtsWarningMetrics.Inc()
			return errors.New(mtsResult.Message)
		}
	})
	if err != nil {
		mts.MtsPullTargetsFailedMetric.Inc()
		return nil, err
	}
	return cfg, err
}
