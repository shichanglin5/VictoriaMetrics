package promscrape

import (
	"errors"
	"fmt"
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
	logger.Infof("pullTargetsAddr: %v, otherHeartbeatAddrs: %v", mts.GetPullTargetAddr(), mts.GetHeartbeatAddrs())
	if p == nil {
		// Nothing to write to w
		return
	}
	_, _ = w.Write(*p)
}

func IsScrapeInitErr(err error) bool {
	return err != nil && errors.Is(err, errGroupNotInitialized)
}

func IsCliStoppingErr(err error) bool {
	return err != nil && errors.Is(err, errMtsClientStopping)
}

var errGroupNotInitialized error = errors.New("mts scrape group not initialized")
var errMtsClientStopping error = errors.New("mts client is stopping")

var sign = &mts.Md5Digest{Md5: "", Timestamp: 0}

// loadConfig 如果 mts 下发配置变化，则返回 config 不为空，且 err 为空
func loadScrapeConfig(_ string) (*Config, error) {
	if mts.Cli.IsStopping() {
		return nil, errMtsClientStopping
	}
	var cfg *Config
	req := mts.MtsGetTargetsRequest{Ident: mts.Ident, Sign: sign.Signature(), Tenant: mts.ScrapeTenant, Group: mts.ScrapeGroup}
	err := mts.PostRequest(mts.Cli, mts.GetPullTargetAddr()+apiGetTarget, req, nil, func(respData *[]byte, mtsResult *mts.MtsResponse[*mts.PullTargetResult]) error {
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
					groupJobName := jobName + "/" + targetGroup.GroupId
					labels := promutil.GetLabels()
					for k, v := range targetGroup.Labels {
						labels.Add(k, v)
					}
					// add tenant labels
					// 如果 job 指定了 tenant，则优先使用
					if targetGroup.ScrapeConfig != nil && targetGroup.ScrapeConfig.Tenant != "" {
						t, tok := mts.TenantToAuthToken.Load(targetGroup.ScrapeConfig.Tenant)
						if !tok {
							mts.MtsWarningMetrics.Inc()
							logger.Warnf("tenant(specified by scrape config) %s not found in mts.TenantToAuthToken", mts.ScrapeTenant)
							continue
						}
						labels.Add("__tenant_id__", t.(*auth.Token).String())
					} else {
						t, tok := mts.TenantToAuthToken.Load(mts.ScrapeTenant)
						if !tok {
							mts.MtsWarningMetrics.Inc()
							logger.Warnf("tenant %s not found in mts.TenantToAuthToken", mts.ScrapeTenant)
							continue
						}
						labels.Add("__tenant_id__", t.(*auth.Token).String())
					}
					staticConfig := StaticConfig{
						Targets: targetGroup.Targets,
						Labels:  labels,
					}
					if jobScrapConfig, ok := jobScrapConfigs[groupJobName]; !ok {
						jobScrapConfig = &ScrapeConfig{
							JobName: groupJobName,
							StaticConfigs: []StaticConfig{
								staticConfig,
							},
						}
						if targetGroup.ScrapeConfig != nil {
							jobScrapConfig.ScrapeTimeout = promutil.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeTimeout))
							jobScrapConfig.ScrapeInterval = promutil.NewDuration(time.Duration(targetGroup.ScrapeConfig.ScrapeInterval))
							jobScrapConfig.MaxScrapeSize = targetGroup.ScrapeConfig.MaxScrapeSize

							if targetGroup.ScrapeConfig.AuthHeaders != nil {
								headers := make([]string, 0, len(targetGroup.ScrapeConfig.AuthHeaders))
								for k, v := range targetGroup.ScrapeConfig.AuthHeaders {
									headers = append(headers, fmt.Sprintf("%s:%s", k, v))
								}
								jobScrapConfig.HTTPClientConfig.Headers = headers
							}
						}
						sws, err := getScrapeWorkConfig(jobScrapConfig, "", globalConfig)
						if err != nil {
							logger.Warnf("getScrapeWorkConfig for %s: %v", groupJobName, err)
							continue
						}
						jobScrapConfig.swc = sws
						jobScrapConfigs[groupJobName] = jobScrapConfig
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
