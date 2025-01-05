package mts

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"os"
	"strings"
	"sync/atomic"
)

var (
	ScrapeGroup  string
	ScrapeTenant string
)

var (
	VmAgentQueueSize             *metrics.Gauge
	MtsPullTargetsNoChangeMetric *metrics.Counter
	MtsPullTargetsSuccessMetric  *metrics.Counter
	MtsPullTargetsUpdateSeconds  *metrics.Gauge
	MtsPullTargetsFailedMetric   *metrics.Counter

	FastQueueSize          atomic.Int64
	PullTargetSize         atomic.Int64
	PullTargetsAddr        atomic.Value
	MtsPullTargetsUpdateTs atomic.Int64
)

func InitCliVmAgent() error {
	VmAgentQueueSize = metrics.NewGauge(`vmagent_fast_queue_size`, func() float64 {
		return float64(FastQueueSize.Load())
	})
	MtsPullTargetsNoChangeMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_nochange`)
	MtsPullTargetsSuccessMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_success`)
	MtsPullTargetsUpdateSeconds = metrics.NewGauge(`vmagent_mts_pull_targets_update_seconds`, func() float64 {
		return float64(MtsPullTargetsUpdateTs.Load())
	})
	MtsPullTargetsFailedMetric = metrics.NewCounter(`vmagent_mts_pull_targets_counter_failed`)
	_ = metrics.NewGauge(`vmagent_mts_pull_targets_size`, func() float64 {
		return float64(PullTargetSize.Load())
	})

	// scrape group
	ScrapeGroup = os.Getenv("IDC")
	if len(ScrapeGroup) > 0 {
		ScrapeGroup = strings.TrimSpace(ScrapeGroup)
		logger.Infof("load mts scrape group from env(IDC): %s", ScrapeGroup)
	} else {
		logger.Fatalf("env IDC not set")
	}

	// scrape group
	ScrapeTenant = os.Getenv("SCRAPE_TENANT")
	if len(ScrapeTenant) > 0 {
		ScrapeTenant = strings.TrimSpace(ScrapeTenant)
		logger.Infof("load mts scrape tenant from env: %s", ScrapeGroup)
	} else {
		logger.Fatalf("env SCRAPE_TENANT not set")
	}
	return nil
}
