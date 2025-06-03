package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/backoff"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/barpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/limiter"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/native"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/stepper"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/vm"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmctl/vmctlutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type vmNativeProcessor struct {
	filter native.Filter

	dst     *native.Client
	src     *native.Client
	backoff *backoff.Backoff

	s            *stats
	rateLimit    int64
	interCluster bool
	cc           int
	isNative     bool
	alignToStep  bool

	continueOnRestart   bool
	shardMigrationLabel string
}

const (
	nativeExportAddr       = "api/v1/export"
	nativeImportAddr       = "api/v1/import"
	nativeWithBackoffTpl   = `{{ blue "%s:" }} {{ counters . }} {{ bar . "[" "█" (cycle . "█") "▒" "]" }} {{ percent . }}`
	nativeSingleProcessTpl = `Total: {{counters . }} {{ cycle . "↖" "↗" "↘" "↙" }} Speed: {{speed . }} {{string . "suffix"}}`
)

func (p *vmNativeProcessor) run(ctx context.Context) error {
	if p.cc == 0 {
		p.cc = 1
	}
	p.s = &stats{
		startTime: time.Now(),
	}

	start, err := vmctlutil.ParseTime(p.filter.TimeStart)
	if err != nil {
		return fmt.Errorf("failed to parse %s, provided: %s, error: %w", vmNativeFilterTimeStart, p.filter.TimeStart, err)
	}

	end := time.Now().In(start.Location())
	if p.filter.TimeEnd != "" {
		end, err = vmctlutil.ParseTime(p.filter.TimeEnd)
		if err != nil {
			return fmt.Errorf("failed to parse %s, provided: %s, error: %w", vmNativeFilterTimeEnd, p.filter.TimeEnd, err)
		}
	}

	ranges := [][]time.Time{{start, end}}
	if p.filter.Chunk != "" {
		ranges, err = stepper.SplitDateRange(start, end, p.filter.Chunk, p.filter.TimeReverse, p.alignToStep)
		if err != nil {
			return fmt.Errorf("failed to create date ranges for the given time filters: %w", err)
		}
	}

	processedKeysCache := p.loadLastProcessedKeysCache()
	defer func() {
		p.saveProcessedKeysCache(processedKeysCache)
	}()

	tenants := []string{""}
	if p.interCluster {
		log.Printf("Discovering tenants...")
		tenants, err = p.src.GetSourceTenants(ctx, p.filter)
		if err != nil {
			return fmt.Errorf("failed to get tenants: %w", err)
		}
		question := fmt.Sprintf("The following tenants were discovered: %s.\n Continue?", tenants)
		if !prompt(question) {
			return nil
		}
	}

	for _, tenantID := range tenants {
		err := p.runBackfilling(ctx, tenantID, ranges, processedKeysCache)
		if err != nil {
			return fmt.Errorf("migration failed: %s", err)
		}
	}

	log.Println("Import finished!")
	log.Print(p.s)

	return nil
}

func (p *vmNativeProcessor) saveProcessedKeysCache(processedKeysCache *sync.Map) {
	if processedKeysCache == nil {
		logger.Infof("skipping save processedKeys because processedKeysCache is nil")
		return
	}

	processedKeys := make([]string, 0, 1024)
	processedKeysCache.Range(func(k, v interface{}) bool {
		processedKeys = append(processedKeys, k.(string))
		return true
	})
	if len(processedKeys) == 0 {
		logger.Infof("skipping save processedKeys because no processed keys cached")
		return
	}

	marshalBytes, err := json.Marshal(processedKeys)
	if err != nil {
		logger.Errorf("failed to marshal processedKeys: %s", err)
		return
	}

	migrationFileName := p.getMigrationContiueOnRestartFilepath()
	err = os.MkdirAll(path.Dir(migrationFileName), 0755)
	if err != nil {
		logger.Errorf("failed to create directory %s: %s", path.Dir(migrationFileName), err)
		return
	}
	file, err := os.Create(migrationFileName)
	if err != nil {
		logger.Errorf("failed to create directory %s: %s", path.Dir(migrationFileName), err)
		return
	}
	_, err = file.Write(marshalBytes)
	if err != nil {
		logger.Errorf("failed to save processedKeys: %s", err)
		return
	}

	logger.Infof("saved processedKeys: %s, keys count: %d", migrationFileName, len(processedKeys))
}

func (p *vmNativeProcessor) loadLastProcessedKeysCache() *sync.Map {
	var processedKeysCache sync.Map
	if p.continueOnRestart {
		migrationFileName := p.getMigrationContiueOnRestartFilepath()
		fileContent, err := os.ReadFile(migrationFileName)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Infof("skipping loading last migration processed keys from file[%s] because it doesn't exist", migrationFileName)
				return &processedKeysCache
			} else {
				logger.Fatalf("cannot read last migration processed keys from file[%s]: %v", migrationFileName, err)
			}
		}
		processKeys := make([]string, 0, 1024)
		err = json.Unmarshal(fileContent, &processKeys)
		if err != nil {
			logger.Fatalf("cannot parse last migration processed keys from file[%s]: %w", migrationFileName, err)
		}
		logger.Infof("loaded last migration processed keys from file[%s], processed keys count: %d", migrationFileName, len(processKeys))
		for _, processedKey := range processKeys {
			processedKeysCache.Store(processedKey, struct{}{})
		}
	}
	return &processedKeysCache
}

func (p *vmNativeProcessor) getMigrationContiueOnRestartFilepath() string {
	migrationTaskMd5 := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("<%s><%s><%s><%s><%s><%v>", p.filter.TimeStart, p.filter.TimeEnd, p.filter.Chunk, p.filter.Match, p.shardMigrationLabel, p.interCluster))))
	return path.Join("vmctl_migration_continue_on_restart", migrationTaskMd5)
}

func (p *vmNativeProcessor) do(ctx context.Context, f native.Filter, srcURL, dstURL string, bar barpool.Bar) error {

	retryableFunc := func() error { return p.runSingle(ctx, f, srcURL, dstURL, bar) }
	attempts, err := p.backoff.Retry(ctx, retryableFunc)
	p.s.Lock()
	p.s.retries += attempts
	p.s.Unlock()
	if err != nil {
		return fmt.Errorf("failed to migrate from %s to %s (retry attempts: %d): %w\nwith filter %s", srcURL, dstURL, attempts, err, f)
	}

	return nil
}

func (p *vmNativeProcessor) runSingle(ctx context.Context, f native.Filter, srcURL, dstURL string, bar barpool.Bar) error {
	reader, err := p.src.ExportPipe(ctx, srcURL, f)
	if err != nil {
		return fmt.Errorf("failed to init export pipe: %w", err)
	}

	//if p.shardMigrationLabel == "" {
	//	pr := bar.NewProxyReader(reader)
	//	if pr != nil {
	//		reader = pr
	//		fmt.Printf("Continue import process with filter %s:\n", f.String())
	//	}
	//}

	pr, pw := io.Pipe()
	importCh := make(chan error)
	go func() {
		importCh <- p.dst.ImportPipe(ctx, dstURL, pr)
		close(importCh)
	}()

	w := io.Writer(pw)
	if p.rateLimit > 0 {
		rl := limiter.NewLimiter(p.rateLimit)
		w = limiter.NewWriteLimiter(pw, rl)
	}

	written, err := io.Copy(w, reader)
	if err != nil {
		select {
		case err = <-importCh:
			return fmt.Errorf("failed to write into %q: error from import pipeline: %s", p.dst.Addr, err)
		default:
			return fmt.Errorf("failed to write into %q: %s", p.dst.Addr, err)
		}
	}

	p.s.Lock()
	p.s.bytes += uint64(written)
	p.s.requests++
	p.s.Unlock()

	if err := pw.Close(); err != nil {
		return err
	}

	return <-importCh
}

func (p *vmNativeProcessor) runBackfilling(ctx context.Context, tenantID string, ranges [][]time.Time, processedKeysCache *sync.Map) error {
	exportAddr := nativeExportAddr
	importAddr := nativeImportAddr
	if p.isNative {
		exportAddr += "/native"
		importAddr += "/native"
	}
	srcURL := fmt.Sprintf("%s/%s", p.src.Addr, exportAddr)

	importAddr, err := vm.AddExtraLabelsToImportPath(importAddr, p.dst.ExtraLabels)
	if err != nil {
		return fmt.Errorf("failed to add labels to import path: %s", err)
	}
	dstURL := fmt.Sprintf("%s/%s", p.dst.Addr, importAddr)

	if p.interCluster {
		srcURL = fmt.Sprintf("%s/select/%s/prometheus/%s", p.src.Addr, tenantID, exportAddr)
		dstURL = fmt.Sprintf("%s/insert/%s/prometheus/%s", p.dst.Addr, tenantID, importAddr)
	}

	initMessage := "Initing import process from %q to %q with filter %s"
	initParams := []any{srcURL, dstURL, p.filter.String()}
	if p.interCluster {
		initMessage = "Initing import process from %q to %q with filter %s for tenant %s"
		initParams = []any{srcURL, dstURL, p.filter.String(), tenantID}
	}

	fmt.Println("") // extra line for better output formatting
	log.Printf(initMessage, initParams...)
	if len(ranges) > 1 {
		log.Printf("Selected time range will be split into %d ranges according to %q step", len(ranges), p.filter.Chunk)
	}

	var foundSeriesMsg string
	var requestsToMake int
	var labelValues = map[string][][]time.Time{
		"": ranges,
	}

	barPrefix := "Requests to make"
	if p.interCluster {
		barPrefix = fmt.Sprintf("Requests to make for tenant %s", tenantID)
	}

	format := fmt.Sprintf(nativeWithBackoffTpl, barPrefix)
	if p.shardMigrationLabel != "" {
		format = fmt.Sprintf(nativeWithBackoffTpl, barPrefix)
		labelValues, err = p.explore(ctx, p.src, tenantID, ranges)
		if err != nil {
			return fmt.Errorf("failed to explore metric names: %s", err)
		}
		if len(labelValues) == 0 {
			errMsg := "no labelValues found"
			if tenantID != "" {
				errMsg = fmt.Sprintf("%s for tenant id: %s", errMsg, tenantID)
			}
			log.Println(errMsg)
			return nil
		}
		for _, m := range labelValues {
			requestsToMake += len(m)
		}
		foundSeriesMsg = fmt.Sprintf("Found %d unique label values to import. Total import/export requests to make %d", len(labelValues), requestsToMake)
	} else {
		requestsToMake = len(ranges)
	}

	if !p.interCluster {
		// do not prompt for intercluster because there could be many tenants,
		// and we don't want to interrupt the process when moving to the next tenant.
		question := foundSeriesMsg + ". Continue?"
		if !prompt(question) {
			return nil
		}
	} else {
		log.Print(foundSeriesMsg)
	}

	bar := barpool.NewSingleProgress(format, requestsToMake)
	bar.Start()
	defer bar.Finish()

	filterCh := make(chan native.Filter)
	errCh := make(chan error, p.cc)

	var wg sync.WaitGroup
	for i := 0; i < p.cc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range filterCh {
				cacheKey := fmt.Sprintf("tenant<%s>, match<%s>, timeStart<%s>, timeEnd<%s>", tenantID, f.Match, f.TimeStart, f.TimeEnd)
				if _, ok := processedKeysCache.Load(cacheKey); ok {
					//logger.Infof("skipping processed key %q, because it has been process in last migration.", cacheKey)
					bar.Increment()
					continue
				}

				if p.shardMigrationLabel != "" {
					if err := p.do(ctx, f, srcURL, dstURL, nil); err != nil {
						errCh <- err
						return
					}
					processedKeysCache.Store(cacheKey, true)
					bar.Increment()
				} else {
					if err := p.runSingle(ctx, f, srcURL, dstURL, nil); err != nil {
						errCh <- err
						return
					}
					bar.Increment()
				}
			}
		}()
	}

	// any error breaks the import
	loopCount := 0
	for labelValue, mRanges := range labelValues {
		match, err := buildMatchWithFilter(p.filter.Match, p.shardMigrationLabel, labelValue)
		if err != nil {
			logger.Errorf("failed to build filter %q for metric name %q: %s", p.filter.Match, labelValue, err)
			continue
		}

		for _, times := range mRanges {
			loopCount++
			if loopCount%50 == 0 {
				// save processKeys cache every 50 loops
				p.saveProcessedKeysCache(processedKeysCache)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("context canceled")
			case infErr := <-errCh:
				return fmt.Errorf("export/import error: %s", infErr)
			case filterCh <- native.Filter{
				Match:     match,
				TimeStart: times[0].Format(time.RFC3339),
				TimeEnd:   times[1].Format(time.RFC3339),
			}:
			}
		}
	}

	close(filterCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		return fmt.Errorf("import process failed: %s", err)
	}

	return nil
}

func (p *vmNativeProcessor) explore(ctx context.Context, src *native.Client, tenantID string, ranges [][]time.Time) (map[string][][]time.Time, error) {
	log.Printf("Exploring metrics...")

	bar := barpool.NewSingleProgress(fmt.Sprintf(nativeWithBackoffTpl, "Explore requests to make"), len(ranges))
	bar.Start()
	defer bar.Finish()

	metrics := make(map[string][][]time.Time)
	for _, r := range ranges {
		ms, err := src.Explore(ctx, p.filter, tenantID, r[0], r[1], p.shardMigrationLabel)
		if err != nil {
			return nil, fmt.Errorf("cannot get metrics from %s on interval %v-%v: %w", src.Addr, r[0], r[1], err)
		}
		for i := range ms {
			metrics[ms[i]] = append(metrics[ms[i]], r)
		}
		bar.Increment()
	}
	return metrics, nil
}

// stats represents client statistic
// when processing data
type stats struct {
	sync.Mutex
	startTime time.Time
	bytes     uint64
	requests  uint64
	retries   uint64
}

func (s *stats) String() string {
	s.Lock()
	defer s.Unlock()

	totalImportDuration := time.Since(s.startTime)
	totalImportDurationS := totalImportDuration.Seconds()
	bytesPerS := byteCountSI(0)
	if s.bytes > 0 && totalImportDurationS > 0 {
		bytesPerS = byteCountSI(int64(float64(s.bytes) / totalImportDurationS))
	}

	return fmt.Sprintf("VictoriaMetrics importer stats:\n"+
		"  time spent while importing: %v;\n"+
		"  total bytes: %s;\n"+
		"  bytes/s: %s;\n"+
		"  requests: %d;\n"+
		"  requests retries: %d;",
		totalImportDuration,
		byteCountSI(int64(s.bytes)), bytesPerS,
		s.requests, s.retries)
}

func byteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func buildMatchWithFilter(filter string, shardMigrationLabelName, shardMigrationLabelValue string) (string, error) {
	var tfss [][]storage.TagFilter
	var err error
	if filter != "" {
		tfss, err = searchutil.ParseMetricSelector(filter)
		if err != nil {
			return "", err
		}
	}

	nameFilter := fmt.Sprintf("%s=%q", shardMigrationLabelName, shardMigrationLabelValue)
	if len(tfss) == 0 {
		if shardMigrationLabelValue == "" {
			return "{__name__=~\".+\"}", nil
		}
		return fmt.Sprintf("{%s}", nameFilter), nil
	}

	var filters []string
	for _, tfs := range tfss {
		var a []string
		for _, tf := range tfs {
			if string(tf.Key) == shardMigrationLabelName {
				continue
			}
			a = append(a, tf.String())
		}
		a = append(a, nameFilter)
		filters = append(filters, strings.Join(a, ","))
	}

	match := "{" + strings.Join(filters, " or ") + "}"
	return match, nil
}
