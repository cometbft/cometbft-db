package db

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
)

// levelDBCollector is a Prometheus collector for LevelDB statistics.
type levelDBCollector struct {
	db      *leveldb.DB
	mu      sync.Mutex
	metrics map[string]prometheus.Histogram
}

// newLevelDBCollector creates a new LevelDBCollector.
func newLevelDBCollector(db *leveldb.DB, dbName string) *levelDBCollector {
	// Initialize Prometheus metrics
	names := getMetricNames()
	metrics := make(map[string]prometheus.Histogram, len(names))
	for _, field := range names {
		metrics[field] = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: PROMETHEUS_NAMESPACE,
			Subsystem: dbName,
			Name:      field,
			Help:      "LevelDB statistics: " + field,
			Buckets:   prometheus.ExponentialBuckets(0.0002, 10, 5),
		})
	}

	return &levelDBCollector{
		db:      db,
		metrics: metrics,
	}
}

func getMetricNames() []string {
	return []string{
		"WriteDelayCount",
		"WriteDelayDuration",

		"AliveSnapshots",
		"AliveIterators",

		"IOWrite",
		"IORead",

		"BlockCacheSize",
		"OpenedTablesCount",

		"TotalLevelSizes",
		"TotalLevelTablesCounts",
		"TotalLevelRead",
		"TotalLevelWrite",
		"TotalLevelDurations",
	}
}

// Describe implements the prometheus.Collector interface.
func (c *levelDBCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		metric.Describe(ch)
	}
}

// Collect implements the prometheus.Collector interface.
func (c *levelDBCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := make(map[string]float64)

	dbStats := leveldb.DBStats{}
	err := c.db.Stats(&dbStats)
	if err != nil {
		panic("Failed to get stats; qed")
	}

	stats["WriteDelayCount"] = float64(dbStats.WriteDelayCount)
	stats["WriteDelayDuration"] = float64(dbStats.WriteDelayDuration.Milliseconds())

	stats["AliveSnapshots"] = float64(dbStats.AliveSnapshots)
	stats["AliveIterators"] = float64(dbStats.AliveIterators)

	stats["IOWrite"] = float64(dbStats.IOWrite) / 1048576.0
	stats["IORead"] = float64(dbStats.IORead) / 1048576.0

	stats["BlockCacheSize"] = float64(dbStats.BlockCacheSize) / 1048576.0
	stats["OpenedTablesCount"] = float64(dbStats.OpenedTablesCount)

	// XXX: DBStats does not have a field with the number of levels, so we have
	// to use the length of the first slice.
	levels := len(dbStats.LevelSizes)
	totalLevelSizes := 0.0
	totalLevelTablesCounts := 0.0
	totalLevelRead := 0.0
	totalLevelWrite := 0.0
	totalLevelDurations := 0.0
	for i := 0; i < levels; i++ {
		totalLevelSizes += float64(dbStats.LevelSizes[i])
		totalLevelTablesCounts += float64(dbStats.LevelTablesCounts[i])
		totalLevelRead += float64(dbStats.LevelRead[i])
		totalLevelWrite += float64(dbStats.LevelWrite[i])
		totalLevelDurations += dbStats.LevelDurations[i].Seconds()
	}

	stats["TotalLevelSizes"] = totalLevelSizes / 1048576.0
	stats["TotalLevelTablesCounts"] = totalLevelTablesCounts
	stats["TotalLevelRead"] = totalLevelRead / 1048576.0
	stats["TotalLevelWrite"] = totalLevelWrite / 1048576.0
	stats["TotalLevelDurations"] = totalLevelDurations

	for name, value := range stats {
		if metric, ok := c.metrics[name]; ok {
			metric.Observe(value)
			metric.Collect(ch)
		}
	}
}
