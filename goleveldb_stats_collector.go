package db

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syndtr/goleveldb/leveldb"
)

// levelDBCollector is a Prometheus collector for LevelDB statistics.
type levelDBCollector struct {
	db           *leveldb.DB
	mu           sync.Mutex
	metrics      map[string]prometheus.Gauge
	levelMetrics map[string]*prometheus.GaugeVec
}

// newLevelDBCollector creates a new LevelDBCollector.
func newLevelDBCollector(db *leveldb.DB, dbName string) *levelDBCollector {
	// Initialize Prometheus metrics
	names := getMetricNames()
	metrics := make(map[string]prometheus.Gauge, len(names))
	for _, field := range names {
		metrics[field] = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: PROMETHEUS_NAMESPACE,
			Subsystem: dbName,
			Name:      field,
			Help:      "LevelDB statistics: " + field,
		})
	}

	levelMetricsNames := []string{
		"LevelSizes",
		"LevelTablesCounts",
		"LevelRead",
		"LevelWrite",
		"LevelDurations",
	}
	levelMetrics := make(map[string]*prometheus.GaugeVec, len(levelMetricsNames))
	for _, field := range levelMetricsNames {
		levelMetrics[field] = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: PROMETHEUS_NAMESPACE,
			Subsystem: dbName,
			Name:      field,
			Help:      "LevelDB statistics: " + field,
		}, []string{"level"})
	}

	return &levelDBCollector{
		db:           db,
		metrics:      metrics,
		levelMetrics: levelMetrics,
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
	}
}

// Describe implements the prometheus.Collector interface.
func (c *levelDBCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		metric.Describe(ch)
	}
	for _, metric := range c.levelMetrics {
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

	stats["IOWrite"] = float64(dbStats.IOWrite)
	stats["IORead"] = float64(dbStats.IORead)

	stats["BlockCacheSize"] = float64(dbStats.BlockCacheSize)
	stats["OpenedTablesCount"] = float64(dbStats.OpenedTablesCount)

	for name, value := range stats {
		if metric, ok := c.metrics[name]; ok {
			metric.Set(value)
			metric.Collect(ch)
		}
	}

	// XXX: DBStats does not have a field with the number of levels, so we have
	// to use the length of the first slice.
	levels := len(dbStats.LevelSizes)
	for i := 0; i < levels; i++ {
		stats := make(map[string]float64)
		stats["LevelSizes"] = float64(dbStats.LevelSizes[i])
		stats["LevelTablesCounts"] = float64(dbStats.LevelTablesCounts[i])
		stats["LevelRead"] = float64(dbStats.LevelRead[i])
		stats["LevelWrite"] = float64(dbStats.LevelWrite[i])
		stats["LevelDurations"] = float64(dbStats.LevelDurations[i].Seconds())

		for name, value := range stats {
			if metric, ok := c.levelMetrics[name]; ok {
				metric.WithLabelValues(fmt.Sprint(i)).Set(value)
				metric.Collect(ch)
			}
		}
	}

}
