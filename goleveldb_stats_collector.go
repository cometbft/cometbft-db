package db

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syndtr/goleveldb/leveldb"
)

// LevelDBCollector is a Prometheus collector for LevelDB statistics.
type LevelDBCollector struct {
	db           *leveldb.DB
	mu           sync.Mutex
	metrics      map[string]prometheus.Gauge
	levelMetrics map[string]*prometheus.GaugeVec
}

// NewLevelDBCollector creates a new LevelDBCollector.
func NewLevelDBCollector(db *leveldb.DB) *LevelDBCollector {
	// Initialize Prometheus metrics
	metrics := make(map[string]prometheus.Gauge)
	for _, field := range getMetricNames() {
		metrics[field] = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "leveldb",
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
	levelMetrics := make(map[string]*prometheus.GaugeVec)
	for _, field := range levelMetricsNames {
		levelMetrics[field] = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "leveldb",
			Name:      field,
			Help:      "LevelDB statistics: " + field,
		}, []string{"level"})
	}

	return &LevelDBCollector{
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
func (c *LevelDBCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		metric.Describe(ch)
	}
}

// Collect implements the prometheus.Collector interface.
func (c *LevelDBCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := make(map[string]float64)

	var dbStats *leveldb.DBStats
	err := c.db.Stats(dbStats)
	if err != nil {
		panic("Failed to get stats; qed")
	}

	stats["WriteDelayCount"] = float64(dbStats.WriteDelayCount)
	stats["WriteDelayDuration"] = float64(dbStats.WriteDelayDuration.Nanoseconds())

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

	levels := len(dbStats.LevelSizes)
	for i := 0; i < levels; i++ {
		stats := make(map[string]float64)
		stats["LevelSizes"] = float64(dbStats.LevelSizes[i])
		stats["LevelTablesCounts"] = float64(dbStats.LevelTablesCounts[i])
		stats["LevelRead"] = float64(dbStats.LevelRead[i])
		stats["LevelWrite"] = float64(dbStats.LevelWrite[i])
		stats["LevelDurations"] = float64(dbStats.LevelDurations[i])

		for name, value := range stats {
			if metric, ok := c.levelMetrics[name]; ok {
				metric.WithLabelValues(fmt.Sprint(i)).Set(value)
				metric.Collect(ch)
			}
		}
	}

}

func main() {
	// Open a LevelDB database (replace this with your LevelDB initialization logic)
	db, err := leveldb.OpenFile("/path/to/your/db", nil)
	if err != nil {
		// Handle the error, e.g., log it and exit.
		panic(err)
	}
	defer db.Close()

	// Create a new LevelDBCollector
	collector := NewLevelDBCollector(db)

	// Register the collector with Prometheus
	prometheus.MustRegister(collector)

	// Set up an HTTP handler to expose the metrics
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}
