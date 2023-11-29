package db

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	batchDurationNs     prometheus.Gauge
	batchSyncDurationNs prometheus.Gauge
)

func init() {
	batchDurationNs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cometbft",
		Subsystem: "db",
		Name:      "batch_duration_ns",
		Help:      "The duration of the batch#write operation in nanoseconds.",
	})
	prometheus.MustRegister(batchDurationNs)
	batchSyncDurationNs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cometbft",
		Subsystem: "db",
		Name:      "batch_sync_duration_ns",
		Help:      "The duration of the batch#write(sync) operation in nanoseconds.",
	})
	prometheus.MustRegister(batchSyncDurationNs)
}

type goLevelDBBatch struct {
	db    *GoLevelDB
	batch *leveldb.Batch
}

var _ Batch = (*goLevelDBBatch)(nil)

func newGoLevelDBBatch(db *GoLevelDB) *goLevelDBBatch {
	return &goLevelDBBatch{
		db:    db,
		batch: new(leveldb.Batch),
	}
}

// Set implements Batch.
func (b *goLevelDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.batch == nil {
		return errBatchClosed
	}
	b.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (b *goLevelDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}
	b.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (b *goLevelDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *goLevelDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *goLevelDBBatch) write(sync bool) error {
	if b.batch == nil {
		return errBatchClosed
	}
	start := time.Now()
	err := b.db.db.Write(b.batch, &opt.WriteOptions{Sync: sync})
	if sync {
		batchSyncDurationNs.Set(float64(time.Since(start).Nanoseconds()))
	} else {
		batchDurationNs.Set(float64(time.Since(start).Nanoseconds()))
	}
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *goLevelDBBatch) Close() error {
	if b.batch != nil {
		b.batch.Reset()
		b.batch = nil
	}
	return nil
}
