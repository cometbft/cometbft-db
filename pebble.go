package db

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// ForceSync
/*
This is set at compile time. Could be 0 or 1, defaults is 0.
It will force using Sync for NoSync functions (Set, Delete, Write)

Used as a workaround for chain-upgrade issue: At the upgrade-block, the sdk will panic without flushing data to disk or
closing dbs properly.

Upgrade guide:
	1. After seeing `UPGRADE "xxxx" NEED at height....`, restart current version with `-X github.com/tendermint/tm-db.ForceSync=1`
	2. Restart new version as normal


Example: Upgrading sifchain from v0.14.0 to v0.15.0

# log:
panic: UPGRADE "0.15.0" NEEDED at height: 8170210: {"binaries":{"linux/amd64":"https://github.com/Sifchain/sifnode/releases/download/v0.15.0/sifnoded-v0.15.0-linux-amd64.zip?checksum=0c03b5846c5a13dcc0d9d3127e4f0cee0aeddcf2165177b2f2e0d60dbcf1a5ea"}}

# step1
git reset --hard
git checkout v0.14.0
go mod edit -replace github.com/tendermint/tm-db=github.com/baabeetaa/tm-db@pebble
go mod tidy
go install -tags pebbledb -ldflags "-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb -X github.com/tendermint/tm-db.ForceSync=1" ./cmd/sifnoded

$HOME/go/bin/sifnoded start --db_backend=pebbledb


# step 2
git reset --hard
git checkout v0.15.0
go mod edit -replace github.com/tendermint/tm-db=github.com/baabeetaa/tm-db@pebble
go mod tidy
go install -tags pebbledb -ldflags "-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb" ./cmd/sifnoded

$HOME/go/bin/sifnoded start --db_backend=pebbledb

*/
var (
	ForceSync   = "0"
	isForceSync = false
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebbleDB(name, dir)
	}
	registerDBCreator(PebbleDBBackend, dbCreator)

	if ForceSync == "1" {
		isForceSync = true
	}
}

// PebbleDB is a PebbleDB backend.
type PebbleDB struct {
	db *pebble.DB
}

var iteratorPool = sync.Pool{
	New: func() interface{} {
		return &pebbleDBIterator{}
	},
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string) (*PebbleDB, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	return NewPebbleDBWithOpts(name, dir)
}

func NewPebbleDBWithOpts(name string, dir string) (*PebbleDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	opts := &pebble.Options{
		Cache:        pebble.NewCache(1 << 32), // 4GB
		MemTableSize: 1 << 31,                  // 4GBå
		MaxOpenFiles: 5000,
		Experimental: pebble.ExperimentalOptions{
			L0CompactionConcurrency: 4, // default is 1
			L0SublevelCompaction:    true,
			L0StopWritesThreshold:   1000,
		},
	}
	p, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleDB{
		db: p,
	}, nil
}

// Get implements DB.
func (db *PebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	res, closer, err := db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return cp(res), nil
}

// Has implements DB.
func (db *PebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	bytesPeb, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytesPeb != nil, nil
}

// Set implements DB.
func (db *PebbleDB) Set(key []byte, value []byte) error {
	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}
	return db.set(key, value, *wopts)
}

// SetSync implements DB.
func (db *PebbleDB) SetSync(key []byte, value []byte) error {
	return db.set(key, value, *pebble.Sync)
}

// Delete implements DB.
func (db *PebbleDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}
	err := db.db.Delete(key, wopts)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db PebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(key, pebble.Sync)
	if err != nil {
		return nil
	}
	return nil
}

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

// Close implements DB.
func (db PebbleDB) Close() error {
	db.db.Close()
	return nil
}

// Print implements DB.
func (db *PebbleDB) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *PebbleDB) Stats() map[string]string {
	m := db.db.Metrics()
	stats := make(map[string]string)

	stats["BlockCacheSize"] = fmt.Sprintf("%d", m.BlockCache.Size)
	stats["BlockCacheHits"] = fmt.Sprintf("%d", m.BlockCache.Hits)
	stats["BlockCacheMisses"] = fmt.Sprintf("%d", m.BlockCache.Misses)
	stats["MemTableSize"] = fmt.Sprintf("%d", m.MemTable.Size)
	stats["Flushes"] = fmt.Sprintf("%d", m.Flush.Count)
	stats["Compactions"] = fmt.Sprintf("%d", m.Compact.Count)
	return stats
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

func newPebbleDBBatch(db *PebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		batch: db.db.NewBatch(),
	}
}

// Iterator implements DB.
func (db *PebbleDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
	itr.First()

	return newPebbleDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *PebbleDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
	itr.Last()
	return newPebbleDBIterator(itr, start, end, true), nil
}

var _ Batch = (*pebbleDBBatch)(nil)

type pebbleDBBatch struct {
	batch *pebble.Batch
}

var _ Batch = (*pebbleDBBatch)(nil)

func (b *pebbleDBBatch) Write() error {
	if b.batch == nil {
		return errBatchClosed
	}

	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}
	err := b.batch.Commit(wopts)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.

	return b.Close()
}

// Set implements Batch.
func (b *pebbleDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.batch == nil {
		return errBatchClosed
	}

	return b.batch.Set(key, value, nil)
}

// Delete implements Batch.
func (b *pebbleDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}

	return b.batch.Delete(key, nil)
}

// Write implements Batch.

// WriteSync implements Batch.
func (b *pebbleDBBatch) WriteSync() error {
	if b.batch == nil {
		return errBatchClosed
	}
	err := b.batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *pebbleDBBatch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = nil
	}

	return nil
}

type pebbleDBIterator struct {
	source     *pebble.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ Iterator = (*pebbleDBIterator)(nil)

func newPebbleDBIterator(source *pebble.Iterator, start, end []byte, isReverse bool) *pebbleDBIterator {
	item := iteratorPool.Get()
	itr, ok := item.(*pebbleDBIterator)
	if !ok {
		panic("item in iteratorPool is not of type *pebbleDBIterator")
	}
	itr.source = source
	itr.start = start
	itr.end = end
	itr.isReverse = isReverse
	itr.isInvalid = false

	if isReverse {
		if end == nil {
			source.Last()
		}
	} else {
		if start == nil {
			source.First()
		}
	}
	return itr
}

// Domain implements Iterator.
func (itr *pebbleDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *pebbleDBIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.source.Error(); err != nil {
		itr.isInvalid = true

		return false
	}

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true

		return false
	}

	// If key is end or past it, invalid.
	start := itr.start
	end := itr.end
	key := itr.source.Key()
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true

			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true

			return false
		}
	}

	// It's valid.
	return true
}

// Key implements Iterator.
func (itr *pebbleDBIterator) Key() []byte {
	itr.assertIsValid()
	return cp(itr.source.Key())
}

// Value implements Iterator.
func (itr *pebbleDBIterator) Value() []byte {
	itr.assertIsValid()
	return cp(itr.source.Value())
}

// Next implements Iterator.
func (itr pebbleDBIterator) Next() {
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// Error implements Iterator.
func (itr *pebbleDBIterator) Error() error {
	return itr.source.Error()
}

// Close implements Iterator.
func (itr *pebbleDBIterator) Close() error {
	err := itr.source.Close()
	if err != nil {
		return err
	}
	itr.source = nil
	itr.start = nil
	itr.end = nil
	itr.isReverse = false
	itr.isInvalid = true
	iteratorPool.Put(itr)
	return nil
}

func (itr *pebbleDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

// helper function to reduce code duplication
func (db *PebbleDB) set(key []byte, value []byte, pebbleSync pebble.WriteOptions) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	err := db.db.Set(key, value, &pebbleSync)
	if err != nil {
		return err
	}
	return nil
}
