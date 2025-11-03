package db

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	pebble2 "github.com/cockroachdb/pebble/v2"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebbleDB(name, dir)
	}
	registerDBCreator(PebbleDBBackend, dbCreator)

	dbCreator2 := func(name string, dir string) (DB, error) {
		return NewPebble2DB(name, dir)
	}
	registerDBCreator(Pebble2DBBackend, dbCreator2)
}

// PebbleDB is a PebbleDB backend.
type PebbleDB struct {
	db pebbleDB
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string) (*PebbleDB, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	return NewPebbleDBWithOpts(name, dir, opts)
}

func NewPebbleDBWithOpts(name string, dir string, opts *pebble.Options) (*PebbleDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	opts.EnsureDefaults()
	p, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleDB{
		db: &pebbleV1Adapter{p},
	}, err
}

// Get implements DB.
func (db *PebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	res, closer, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	if closer == nil {
		// Key not found
		return nil, nil
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
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	err := db.db.Set(key, value, db.db.noSyncOpts())
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *PebbleDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Set(key, value, db.db.syncOpts())
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *PebbleDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	return db.db.Delete(key, db.db.noSyncOpts())
}

// DeleteSync implements DB.
func (db PebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.db.Delete(key, db.db.syncOpts())
}

func (db *PebbleDB) DB() interface{} {
	return db.db.DB()
}

func (db *PebbleDB) Compact(start, end []byte) (err error) {
	// Currently nil,nil is an invalid range in Pebble.
	// This was taken from https://github.com/cockroachdb/pebble/issues/1474
	// In case the start and end keys are the same
	// pebbleDB will throw an error that it cannot compact.
	if start != nil && end != nil {
		return db.db.Compact(start, end, true)
	}
	iter, err := db.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() {
		err2 := iter.Close()
		if err2 != nil {
			err = err2
		}
	}()
	if start == nil && iter.First() {
		start = append(start, iter.Key()...)
	}
	if end == nil && iter.Last() {
		end = append(end, iter.Key()...)
	}
	return db.db.Compact(start, end, true)
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
func (*PebbleDB) Stats() map[string]string {
	return nil
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

// Iterator implements DB.
func (db *PebbleDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebbleIterOptions{
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
	o := pebbleIterOptions{
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
	db    *PebbleDB
	batch pebbleBatch
}

var _ Batch = (*pebbleDBBatch)(nil)

func newPebbleDBBatch(db *PebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		// For regular batch operations batch.db is going to be set to db
		// and it is not needed to initialize the DB here.
		// This is set to enable general DB operations like compaction
		// (e.x. a call do pebbleDBBatch.db.Compact() would throw a nil pointer exception)
		db:    db,
		batch: db.db.NewBatch(),
	}
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
func (b *pebbleDBBatch) Write() error {
	if b.batch == nil {
		return errBatchClosed
	}

	err := b.batch.Commit(b.db.db.noSyncOpts())
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.

	return b.Close()
}

// WriteSync implements Batch.
func (b *pebbleDBBatch) WriteSync() error {
	if b.batch == nil {
		return errBatchClosed
	}
	err := b.batch.Commit(b.db.db.syncOpts())
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
	source     pebbleIter
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ Iterator = (*pebbleDBIterator)(nil)

func newPebbleDBIterator(source pebbleIter, start, end []byte, isReverse bool) *pebbleDBIterator {
	if isReverse {
		if end == nil {
			source.Last()
		}
	} else {
		if start == nil {
			source.First()
		}
	}
	return &pebbleDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (itr *pebbleDBIterator) Domain() (start []byte, end []byte) {
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
// The caller should not modify the contents of the returned slice.
// Instead, the caller should make a copy and work on the copy.
func (itr *pebbleDBIterator) Key() []byte {
	itr.assertIsValid()
	return itr.source.Key()
}

// Value implements Iterator.
// The caller should not modify the contents of the returned slice.
// Instead, the caller should make a copy and work on the copy.
func (itr *pebbleDBIterator) Value() []byte {
	itr.assertIsValid()
	return itr.source.Value()
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
	return nil
}

func (itr *pebbleDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

// pebbleDB is an interface unifying pebble db v1 and v2 implementations.
type pebbleDB interface {
	Get(key []byte) (value []byte, closer io.Closer, err error)
	Set(key, value []byte, opts pebbleWriteOptions) error
	Delete(key []byte, opts pebbleWriteOptions) error
	NewBatch() pebbleBatch
	NewIter(opts *pebbleIterOptions) (pebbleIter, error)
	Compact(start, end []byte, parallelize bool) error
	Close() error

	DB() interface{}

	// syncOpts returns the appropriate sync/nosync options for this adapter
	syncOpts() pebbleWriteOptions
	noSyncOpts() pebbleWriteOptions
}

type pebbleBatch interface {
	Set(key, value []byte, opts pebbleWriteOptions) error
	Delete(key []byte, opts pebbleWriteOptions) error
	Commit(opts pebbleWriteOptions) error
	Close() error
}

type pebbleIter interface {
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type pebbleWriteOptions interface{}

type pebbleIterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

type pebbleV1Adapter struct {
	db *pebble.DB
}

func (p *pebbleV1Adapter) Get(key []byte) (value []byte, closer io.Closer, err error) {
	value, closer, err = p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil, nil
	}
	return value, closer, err
}

func (p *pebbleV1Adapter) Set(key []byte, value []byte, opts pebbleWriteOptions) error {
	return p.db.Set(key, value, opts.(*pebble.WriteOptions))
}

func (p *pebbleV1Adapter) Delete(key []byte, opts pebbleWriteOptions) error {
	return p.db.Delete(key, opts.(*pebble.WriteOptions))
}

func (p *pebbleV1Adapter) NewBatch() pebbleBatch {
	return &pebbleV1BatchAdapter{batch: p.db.NewBatch()}
}

func (p *pebbleV1Adapter) NewIter(opts *pebbleIterOptions) (pebbleIter, error) {
	var iterOpts *pebble.IterOptions
	if opts != nil {
		iterOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}
	return p.db.NewIter(iterOpts)
}

func (p *pebbleV1Adapter) Compact(start []byte, end []byte, parallelize bool) error {
	return p.db.Compact(start, end, parallelize)
}

func (p *pebbleV1Adapter) Close() error {
	return p.db.Close()
}

func (p *pebbleV1Adapter) DB() interface{} {
	return p.db
}

func (*pebbleV1Adapter) syncOpts() pebbleWriteOptions {
	return pebble.Sync
}

func (*pebbleV1Adapter) noSyncOpts() pebbleWriteOptions {
	return pebble.NoSync
}

type pebbleV1BatchAdapter struct {
	batch *pebble.Batch
}

func (b *pebbleV1BatchAdapter) Set(key, value []byte, opts pebbleWriteOptions) error {
	var writeOpts *pebble.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble.WriteOptions)
	}
	return b.batch.Set(key, value, writeOpts)
}

func (b *pebbleV1BatchAdapter) Delete(key []byte, opts pebbleWriteOptions) error {
	var writeOpts *pebble.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble.WriteOptions)
	}
	return b.batch.Delete(key, writeOpts)
}

func (b *pebbleV1BatchAdapter) Commit(opts pebbleWriteOptions) error {
	var writeOpts *pebble.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble.WriteOptions)
	}
	return b.batch.Commit(writeOpts)
}

func (b *pebbleV1BatchAdapter) Close() error {
	return b.batch.Close()
}

type pebbleV2Adapter struct {
	db *pebble2.DB
}

func (p *pebbleV2Adapter) Get(key []byte) (value []byte, closer io.Closer, err error) {
	value, closer, err = p.db.Get(key)
	if err == pebble2.ErrNotFound {
		return nil, nil, nil
	}
	return value, closer, err
}

func (p *pebbleV2Adapter) Set(key []byte, value []byte, opts pebbleWriteOptions) error {
	return p.db.Set(key, value, opts.(*pebble2.WriteOptions))
}

func (p *pebbleV2Adapter) Delete(key []byte, opts pebbleWriteOptions) error {
	return p.db.Delete(key, opts.(*pebble2.WriteOptions))
}

func (p *pebbleV2Adapter) NewBatch() pebbleBatch {
	return &pebbleV2BatchAdapter{batch: p.db.NewBatch()}
}

func (p *pebbleV2Adapter) NewIter(opts *pebbleIterOptions) (pebbleIter, error) {
	var iterOpts *pebble2.IterOptions
	if opts != nil {
		iterOpts = &pebble2.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}
	return p.db.NewIter(iterOpts)
}

func (p *pebbleV2Adapter) Compact(start []byte, end []byte, parallelize bool) error {
	return p.db.Compact(context.TODO(), start, end, parallelize)
}

func (p *pebbleV2Adapter) Close() error {
	return p.db.Close()
}

func (p *pebbleV2Adapter) DB() interface{} {
	return p.db
}

func (*pebbleV2Adapter) syncOpts() pebbleWriteOptions {
	return pebble2.Sync
}

func (*pebbleV2Adapter) noSyncOpts() pebbleWriteOptions {
	return pebble2.NoSync
}

type pebbleV2BatchAdapter struct {
	batch *pebble2.Batch
}

func (b *pebbleV2BatchAdapter) Set(key, value []byte, opts pebbleWriteOptions) error {
	var writeOpts *pebble2.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble2.WriteOptions)
	}
	return b.batch.Set(key, value, writeOpts)
}

func (b *pebbleV2BatchAdapter) Delete(key []byte, opts pebbleWriteOptions) error {
	var writeOpts *pebble2.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble2.WriteOptions)
	}
	return b.batch.Delete(key, writeOpts)
}

func (b *pebbleV2BatchAdapter) Commit(opts pebbleWriteOptions) error {
	var writeOpts *pebble2.WriteOptions
	if opts != nil {
		writeOpts = opts.(*pebble2.WriteOptions)
	}
	return b.batch.Commit(writeOpts)
}

func (b *pebbleV2BatchAdapter) Close() error {
	return b.batch.Close()
}

// Pebble2DB is a PebbleDB v2 backend.
type Pebble2DB struct {
	*PebbleDB
}

var _ DB = (*Pebble2DB)(nil)

func NewPebble2DB(name string, dir string) (*Pebble2DB, error) {
	opts := &pebble2.Options{}
	opts.EnsureDefaults()
	return NewPebble2DBWithOpts(name, dir, opts)
}

func NewPebble2DBWithOpts(name string, dir string, opts *pebble2.Options) (*Pebble2DB, error) {
	dbPath := filepath.Join(dir, name+".db")
	opts.EnsureDefaults()
	p, err := pebble2.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}

	// Use the pebbleV2Adapter
	pdb := &PebbleDB{
		db: &pebbleV2Adapter{db: p},
	}

	return &Pebble2DB{PebbleDB: pdb}, nil
}

// DB returns the underlying pebble v2 DB.
func (db *Pebble2DB) DB() *pebble2.DB {
	return db.PebbleDB.db.DB().(*pebble2.DB)
}
