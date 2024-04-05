//go:build sqlite
// +build sqlite

package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"

	_ "github.com/glebarez/go-sqlite"
)

func init() {
	registerDBCreator(SQLiteBackend, sqliteDBCreator)
}

func sqliteDBCreator(name, dir string) (DB, error) {
	return NewSQLiteDB(name, dir)
}

type SQLiteDB struct {
	db *sql.DB
}

var _ DB = (*SQLiteDB)(nil)

func NewSQLiteDB(dbName, dir string) (*SQLiteDB, error) {
	dbPath := filepath.Join(dir, dbName+".db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kv (
		key BLOB PRIMARY KEY ON CONFLICT REPLACE,
		value BLOB
	) WITHOUT ROWID`)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &SQLiteDB{db: db}, nil
}

// Get implements DB.
func (db *SQLiteDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	var value []byte
	err := db.db.QueryRow("SELECT value FROM kv WHERE key = ?", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	// sqlite.QueryRow().Scan() returns an empty byte slice if there is no value.
	// Get() is expected to return nil in this case.
	if len(value) == 0 {
		return nil, nil
	}

	return value, nil
}

// Has implements DB.
func (db *SQLiteDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	var count int
	err := db.db.QueryRow("SELECT COUNT(*) FROM kv WHERE key = ?", key).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Set implements DB.
func (db *SQLiteDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	_, err := db.db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
	return err
}

// SetSync implements DB.
func (db *SQLiteDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *SQLiteDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	_, err := db.db.Exec("DELETE FROM kv WHERE key = ?", key)
	return err
}

// DeleteSync implements DB.
func (db *SQLiteDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Close implements DB.
func (db *SQLiteDB) Close() error {
	return db.db.Close()
}

// Print implements DB.
func (db *SQLiteDB) Print() error {
	rows, err := db.db.Query("SELECT key, value FROM kv")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key, value []byte
		err := rows.Scan(&key, &value)
		if err != nil {
			return err
		}
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return rows.Err()
}

// Stats implements DB.
func (*SQLiteDB) Stats() map[string]string {
	return nil
}

// NewBatch implements DB.
func (db *SQLiteDB) NewBatch() Batch {
	return newSQLiteBatch(db)
}

// Iterator implements DB.
func (db *SQLiteDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	// Ensure the iterator includes the start key and excludes the end key.
	stmt := "SELECT key, value FROM kv"
	args := []interface{}{}

	if start != nil {
		stmt += " WHERE key >= ?"
		args = append(args, start)
	}
	if end != nil {
		stmt += " AND key < ?"
		args = append(args, end)
	}
	stmt += " ORDER BY key"

	rows, err := db.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}

	return newSQLiteIterator(rows, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *SQLiteDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	stmt := "SELECT key, value FROM kv"
	args := []interface{}{}

	if start != nil {
		stmt += " WHERE key < ?"
		args = append(args, start)
	}
	if end != nil {
		if start != nil {
			stmt += " AND key >= ?"
		} else {
			stmt += " WHERE key >= ?"
		}
		args = append(args, end)
	}
	stmt += " ORDER BY key DESC"

	rows, err := db.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}

	return newSQLiteIterator(rows, end, start, true), nil
}

// Compact implements DB.
func (*SQLiteDB) Compact(_, _ []byte) error {
	// SQLite does not support manual compaction, so this is a no-op.
	return nil
}

// ============ BATCH ===============

var _ Batch = (*sqliteBatch)(nil)

type sqliteBatch struct {
	db  *SQLiteDB
	tx  *sql.Stmt
	ops []operation
}

func newSQLiteBatch(db *SQLiteDB) *sqliteBatch {
	return &sqliteBatch{
		db:  db,
		ops: []operation{},
	}
}

// Set implements Batch.
func (b *sqliteBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.tx == nil {
		return errBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *sqliteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.tx == nil {
		return errBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *sqliteBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *sqliteBatch) WriteSync() error {
	return b.write(true)
}

func (b *sqliteBatch) write(sync bool) error {
	if b.tx == nil {
		return errBatchClosed
	}

	tx, err := b.db.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
			if err != nil {
				log.Printf("failed to rollback transaction: %v", err)
			}
		}
	}()

	for _, op := range b.ops {
		switch op.opType {
		case opTypeSet:
			_, err = tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", op.key, op.value)
		case opTypeDelete:
			_, err = tx.Exec("DELETE FROM kv WHERE key = ?", op.key)
		default:
			err = fmt.Errorf("unknown operation type: %v", op.opType)
		}
		if err != nil {
			return err
		}
	}

	if sync {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		return err
	}

	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *sqliteBatch) Close() error {
	b.ops = nil
	b.tx = nil
	return nil
}

// =========== ITERATOR ================

var _ Iterator = (*sqliteIterator)(nil)

type sqliteIterator struct {
	rows       *sql.Rows
	start, end []byte
	isReverse  bool
	isInvalid  bool
	key, value []byte
}

func newSQLiteIterator(rows *sql.Rows, start, end []byte, isReverse bool) *sqliteIterator {
	itr := &sqliteIterator{
		rows:      rows,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
	if isReverse {
		if end == nil {
			itr.last()
		} else {
			itr.seek(end)
			if itr.valid() {
				eoakey := itr.key // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					itr.prev()
				}
			} else {
				itr.last()
			}
		}
	} else {
		if start == nil {
			itr.first()
		} else {
			itr.seek(start)
		}
	}
	return itr
}

func (itr *sqliteIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *sqliteIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source errors, invalid.
	if err := itr.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	start := itr.start
	end := itr.end
	key := itr.key
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

	// Valid
	return true
}

func (itr *sqliteIterator) Key() []byte {
	itr.assertIsValid()
	return cp(itr.key)
}

func (itr *sqliteIterator) Value() []byte {
	itr.assertIsValid()
	return cp(itr.value)
}

func (itr *sqliteIterator) Next() {
	itr.assertIsValid()
	if itr.isReverse {
		itr.prev()
	} else {
		itr.next()
	}
}

func (itr *sqliteIterator) Error() error {
	return itr.rows.Err()
}

func (itr *sqliteIterator) Close() error {
	return itr.rows.Close()
}

func (itr *sqliteIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

func (itr *sqliteIterator) first() {
	if itr.rows.Next() {
		itr.scanRow()
	} else {
		itr.isInvalid = true
	}
}

func (itr *sqliteIterator) last() {
	for itr.rows.Next() {
		itr.scanRow()
	}
}

func (itr *sqliteIterator) seek(key []byte) {
	for itr.rows.Next() {
		itr.scanRow()
		if bytes.Compare(itr.key, key) >= 0 {
			break
		}
	}
}

func (itr *sqliteIterator) next() {
	if itr.rows.Next() {
		itr.scanRow()
	} else {
		itr.isInvalid = true
	}
}

func (itr *sqliteIterator) prev() {
	if itr.rows.Next() {
		itr.scanRow()
	} else {
		itr.isInvalid = true
	}
}

func (itr *sqliteIterator) valid() bool {
	return !itr.isInvalid
}

func (itr *sqliteIterator) scanRow() {
	err := itr.rows.Scan(&itr.key, &itr.value)
	if err != nil {
		itr.isInvalid = true
	}
}
