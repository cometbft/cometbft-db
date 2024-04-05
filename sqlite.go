package db

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"

	_ "modernc.org/sqlite" // this is the conventional way to import the sqlite driver when using modernc.org/sqlite
)

func init() {
	registerDBCreator(SQLiteDBBackend, sqliteDBCreator)
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

	// when value is empty, return []byte{} instead of nil
	if len(value) == 0 {
		return []byte{}, nil
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
		stmt += " WHERE key <= ?"
		args = append(args, start)
	}
	if end != nil {
		if start != nil {
			stmt += " AND key > ?"
		} else {
			stmt += " WHERE key > ?"
		}
		args = append(args, end)
	}
	stmt += " ORDER BY key DESC"

	rows, err := db.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}

	return newSQLiteIterator(rows, start, end, true), nil
}

// Compact implements DB.
func (*SQLiteDB) Compact(_, _ []byte) error {
	// SQLite does not support manual compaction, so this is a no-op.
	return nil
}
