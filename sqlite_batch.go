package db

import (
	"database/sql"
	"fmt"
)

// ============ BATCH ===============

var _ Batch = (*sqliteBatch)(nil)

type sqliteBatch struct {
	db  *SQLiteDB
	tx  *sql.Tx
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
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *sqliteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *sqliteBatch) Write() error {
	if b.tx != nil {
		return fmt.Errorf("batch already written or not properly closed")
	}
	tx, err := b.db.db.Begin()
	if err != nil {
		return err
	}
	b.tx = tx
	err = b.write(false)
	if err != nil {
		rollErr := b.tx.Rollback()
		if rollErr != nil {
			return fmt.Errorf("write failed: %v, rollback failed: %v", err, rollErr)
		}
		b.tx = nil
		return err
	}
	return b.tx.Commit()
}

// WriteSync implements Batch.
func (b *sqliteBatch) WriteSync() error {
	if b.tx != nil {
		return fmt.Errorf("batch already written or not properly closed")
	}
	tx, err := b.db.db.Begin()
	if err != nil {
		return err
	}
	b.tx = tx
	err = b.write(true)
	if err != nil {
		rollErr := b.tx.Rollback()
		if rollErr != nil {
			return fmt.Errorf("write failed: %v, rollback failed: %v", err, rollErr)
		}
		b.tx = nil
		return err
	}
	return b.tx.Commit()
}

func (b *sqliteBatch) write(sync bool) error {
	if b.tx == nil {
		return fmt.Errorf("cannot write to closed batch")
	}

	for _, op := range b.ops {
		switch op.opType {
		case opTypeSet:
			_, err := b.tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", op.key, op.value)
			if err != nil {
				return err
			}
		case opTypeDelete:
			_, err := b.tx.Exec("DELETE FROM kv WHERE key = ?", op.key)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown operation type: %v", op.opType)
		}
	}

	// Clear the batch after writing
	b.ops = nil

	if sync {
		return b.tx.Commit()
	}
	return nil
}

// Close implements Batch.
func (b *sqliteBatch) Close() error {
	if b.tx != nil {
		err := b.tx.Rollback()
		b.tx = nil
		if err != nil {
			return err
		}
	}
	b.ops = nil
	return nil
}
