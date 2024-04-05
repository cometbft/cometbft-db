package db

import (
	"bytes"
	"database/sql"
)

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
		itr.last()
	} else {
		itr.first()
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

func (itr *sqliteIterator) next() {
	if itr.rows.Next() {
		itr.scanRow()
	} else {
		itr.isInvalid = true
	}
}

func (itr *sqliteIterator) prev() {
	itr.isInvalid = true
}

func (itr *sqliteIterator) scanRow() {
	err := itr.rows.Scan(&itr.key, &itr.value)
	if err != nil {
		itr.isInvalid = true
	}
}
