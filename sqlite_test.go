package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteDBGetSetDelete(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewSQLiteDB(name, dir)
	require.NoError(t, err)
	defer db.Close()
	defer os.RemoveAll(dir)

	testBackendGetSetDelete(t, SQLiteDBBackend)
}

func TestSQLiteDBIterator(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewSQLiteDB(name, dir)
	require.NoError(t, err)
	defer db.Close()
	defer os.RemoveAll(dir)

	testDBIterator(t, SQLiteDBBackend)
}

func TestSQLiteDBBatch(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewSQLiteDB(name, dir)
	require.NoError(t, err)
	defer db.Close()
	defer os.RemoveAll(dir)

	testDBBatch(t, SQLiteDBBackend)
}

func BenchmarkSQLiteDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewSQLiteDB(name, dir)
	require.NoError(b, err)
	defer db.Close()
	defer os.RemoveAll(dir)

	benchmarkRandomReadsWrites(b, db)
}
