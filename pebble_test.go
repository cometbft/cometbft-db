package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPebbleLegacyFormatSaysWhatToDo covers the one failure an operator meets
// by upgrading in the wrong order. These stores open before the application's,
// so this is where a node that skipped the ratchet release stops -- and the
// move that would normally follow, letting the new build migrate it, is
// exactly the one that cannot work.
//
// A legacy store is one carrying a CURRENT file, which is what pebble looks
// for; writing one is enough to reach the check without a v1 build to make a
// real one.
func TestPebbleLegacyFormatSaysWhatToDo(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "blockstore.db")
	require.NoError(t, os.MkdirAll(dbPath, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dbPath, "CURRENT"), []byte("MANIFEST-000001\n"), 0o644))

	_, err := NewPebbleDB("blockstore", dir)
	require.Error(t, err)
	t.Logf("operator sees:\n%v", err)

	require.ErrorContains(t, err, "format major version")
	require.ErrorContains(t, err, "previous release")
	require.ErrorContains(t, err, "cannot upgrade it")
	require.ErrorContains(t, err, "Nothing has been changed on disk")
}

// TestPebbleOtherOpenErrorsAreUntouched: the hint is for one failure, and must
// not be pinned to every other way an open can go wrong.
func TestPebbleOtherOpenErrorsAreUntouched(t *testing.T) {
	orig := errors.New("pebble: some other trouble entirely")
	require.Equal(t, orig, withUpgradeHint(orig))
}

// v1FormatNewest is the highest format major version pebble v1 can open. It is
// the ceiling on going back: a database carried above it can be read by this
// build and by nothing that came before it.
const v1FormatNewest = 16

// TestPebbleStaysAtTheVersionV1CanRead covers the FormatMajorVersion that
// NewPebbleDB deliberately does not set. Naming FormatNewest there is a
// one-line change that reads as an improvement, and it would take every
// database this build opens to a version the release before it cannot, with
// no error and no way back. Nothing else here would notice.
func TestPebbleStaysAtTheVersionV1CanRead(t *testing.T) {
	dir := t.TempDir()

	db, err := NewPebbleDB("blockstore", dir)
	require.NoError(t, err)
	vers := db.DB().FormatMajorVersion()
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
	require.NoError(t, db.Close())

	require.Equal(t, pebble.FormatMinSupported, vers,
		"a new database should sit at the lowest version this build supports")
	require.LessOrEqual(t, uint64(vers), uint64(v1FormatNewest),
		"this is above what pebble v1 opens, so a rollback would be refused")

	again, err := NewPebbleDB("blockstore", dir)
	require.NoError(t, err)
	defer again.Close()
	require.Equal(t, vers, again.DB().FormatMajorVersion(),
		"reopening moved the format, so the way back closes on its own")
}

func TestPebbleDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*PebbleDB)
	assert.True(t, ok)
}

func BenchmarkPebbleDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = db.Close()
		require.NoError(b, err)

		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}

func TestPebbleDBNewPebbleDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := NewPebbleDB(name, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, wr1.Close())
	})
	_, err = NewPebbleDB(name, "")
	require.Error(t, err, "should not be able to open db twice")
}
