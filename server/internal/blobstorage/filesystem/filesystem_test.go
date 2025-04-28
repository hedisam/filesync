package filesystem_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/server/internal/blobstorage/filesystem"
)

// errorReader always returns an error on Read.
type errorReader struct {
	err error
}

func (e errorReader) Read(p []byte) (int, error) {
	return 0, e.err
}

// I haven't used table testing here because each case can have its own custom setup and putting them
// into one table would hide what is really going on
func TestPutObject(t *testing.T) {
	logger := logrus.New()

	t.Run("happy path", func(t *testing.T) {
		tmpDir := t.TempDir()
		fs, err := filesystem.New(logger, tmpDir)
		require.NoError(t, err)

		ctx := context.Background()
		data := []byte("hello world")
		objectID := uuid.NewString()

		checksum, written, err := fs.PutObject(ctx, bytes.NewReader(data), objectID)
		require.NoError(t, err)
		assert.EqualValues(t, len(data), written)

		sum := sha256.Sum256(data)
		assert.Equal(t, hex.EncodeToString(sum[:]), checksum)

		fullPath := filepath.Join(tmpDir, objectID)
		content, err := os.ReadFile(fullPath)
		require.NoError(t, err)
		assert.Equal(t, data, content)
	})

	t.Run("read error from reader", func(t *testing.T) {
		tmpDir := t.TempDir()
		fs, err := filesystem.New(logger, tmpDir)
		require.NoError(t, err)

		r := errorReader{err: errors.New("read error")}
		_, _, err = fs.PutObject(context.Background(), r, uuid.NewString())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write to object file: read error")
	})

	t.Run("create failure due to perms", func(t *testing.T) {
		roDir := t.TempDir()
		// remove write perms
		err := os.Chmod(roDir, 0500)
		require.NoError(t, err)

		fs, err := filesystem.New(logger, roDir)
		require.NoError(t, err)

		_, _, err = fs.PutObject(context.Background(), bytes.NewReader([]byte("nope")), uuid.NewString())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create object file")
	})
}

// I haven't used table testing here because each case can have its own custom setup and putting them
// into one table would hide what is really going on
func TestDeleteObject(t *testing.T) {
	logger := logrus.New()

	t.Run("happy path", func(t *testing.T) {
		tmpDir := t.TempDir()
		fs, err := filesystem.New(logger, tmpDir)
		require.NoError(t, err)

		// seed a file to delete
		objectID := uuid.NewString()
		fullPath := filepath.Join(tmpDir, objectID)
		err = os.WriteFile(fullPath, []byte("data"), 0644)
		require.NoError(t, err)

		err = fs.DeleteObject(context.Background(), objectID)
		require.NoError(t, err)

		_, err = os.Stat(fullPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("no errors when file does not exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		fs, err := filesystem.New(logger, tmpDir)
		require.NoError(t, err)

		err = fs.DeleteObject(context.Background(), "missing.txt")
		require.NoError(t, err)
	})
}
