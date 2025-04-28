package filesystem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

type FileSystem struct {
	logger *logrus.Logger
	dir    *os.Root
}

func New(logger *logrus.Logger, rootDir string) (*FileSystem, error) {
	logger.WithField("root_dir", rootDir).Info("Getting directory-limited filesystem access")

	dir, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, fmt.Errorf("open root dir: %w", err)
	}

	return &FileSystem{
		logger: logger,
		dir:    dir,
	}, nil
}

// PutObject reads data from the provided io.Reader and stores it under the given objectID as name. It calculates
// the data checksum along the way as well and returns it along with the number of bytes written.
func (fs *FileSystem) PutObject(ctx context.Context, r io.Reader, objectID string) (checksum string, written int64, err error) {
	logger := fs.logger.WithContext(ctx).WithField("object_id", objectID)

	f, err := fs.dir.Create(objectID)
	if err != nil {
		logger.WithError(err).Error("Could not create when putting object in filesystem")
		return "", 0, fmt.Errorf("create object file: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	mw := io.MultiWriter(f, hasher)

	written, err = io.Copy(mw, r)
	if err != nil {
		logger.WithError(err).Error("Could not write to file when putting object in filesystem")
		return "", 0, fmt.Errorf("write to object file: %w", err)
	}

	checksum = hex.EncodeToString(hasher.Sum(nil))

	return checksum, written, nil
}

func (fs *FileSystem) DeleteObject(ctx context.Context, objectID string) error {
	logger := fs.logger.WithContext(ctx).WithField("object_id", objectID)

	err := fs.dir.Remove(objectID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		logger.WithError(err).Error("Could not remove file from filesystem")
		return fmt.Errorf("remove object file: %w", err)
	}

	return nil
}
