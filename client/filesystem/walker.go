package filesystem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/ops"
	"github.com/hedisam/filesync/lib/wal"
	"github.com/hedisam/pipeline/chans"
)

type Watcher interface {
	Add(dirPath string) error
}

// Walk walks through the given directory recursively performing the following actions:
//  1. Add every non-hidden directory to the filesystem watcher
//  2. Add every non-hidden regular file to the indexer.
//  3. Increment the watcher's stage by one so we know fs change events with the next stage number have not happened
//
// during this recursive Walk.
func Walk(ctx context.Context, log *logrus.Logger, rootDir string, watcher Watcher, w *wal.WAL) <-chan error {
	logger := log.WithField("root_dir", rootDir)
	logger.Info("Walking directory")

	errorCh := make(chan error)

	go func() {
		defer close(errorCh)

		err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if strings.HasSuffix(path, "~") {
				// skip temp files created by other apps e.g. editors
				return nil
			}

			if name := filepath.Base(path); name != "." && strings.HasPrefix(name, ".") {
				// skip hidden files or directories
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			if d.IsDir() {
				// the fsnotify module suggests not to add individual files to the watcher
				err = watcher.Add(path)
				if err != nil {
					return fmt.Errorf("add dir to watcher: %w", err)
				}
				return nil
			}

			if !d.Type().IsRegular() {
				// skip irregular files e.g. symlinks
				return nil
			}

			op := &ops.FileOp{
				Path:      path,
				Op:        ops.OpCreated,
				Timestamp: time.Now().UTC(),
			}
			data, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("marshal existing file op: %w", err)
			}
			err = w.Append(data)
			if err != nil {
				return fmt.Errorf("append existing file op to WAL: %w", err)
			}

			logger.WithField("path", path).Debug("File picked up by Walker")

			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			chans.SendOrDone(ctx, errorCh, fmt.Errorf("error occurred while walking directory: %w", err))
		}
	}()

	return errorCh
}
