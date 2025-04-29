package filesystem

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/indexer"
	"github.com/hedisam/filesync/client/ops"
)

type Watcher interface {
	Add(dirPath string) error
	IncStageNum()
}

type FileIndexer interface {
	Index(ctx context.Context, fileMetadata *indexer.FileMetadata) error
}

// Walk walks through the given directory recursively performing the following actions:
//  1. Add every non-hidden directory to the filesystem watcher
//  2. Add every non-hidden regular file to the indexer.
//  3. Increment the watcher's stage by one so we know fs change events with the next stage number have not happened
//
// during this recursive Walk.
func Walk(ctx context.Context, log *logrus.Logger, rootDir string, watcher Watcher, idx FileIndexer) error {
	logger := log.WithField("root_dir", rootDir)
	logger.Info("Walking directory")

	defer watcher.IncStageNum()

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

		err = idx.Index(ctx, &indexer.FileMetadata{
			Path:      path,
			Operation: ops.OpCreated,
			EventTime: time.Time{}, // this is the baseline compared to file change events. its timestamp must be the smallest.
		})
		if err != nil {
			logger.WithField("path", path).WithError(err).Error("Error adding file to index")
			return fmt.Errorf("adding file to index: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
