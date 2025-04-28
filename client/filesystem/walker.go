package filesystem

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/lib/chans"
)

type Watcher interface {
	Add(dirPath string) error
}

type Walker struct {
	logger  *logrus.Logger
	watcher Watcher
}

func NewWalker(logger *logrus.Logger, watcher Watcher) *Walker {
	return &Walker{
		logger:  logger,
		watcher: watcher,
	}
}

func (w *Walker) Start(ctx context.Context, sourceDir string, interval time.Duration) {
	logger := w.logger.WithField("dir", sourceDir)

	run := func() {
		err := w.walk(sourceDir)
		if err != nil {
			logger.WithError(err).Error("Error occurred while walking directory")
		}
	}
	// run for the first time so we don't wait for the ticker when starting the walker
	run()

	if interval == 0 {
		logger.Info("Walk interval is disabled, no background walks will happen")
		return
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for range chans.ReceiveOrDoneSeq(ctx, t.C) {
		run()
	}
}

func (w *Walker) walk(sourceDir string) error {
	logger := w.logger.WithField("dir", sourceDir)
	logger.Info("Walking directory")

	err := filepath.WalkDir(sourceDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			// the fsnotify module suggests not to add individual files to the watcher
			return nil
		}
		err = w.watcher.Add(path)
		if err != nil {
			return fmt.Errorf("add dir to watcher: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
