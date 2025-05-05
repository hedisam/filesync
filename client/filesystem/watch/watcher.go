package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/ops"
	"github.com/hedisam/filesync/lib/wal"
)

type Watcher struct {
	logger   *logrus.Logger
	watcher  *fsnotify.Watcher
	wal      *wal.WAL
	stageNum atomic.Int64
}

func New(logger *logrus.Logger, w *wal.WAL) (*Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		logger:  logger,
		watcher: watcher,
		wal:     w,
	}, nil
}

func (w *Watcher) Add(dirPath string) error {
	err := w.watcher.Add(dirPath)
	if err != nil {
		return fmt.Errorf("add dir to watcher: %w", err)
	}

	w.logger.WithField("dir", dirPath).Debug("Watching directory...")
	return nil
}

func (w *Watcher) Start(ctx context.Context) <-chan error {
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}

				if strings.HasSuffix(event.Name, "~") || strings.HasPrefix(event.Name, ".") {
					// ignore hidden and temporary files (e.g. file.txt~ that are automatically created by editors)
					continue
				}

				if event.Has(fsnotify.Create) {
					info, err := os.Stat(event.Name)
					if err != nil {
						w.logger.WithField("path", event.Name).WithError(err).Warn("Failed to get stat info processing fs watcher event, ignoring")
						continue
					}
					if info.IsDir() {
						err = w.Add(event.Name)
						if err != nil {
							w.logger.WithError(err).Warn("Failed to add newly created directory to watcher, ignoring")
						}
						continue
					}
				}

				stageNum := w.stageNum.Load()
				events, err := w.marshalEvent(event, stageNum)
				if err != nil {
					// todo: in a production system we may wanna know what is this error by having a metric or something
					// behaviour depends on product requirements.
					w.logger.WithError(err).Error("Failed to marshal watch event")
					errChan <- fmt.Errorf("marshal watch event: %w", err)
					continue
				}

				for eventBytes := range slices.Values(events) {
					err = w.wal.Append(eventBytes)
					if err != nil {
						w.logger.WithError(err).Error("Failed to append watch event to WAL")
						errChan <- fmt.Errorf("append watch event to WAL: %w", err)
					}
				}

			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}
				// todo: in a production system we may wanna know what is this error by having a metric or something
				w.logger.WithError(err).Error("Received error from watcher")
				errChan <- fmt.Errorf("watcher: %w", err)
			}
		}
	}()
	return errChan
}

func (w *Watcher) IncStageNum() {
	w.stageNum.Add(1)
}

func (w *Watcher) Stop() {
	_ = w.watcher.Close()
}

func (w *Watcher) marshalEvent(event fsnotify.Event, stageNum int64) ([][]byte, error) {
	var fileOps []ops.Op

	// event.Op is a bitmask and some systems may send multiple operations at once.
	if event.Has(fsnotify.Create) {
		w.logger.WithField("file", event.Name).Debug("File created")
		fileOps = append(fileOps, ops.OpCreated)
	}
	if event.Has(fsnotify.Remove) {
		w.logger.WithField("file", event.Name).Debug("File removed")
		fileOps = append(fileOps, ops.OpRemoved)
	}

	if event.Has(fsnotify.Rename) {
		// this is like a removal; on a rename, the file with the old name is kinda removed you could say.
		w.logger.WithField("file", event.Name).Debug("File renamed")
		fileOps = append(fileOps, ops.OpRemoved)
	}
	if event.Op&fsnotify.Write == fsnotify.Write {
		w.logger.WithField("file", event.Name).Debug("File modified")
		fileOps = append(fileOps, ops.OpModified)
	}

	var result [][]byte
	for op := range slices.Values(fileOps) {
		b, err := json.Marshal(&ops.FileOp{
			Path:      event.Name,
			Op:        op,
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return nil, fmt.Errorf("marshal event: %w", err)
		}
		result = append(result, b)
	}

	return result, nil
}
