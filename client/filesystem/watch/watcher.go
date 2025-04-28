package watch

import (
	"context"
	"fmt"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

type Watcher struct {
	logger  *logrus.Logger
	watcher *fsnotify.Watcher
}

func New(logger *logrus.Logger) (*Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		logger:  logger,
		watcher: watcher,
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

func (w *Watcher) Start(ctx context.Context) {
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

			// event.Op is a bitmask and some systems may send multiple operations at once.
			if event.Op&fsnotify.Create == fsnotify.Create {
				w.logger.WithField("file", event.Name).Debug("File created")
			}
			if event.Op&fsnotify.Remove == fsnotify.Remove {
				w.logger.WithField("file", event.Name).Debug("File removed")
			}
			if event.Op&fsnotify.Rename == fsnotify.Rename {
				w.logger.WithField("file", event.Name).Debug("File renamed")
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				w.logger.WithField("file", event.Name).Debug("File modified")
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.logger.WithError(err).Error("Received error from watcher")
		}
	}
}

func (w *Watcher) Close() {
	_ = w.watcher.Close()
}
