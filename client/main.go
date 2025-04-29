package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/filesystem"
	"github.com/hedisam/filesync/client/filesystem/watch"
	"github.com/hedisam/filesync/client/indexer"
	"github.com/hedisam/filesync/client/plan"
	"github.com/hedisam/filesync/lib/wal"
)

type Options struct {
	SourceDir      string
	ServerAddr     string
	AccessKeyID    string
	SecretKey      string
	UploadEndpoint string
	Quite          bool
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	var opts Options
	flag.StringVar(&opts.SourceDir, "src-dir", ".", "Source directory to sync its content with the server.")
	flag.StringVar(&opts.AccessKeyID, "aki", "", "Your access key ID as printed by the server (required).")
	flag.StringVar(&opts.SecretKey, "secret", "", "Your secret key as printed by the server (required).")
	flag.StringVar(&opts.ServerAddr, "server-addr", "http://localhost:8080", "FileServer address to connect to.")
	flag.BoolVar(&opts.Quite, "quite", false, "Quite output")
	flag.StringVar(&opts.UploadEndpoint, "upload-endpoint", "/v1/files/upload", "Upload endpoint used for generating presigned upload URLs")
	flag.Parse()

	if opts.Quite {
		logger.SetLevel(logrus.InfoLevel)
	}

	if opts.SourceDir == "" || opts.AccessKeyID == "" || opts.SecretKey == "" {
		flag.Usage()
		os.Exit(1)
	}

	baseUploadURL, err := url.JoinPath(opts.ServerAddr, opts.UploadEndpoint)
	if err != nil {
		logger.WithError(err).Fatal("Invalid server address or upload endpoint")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	w := mustCreateWal(logger)
	defer w.Close()

	watcher, err := watch.New(logger, w)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize watcher")
	}
	go watcher.Start(ctx)
	defer watcher.Close()

	idx := indexer.New(logger, 4)
	go func() {
		err := idx.Run(ctx)
		if err != nil {
			logger.WithError(err).Error("Indexer pipeline failed with an error")
			cancel()
		}
	}()

	// create the baseline index by walking through the source dir recursively.
	err = filesystem.Walk(ctx, logger, opts.SourceDir, watcher, idx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to walk source directory")
	}

	err = indexFirstStageFileChanges(ctx, logger, w, idx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to index first stage change files")
		cancel()
		return
	}

	httpClient := restapi.NewClient(logger, opts.ServerAddr)
	p, err := plan.Generate(ctx, httpClient, idx)
	if err != nil {
		logger.WithError(err).Error("Failed to generate the initial plan")
		cancel()
		return
	}

	err = plan.Apply(ctx, logger, baseUploadURL, httpClient, p, 10, plan.Credentials{
		AccessKeyID: opts.AccessKeyID,
		SecretKey:   opts.SecretKey,
	})
	if err != nil {
		logger.WithError(err).Error("Failed to apply the plan")
		cancel()
		return
	}

	// todo: resume consuming the filesystem change events' WAL and update the index accordingly
	// todo: add a debounce layer between the WAL consumer and the indexer to filter out noise
	// todo: continuously generate and apply plan to sync up with the server

	<-ctx.Done()
}

func indexFirstStageFileChanges(ctx context.Context, logger *logrus.Logger, w *wal.WAL, idx *indexer.Indexer) error {
	// read the wal fs events with a stop function that tells the consumer to stop as soon as we reach an event
	// with the initial stage number which is zero.
	// basically, we are reading all the filesystem changes that have happened while we were still walking through the
	// source directory. right now, we're not interested in any file changes after that.
	// this is to make sure we're not uploading files that we've seen during the walk and got deleted right away after
	// we indexed them
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	fileChangeEvents := w.Consume(ctx, wal.WithStopFunc(func(entry *wal.Entry) (stop bool) {
		return entry.StageNum > 0
	}))
	for event := range fileChangeEvents {
		if event.ErroredBytes != nil {
			logger.WithError(errors.New(event.Error)).WithField(
				"errored_bytes", fmt.Sprintf("%q", event.ErroredBytes),
			).Error("Error occurred while consuming file changes")
			return fmt.Errorf("invalid wal entry: %q", event.Error)
		}

		var msg watch.Event
		err := json.Unmarshal(event.Message, &msg)
		if err != nil {
			logger.WithError(err).WithField(
				"message", fmt.Sprintf("%q", event.Message),
			).Error("Failed to unmarshal event while consuming file changes for adding to indexer")
			return fmt.Errorf("invalid wal message: %w", err)
		}

		err = idx.Index(ctx, &indexer.FileMetadata{
			Path:      msg.Path,
			Operation: msg.Operation,
			EventTime: event.Timestamp,
		})
		if err != nil {
			logger.WithError(err).WithField("path", msg.Path).Error("Failed to index file while consuming file changes")
			return fmt.Errorf("could not index file change op: %w", err)
		}
	}

	// wait for the indexer to finish processing in flight items
	idx.WaitOnInFlights()
	fmt.Println("Successfully indexed file changes")

	return nil
}

func mustCreateWal(logger *logrus.Logger) *wal.WAL {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "filesync")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory")
	}
	w, err := wal.New(logger, filepath.Join(tmpDir, "wal.log"))
	if err != nil {
		logger.WithError(err).Fatal("Failed to create append-only log for the filesystem watcher")
	}

	return w
}
