package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/filesystem"
	"github.com/hedisam/filesync/client/filesystem/watch"
	"github.com/hedisam/filesync/client/index"
	"github.com/hedisam/filesync/client/plan"
	"github.com/hedisam/filesync/client/syncpipeline"
	"github.com/hedisam/filesync/lib/wal"
	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/chans"
	"github.com/hedisam/pipeline/stage"
)

type Options struct {
	SourceDir    string
	ServerAddr   string
	AccessKeyID  string
	SecretKey    string
	SyncInterval time.Duration
	Verbose      bool
}

func main() {
	logger := logrus.New()

	var opts Options
	flag.StringVar(&opts.SourceDir, "src-dir", ".", "Source directory to sync its content with the server.")
	flag.StringVar(&opts.AccessKeyID, "aki", "", "Your access key ID as printed by the server (required).")
	flag.StringVar(&opts.SecretKey, "secret", "", "Your secret key as printed by the server (required).")
	flag.StringVar(&opts.ServerAddr, "server-addr", "http://localhost:8080", "FileServer address to connect to.")
	flag.DurationVar(&opts.SyncInterval, "sync-interval", time.Second*10, "How often to sync up with the server")
	flag.BoolVar(&opts.Verbose, "v", false, "Verbose output")
	flag.Parse()

	if opts.Verbose {
		logger.SetLevel(logrus.DebugLevel)
	}

	if opts.SourceDir == "" || opts.AccessKeyID == "" || opts.SecretKey == "" {
		flag.Usage()
		os.Exit(1)
	}

	restClient, err := restapi.NewClient(logger, opts.ServerAddr)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create rest client")
	}

	tmpDir, err := os.MkdirTemp(os.TempDir(), "filesync")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory")
	}

	var errorChans []<-chan error

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	watchWAL := mustCreateWal(logger, filepath.Join(tmpDir, "watch.log"))
	defer watchWAL.Close()
	watcher, err := watch.New(logger, watchWAL)
	if err != nil {
		logger.WithError(err).Error("Failed to initialize file watcher")
		return
	}
	watchErrCh := watcher.Start(ctx)
	defer watcher.Stop()
	errorChans = append(errorChans, watchErrCh)

	// create the baseline index by walking through the source dir recursively.
	walkWAL := mustCreateWal(logger, filepath.Join(tmpDir, "watcher.log"))
	defer walkWAL.Close()
	walkErrCh := filesystem.Walk(ctx, logger, opts.SourceDir, watcher, walkWAL)
	walkErrCh1, walkErrCh2 := chans.Tee2(ctx, walkErrCh)
	errorChans = append(errorChans, walkErrCh1)
	chans.OnDone(ctx, walkErrCh2, func(_ context.Context) {
		// close the walker WAL so we can start consuming the file changes' WAL
		logger.Debug("Walker background job is done, closing its WAL stream")
		walkWAL.Close()
	})

	idx := index.New(logger, index.DefaultIndexSize)

	// a pipeline with multiple sequential sources; first consume the walker WAL and then the file watcher's
	filesPipeline := pipeline.NewPipeline(
		walkWAL, idx.IndexerSink(),
		pipeline.WithSequentialSourcing(),
		pipeline.WithSources(watchWAL),
	)
	pipeErrCh := filesPipeline.RunAsync(ctx,
		stage.FIFORunner(idx.UnmarshalWALDataProcessor()),
		stage.WorkerPoolRunner(
			uint(runtime.NumCPU()),
			idx.MetadataExtractorProcessor(),
		),
	)
	errorChans = append(errorChans, pipeErrCh)

	// todo: add a debounce layer between the WAL consumer and the indexer to filter out noise

	planner := plan.NewPlanner(logger)
	syncClient := syncpipeline.New(logger, restClient, planner, opts.AccessKeyID, opts.SecretKey)
	snapshotSource := syncpipeline.NewSnapshotSource(ctx, restClient, idx, opts.SyncInterval)
	sp := pipeline.NewPipeline(snapshotSource, syncClient.OutputSink())
	spErrCh := sp.RunAsync(ctx,
		stage.FIFORunner(syncClient.PlanGenerator()),
		stage.SplitterRunner(syncClient.PlanSplitter()),
		stage.WorkerPoolRunner(
			uint(runtime.NumCPU()),
			syncClient.PlanRequestApplier(),
		),
	)
	errorChans = append(errorChans, spErrCh)

	asyncErr := <-chans.FanIn(ctx, errorChans...)
	if asyncErr != nil {
		logger.WithError(asyncErr).Error("Received async error, shutting down...")
		return
	}
}

func mustCreateWal(logger *logrus.Logger, path string) *wal.WAL {
	w, err := wal.New(logger, path)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create append-only log for the filesystem watcher")
	}

	return w
}
