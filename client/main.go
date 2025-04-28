package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hedisam/filesync/client/filesystem"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/filesystem/watch"
)

type Options struct {
	SourceDir    string
	ServerAddr   string
	AccessKeyID  string
	SecretKey    string
	WalkInterval string
	Quite        bool
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	var opts Options
	flag.StringVar(&opts.SourceDir, "src-dir", "", "Source directory to sync its content with the server(required)")
	flag.StringVar(&opts.AccessKeyID, "aki", "", "Your access key ID as printed by the server. (required)")
	flag.StringVar(&opts.SecretKey, "secret", "", "Your secret key as printed by the server. (required)")
	flag.StringVar(&opts.ServerAddr, "server-addr", "localhost:8080", "FileServer address to connect to")
	flag.StringVar(&opts.WalkInterval, "walk-interval", "1h", "Interval duration in go format to determine how often we should do a full filesystem scan")
	flag.BoolVar(&opts.Quite, "quite", false, "Quite output")
	flag.Parse()

	if opts.Quite {
		logger.SetLevel(logrus.InfoLevel)
	}

	if opts.SourceDir == "" || opts.AccessKeyID == "" || opts.SecretKey == "" {
		flag.Usage()
		os.Exit(1)
	}

	var walkInterval time.Duration
	if opts.WalkInterval != "" {
		wi, err := time.ParseDuration(opts.WalkInterval)
		if err != nil {
			logger.WithError(err).WithField("value", opts.WalkInterval).Fatal("Failed to parse --walk-interval flag")
		}
		walkInterval = wi
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	watcher, err := watch.New(logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize watcher")
	}
	go watcher.Start(ctx)
	defer watcher.Close()

	walker := filesystem.NewWalker(logger, watcher)
	go walker.Start(ctx, opts.SourceDir, walkInterval)

	<-ctx.Done()
}
