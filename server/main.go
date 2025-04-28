package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	asyncapi "github.com/hedisam/filesync/server/api/async"
	restapi "github.com/hedisam/filesync/server/api/rest"
	"github.com/hedisam/filesync/server/internal/auth"
	"github.com/hedisam/filesync/server/internal/blobstorage/filesystem"
	"github.com/hedisam/filesync/server/internal/emitter"
	"github.com/hedisam/filesync/server/internal/interceptors"
	"github.com/hedisam/filesync/server/internal/store/memdb"
)

const (
	appName = "filesync-server"
)

// Options defines a set of config options.
type Options struct {
	DestinationDir string
	ServerAddr     string
	Quite          bool
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.AddHook(&interceptors.TraceHook{})

	var opts Options
	flag.StringVar(&opts.DestinationDir, "dest-dir", "", "Destination directory to store file objects (required)")
	flag.StringVar(&opts.ServerAddr, "server-addr", "localhost:8080", "FileServer address to listen on")
	flag.BoolVar(&opts.Quite, "quite", false, "Quite output")
	flag.Parse()

	if opts.DestinationDir == "" {
		flag.Usage()
		os.Exit(1)
	}
	if opts.Quite {
		logger.SetLevel(logrus.InfoLevel)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	authService := auth.New()
	generateAndPrintAccessKey(authService)

	e := emitter.New()
	defer e.Close()

	mdStore := memdb.NewMetadataStore(e)
	fileServer := restapi.NewFilesServer(logger, mdStore)

	fileStorage, err := filesystem.New(logger, opts.DestinationDir)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize filesystem")
	}
	uploadServer := restapi.NewUploadServer(logger, fileStorage, mdStore, authService)

	janitor := asyncapi.NewJanitor(logger, fileStorage)
	go janitor.Run(ctx, e.Chan())

	mux := http.NewServeMux()
	restapi.RegisterFunc(logger, mux, http.MethodDelete, "/v1/files/{key}", fileServer.DeleteFile)
	mux.HandleFunc("PUT /v1/files/upload", uploadServer.UploadFile)

	shutdown := mustInitTracer(logger, appName)
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*3)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			logger.WithError(err).Error("Failed to shutdown tracer")
		}
	}()
	handler := otelhttp.NewHandler(mux, appName)
	handler = interceptors.InterceptWithDefaultMetrics(handler)

	// Expose the registered metrics via HTTP
	mux.Handle("/metrics", promhttp.Handler())

	logger.WithField("addr", opts.ServerAddr).Info("Starting server")
	err = http.ListenAndServe(opts.ServerAddr, handler)
	if err != nil {
		logger.WithError(err).Fatal("Server failed with error")
	}
}

func generateAndPrintAccessKey(authService *auth.Auth) {
	accessKey := authService.GenerateAccessKey()
	fmt.Println("[!] Use the following access key with your client:")
	fmt.Printf("  Access Key ID:     %s\n", accessKey.AccessKeyID)
	fmt.Printf("  Access Key Secret: %s\n", accessKey.SecretKey)
}

func mustInitTracer(logger *logrus.Logger, appName string) func(context.Context) error {
	exp, err := interceptors.NewSTDOUTExporter(true)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize STDOUT trace exporter")
	}

	tp, err := interceptors.RegisterTraceProvider(appName, exp)
	if err != nil {
		logger.WithError(err).Fatal("Failed to register trace provider")
	}

	return tp.Shutdown
}
