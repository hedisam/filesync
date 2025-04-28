package interceptors

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func NewSTDOUTExporter(useIODiscard bool) (*stdouttrace.Exporter, error) {
	var w io.Writer = os.Stdout
	if useIODiscard {
		w = io.Discard
	}
	exp, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(w),
	)
	if err != nil {
		return nil, fmt.Errorf("create stdout exporter: %w", err)
	}

	return exp, nil
}

func RegisterTraceProvider(appName string, exp sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(appName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource merger: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // todo: AlwaysSample is not suitable for a production environment with significant traffic
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)

	otel.SetTracerProvider(tp)

	return tp, nil
}

func initTracer(appName string) func(context.Context) error {
	exp, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(io.Discard),
	)
	if err != nil {
		log.Fatalf("failed to create stdout exporter: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(appName),
		)),
	)

	otel.SetTracerProvider(tp)

	// Return a shutdown func to flush when your app exits
	return tp.Shutdown
}

// TracingMiddleware wraps any http.Handler, starting a span for each request.
func TracingMiddleware(appName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer(appName).Start(r.Context(), "")
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
