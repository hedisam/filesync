package interceptors

import (
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type TraceHook struct{}

// Levels returns the levels we want this TraceHook to be fired on.
func (h *TraceHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire populates the logger with trace_id retrieved from the context.
// API endpoints should use WithContext(ctx) for the trace id to be logged.
func (h *TraceHook) Fire(entry *logrus.Entry) error {
	if entry.Context == nil {
		return nil
	}

	span := trace.SpanFromContext(entry.Context)
	sc := span.SpanContext()
	if sc.IsValid() {
		entry.Data["trace_id"] = sc.TraceID().String()
	}

	return nil
}
