package interceptors

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func InterceptWithDefaultMetrics(handler http.Handler) http.Handler {
	// Initialize Prometheus metrics
	inFlightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "filesync_http_in_flight_requests",
		Help: "Current number of in-flight HTTP requests",
	})
	requestCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "filesync_http_requests_total",
		Help: "Total HTTP requests processed, labeled by status code and method",
	}, []string{"code", "method"})
	requestLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "filesync_http_request_duration_seconds",
		Help: "Histogram of HTTP request durations in seconds",
	}, []string{"method"})

	prometheus.MustRegister(inFlightGauge, requestCount, requestLatency)

	return promhttp.InstrumentHandlerInFlight(inFlightGauge,
		promhttp.InstrumentHandlerDuration(requestLatency,
			promhttp.InstrumentHandlerCounter(requestCount, handler),
		),
	)
}
