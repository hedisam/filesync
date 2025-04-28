package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"

	"github.com/sirupsen/logrus"
)

var (
	pathParamRegex = regexp.MustCompile(`{([^}]+)}`)
)

// Err defines an error type that can be enriched with a http status code.
type Err struct {
	Message string
	Status  int
}

// Error implements the std error type.
func (e *Err) Error() string {
	return fmt.Sprintf("Error Code: %d Message: %s", e.Status, e.Message)
}

func NewErrf(status int, msg string, a ...any) *Err {
	return &Err{
		Message: fmt.Sprintf(msg, a...),
		Status:  status,
	}
}

// Func defines a server Func that implements an restful api endpoint.
type Func[Req any, Resp any] func(ctx context.Context, req *Req) (*Resp, error)

type Mux interface {
	HandleFunc(pattern string, f func(w http.ResponseWriter, r *http.Request))
}

func RegisterFunc[Req any, Resp any](logger *logrus.Logger, mux Mux, method, endpoint string, f Func[Req, Resp]) {
	var pathParamKeys []string
	matches := pathParamRegex.FindAllStringSubmatch(endpoint, -1)
	for match := range slices.Values(matches) {
		pathParamKeys = append(pathParamKeys, match[1])
	}
	pattern := fmt.Sprintf("%s %s", method, endpoint)
	mux.HandleFunc(pattern, FuncAdapter(logger, f, pathParamKeys...))
}

// FuncAdapter accepts a server Func and returns a http.HandlerFunc that can be used for API endpoint registration.
// This saves us from explicitly writing http responses or errors each time we need to terminate or return from the
// function. It gives us the ability to simply return a response and error, just like gRPC server methods.
// It also makes unit testing easier as it eliminates the need for a mock http server in every test.
// todo: add custom metrics per each specific handler func (labeled by pattern possibly)
func FuncAdapter[Req any, Resp any](log *logrus.Logger, f Func[Req, Resp], pathParamKeys ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := log.WithFields(logrus.Fields{
			"method":  r.Method,
			"path":    r.URL.Path,
			"pattern": r.Pattern,
			"query":   r.URL.Query(),
		})
		logger.Debug("Handling request in FuncAdapter")

		reqData := make(map[string]any)

		// populate the request body values first, if any.
		if r.Body != nil && r.ContentLength > 0 {
			err := json.NewDecoder(r.Body).Decode(&reqData)
			if err != nil {
				logger.WithError(err).Error("Failed to unmarshal request body in FuncAdapter")
				http.Error(w, fmt.Sprintf("unmarshal request body: %q", err.Error()), http.StatusBadRequest)
				return
			}
		}

		// then populate query param values, replacing request body values if there's any conflict.
		for qParam, val := range r.URL.Query() {
			switch {
			case len(val) == 1:
				reqData[qParam] = val[0]
			case len(val) > 1:
				reqData[qParam] = val
			}
		}

		// final step, populate url path values which can replace existing values populated
		// from query params and req body values
		for param := range slices.Values(pathParamKeys) {
			if val := r.PathValue(param); val != "" {
				reqData[param] = val
			}
		}

		reqBody, err := json.Marshal(reqData)
		if err != nil {
			logger.WithError(err).Error("Failed to marshal merged request data in FuncAdapter")
			http.Error(w, fmt.Sprintf("marshal merged request data: %q", err.Error()), http.StatusInternalServerError)
		}

		var req Req
		err = json.Unmarshal(reqBody, &req)
		if err != nil {
			logger.WithError(err).Error("Failed to unmarshal merged request body in FuncAdapter")
			http.Error(w, fmt.Sprintf("unmarshal merged request body: %q", err.Error()), http.StatusInternalServerError)
			return
		}

		ctx := r.Context()
		for k, v := range r.Header {
			ctx = context.WithValue(ctx, k, v)
		}

		resp, err := f(ctx, &req)
		if err != nil {
			var stErr *Err
			if !errors.As(err, &stErr) {
				stErr = &Err{
					Message: err.Error(),
					Status:  http.StatusInternalServerError,
				}
			}
			http.Error(w, stErr.Message, stErr.Status)
			return
		}

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(resp)
		if err != nil {
			logger.WithError(err).Error("Failed to write response body in FuncAdapter")
		}
	}
}
