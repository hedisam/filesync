package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/server/api/rest"
)

type sampleReq struct {
	A string `json:"a"`
	B string `json:"b"`
}

type sampleResp struct {
	Sum string `json:"sum"`
}

func TestFuncAdapter(t *testing.T) {
	tests := map[string]struct {
		handlerResp     *sampleResp
		handlerErr      error
		body            string
		method          string
		url             string
		headers         map[string]string
		expectedStatus  int
		expectedResp    *sampleResp
		expectedRespStr string
	}{
		"Success": {
			handlerResp:    &sampleResp{Sum: "foobar"},
			method:         "POST",
			url:            "http://localhost:8080/endpoint?b=baz",
			body:           `{"a":"foo","b":"bar"}`,
			headers:        map[string]string{"Content-Type": "application/json", "Bla-Key": "bla-value"},
			expectedStatus: http.StatusOK,
			expectedResp:   &sampleResp{Sum: "foobar"},
		},
		"StatusError": {
			handlerErr:      rest.NewErrf(http.StatusUnprocessableEntity, "invalid request"),
			method:          "GET",
			url:             "http://example.com/foo",
			expectedStatus:  http.StatusUnprocessableEntity,
			expectedRespStr: "invalid request",
		},
		"GenericError": {
			handlerErr:      errors.New("oops"),
			method:          "GET",
			url:             "http://example.com/foo",
			expectedStatus:  http.StatusInternalServerError,
			expectedRespStr: "oops",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := func(ctx context.Context, req *sampleReq) (*sampleResp, error) {
				for k, v := range tc.headers {
					assert.Equal(t, []string{v}, ctx.Value(k))
				}
				return tc.handlerResp, tc.handlerErr
			}
			handler := rest.FuncAdapter(logrus.New(), f)

			var req *http.Request
			if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.url, bytes.NewBufferString(tc.body))
				req.ContentLength = int64(len(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.url, nil)
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			rr := httptest.NewRecorder()
			handler(rr, req)

			assert.Equal(t, tc.expectedStatus, rr.Code)
			if tc.expectedResp != nil {
				var resp *sampleResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Equal(t, tc.expectedResp, resp)
				return
			}
			assert.Equal(t, tc.expectedRespStr, strings.TrimSpace(rr.Body.String()))
		})
	}
}
