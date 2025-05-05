package rest_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/hedisam/filesync/server/api/rest"
	"github.com/hedisam/filesync/server/api/rest/mocks"
	"github.com/hedisam/filesync/server/internal/store"
)

func TestUploadFile(t *testing.T) {
	tests := map[string]struct {
		url            string
		authKeyID      string
		authSecret     string
		authOK         bool
		wantStatus     int
		wantBodySubstr string
	}{
		"missing key": {
			url:            "http://localhost/upload",
			authKeyID:      "",
			authSecret:     "",
			authOK:         false,
			wantStatus:     http.StatusUnauthorized,
			wantBodySubstr: "invalid access key id",
		},
		"invalid key": {
			url:            "http://localhost/upload?aki=foo",
			authKeyID:      "foo",
			authSecret:     "",
			authOK:         false,
			wantStatus:     http.StatusUnauthorized,
			wantBodySubstr: "invalid access key id",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			authMock := &mocks.AuthMock{
				GetSecretKeyByIDFunc: func(keyID string) (string, bool) {
					assert.Equal(t, tc.authKeyID, keyID)
					return tc.authSecret, tc.authOK
				},
			}
			fsMock := &mocks.FileStorageMock{
				PutObjectFunc: func(ctx context.Context, r io.Reader, objectID string) (string, int64, error) {
					return "", 0, nil
				},
			}
			mdMock := &mocks.UploadMetadataStoreMock{
				CreateFunc: func(ctx context.Context, md *store.ObjectMetadata) error {
					return nil
				},
				PutObjectCompletedFunc: func(ctx context.Context, key, objectID string) error {
					return nil
				},
			}

			srv := rest.NewUploadServer(logrus.New(), fsMock, mdMock, authMock)
			req := httptest.NewRequest("PUT", tc.url, nil)
			rr := httptest.NewRecorder()
			srv.UploadFile(rr, req)

			assert.Equal(t, tc.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tc.wantBodySubstr)
		})
	}
}
