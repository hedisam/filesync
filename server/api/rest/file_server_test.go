package rest_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	restapi "github.com/hedisam/filesync/server/api/rest"
	"github.com/hedisam/filesync/server/api/rest/mocks"
	"github.com/hedisam/filesync/server/internal/store"
)

//go:generate moq -out mocks/file_metadata_store.go -pkg mocks -skip-ensure . FileMetadataStore

func TestDeleteFile(t *testing.T) {
	tests := map[string]struct {
		req *restapi.DeleteFileRequest

		existingFiles  map[string]*store.ObjectMetadata
		storeDeleteErr error

		expectedStoreCalls int
		expectedResp       *restapi.DeleteFileResponse
		expectedErr        *restapi.Err
	}{
		"simple success case": {
			req: &restapi.DeleteFileRequest{
				Key: "/data/file.csv",
			},
			existingFiles: map[string]*store.ObjectMetadata{
				"/data/file.csv":         {},
				"/data/another_file.csv": {},
			},
			expectedStoreCalls: 1,
			expectedResp:       &restapi.DeleteFileResponse{},
		},
		"success case - file does not exist": {
			req: &restapi.DeleteFileRequest{
				Key: "/data/file.csv",
			},
			existingFiles: map[string]*store.ObjectMetadata{
				"/data/file2.csv": {},
			},
			expectedStoreCalls: 1,
			expectedResp:       &restapi.DeleteFileResponse{},
		},
		"key is missing": {
			req: &restapi.DeleteFileRequest{},
			expectedErr: &restapi.Err{
				Message: "invalid request body: 'key' is required",
				Status:  http.StatusBadRequest,
			},
		},
		"store call fails": {
			req: &restapi.DeleteFileRequest{
				Key: "/data/file.csv",
			},
			existingFiles: map[string]*store.ObjectMetadata{
				"/data/file.csv": {},
			},
			expectedStoreCalls: 1,
			storeDeleteErr:     errors.New("dummy error"),
			expectedErr: &restapi.Err{
				Message: "could not delete file metadata: dummy error",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mdStore := &mocks.FileMetadataStoreMock{
				DeleteFunc: func(ctx context.Context, key string) error {
					assert.Equal(t, test.req.Key, key)
					if test.storeDeleteErr != nil {
						return test.storeDeleteErr
					}
					return nil
				},
			}

			s := restapi.NewFilesServer(logrus.New(), mdStore)

			resp, err := s.DeleteFile(context.Background(), test.req)
			assert.Equal(t, test.expectedStoreCalls, len(mdStore.DeleteCalls()))
			if test.expectedErr != nil {
				require.Error(t, err)
				castedErr := &restapi.Err{}
				if errors.As(err, &castedErr) {
					assert.Equal(t, test.expectedErr, castedErr)
					return
				}
				assert.Equal(t, test.expectedErr.Message, err.Error())
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResp, resp)
		})
	}
}
