package filesystem_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/client/filesystem"
	"github.com/hedisam/filesync/client/filesystem/mocks"
	"github.com/hedisam/filesync/client/indexer"
)

//go:generate moq -out mocks/file_indexer.go -pkg mocks -skip-ensure . FileIndexer
//go:generate moq -out mocks/watcher.go -pkg mocks -skip-ensure . Watcher

func TestWalk(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		existingDirs         []string
		existingFiles        []string
		existingSymlinks     map[string]string
		expectedWatchedDirs  []string
		expectedIndexedFiles []string
	}{
		"empty": {
			existingDirs:         []string{},
			existingFiles:        []string{},
			existingSymlinks:     nil,
			expectedWatchedDirs:  []string{"."},
			expectedIndexedFiles: []string{},
		},
		"single file": {
			existingDirs:         []string{},
			existingFiles:        []string{"file1.txt"},
			existingSymlinks:     nil,
			expectedWatchedDirs:  []string{"."},
			expectedIndexedFiles: []string{"file1.txt"},
		},
		"nested structure": {
			existingDirs:         []string{"a", "a/b"},
			existingFiles:        []string{"a/b/c.txt"},
			existingSymlinks:     nil,
			expectedWatchedDirs:  []string{".", "a", "a/b"},
			expectedIndexedFiles: []string{"a/b/c.txt"},
		},
		"skip hidden and temp": {
			existingDirs:         []string{},
			existingFiles:        []string{".hidden", "temp~", "visible.txt"},
			existingSymlinks:     nil,
			expectedWatchedDirs:  []string{"."},
			expectedIndexedFiles: []string{"visible.txt"},
		},
		"skip symlinks": {
			existingDirs:         []string{},
			existingFiles:        []string{"real.txt"},
			existingSymlinks:     map[string]string{"link": "real.txt"},
			expectedWatchedDirs:  []string{"."},
			expectedIndexedFiles: []string{"real.txt"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, err := os.MkdirTemp("", "walker_test")
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = os.RemoveAll(root)
			})

			for _, d := range tc.existingDirs {
				path := filepath.Join(root, d)
				err := os.MkdirAll(path, 0755)
				require.NoError(t, err)
			}

			for _, f := range tc.existingFiles {
				path := filepath.Join(root, f)
				err := os.WriteFile(path, []byte("data"), 0644)
				require.NoError(t, err)
			}

			for link, target := range tc.existingSymlinks {
				linkPath := filepath.Join(root, link)
				targetPath := filepath.Join(root, target)
				err := os.Symlink(targetPath, linkPath)
				require.NoError(t, err)
			}

			watcherMock := &mocks.WatcherMock{
				AddFunc: func(dirPath string) error {
					rel, err := filepath.Rel(root, dirPath)
					require.NoError(t, err)
					assert.Contains(t, tc.expectedWatchedDirs, rel)
					return nil
				},
				IncStageNumFunc: func() {},
			}

			indexerMock := &mocks.FileIndexerMock{
				IndexFunc: func(_ context.Context, md *indexer.FileMetadata) error {
					rel, err := filepath.Rel(root, md.Path)
					require.NoError(t, err)
					assert.Contains(t, tc.expectedIndexedFiles, rel)
					return nil
				},
			}
			logger := logrus.New()
			err = filesystem.Walk(context.Background(), logger, root, watcherMock, indexerMock)
			require.NoError(t, err)

			assert.Equal(t, len(tc.expectedWatchedDirs), len(watcherMock.AddCalls()))
			assert.Equal(t, len(tc.expectedIndexedFiles), len(indexerMock.IndexCalls()))
			assert.Equal(t, 1, len(watcherMock.IncStageNumCalls()))
		})
	}
}
