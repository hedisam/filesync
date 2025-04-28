package auth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/server/internal/auth"
)

func TestGenerateAccessKey(t *testing.T) {
	tests := map[string]struct {
		count int
	}{
		"one key":       {count: 1},
		"multiple keys": {count: 5},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := auth.New()
			seenIDs := make(map[string]bool)

			for i := 0; i < tc.count; i++ {
				ak := a.GenerateAccessKey()
				require.Len(t, ak.AccessKeyID, 20)
				require.Len(t, ak.SecretKey, 40)

				// ensure secret is retrievable
				secret, ok := a.GetSecretKeyByID(ak.AccessKeyID)
				require.True(t, ok)
				assert.Equal(t, ak.SecretKey, secret)

				// ensure IDs are unique
				assert.False(t, seenIDs[ak.AccessKeyID])
				seenIDs[ak.AccessKeyID] = true
			}
		})
	}
}

func TestGetSecretKeyByID(t *testing.T) {
	tests := map[string]struct {
		existing bool
	}{
		"existing key": {existing: true},
		"missing key":  {existing: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := auth.New()
			var ak *auth.AccessKey

			if tc.existing {
				ak = a.GenerateAccessKey()
			}

			keyID := "not-there"
			if tc.existing {
				keyID = ak.AccessKeyID
			}

			secret, ok := a.GetSecretKeyByID(keyID)
			assert.Equal(t, tc.existing, ok)
			if tc.existing {
				assert.Equal(t, ak.SecretKey, secret)
				return
			}
			assert.Empty(t, secret)
		})
	}
}
