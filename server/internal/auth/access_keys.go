package auth

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"sync"
)

// Auth is a simple authentication system that holds raw secret keys in memory until the program exits.
// Super safe, right? In a production system, the secret will be encrypted.
// Why not hash the secret? It's not a password, and we need the raw secret for validating presigned urls.
type Auth struct {
	m  map[string]string
	mu sync.RWMutex
}

func New() *Auth {
	return &Auth{
		m: make(map[string]string),
	}
}

type AccessKey struct {
	AccessKeyID string
	SecretKey   string
}

// GenerateAccessKey generates a pair of access key ID and secret.
// Kinda like AWS access keys, we generate 20 and 40 chars for the access key ID and secret key, respectively.
func (auth *Auth) GenerateAccessKey() *AccessKey {
	// 12 bytes base32 encoded
	idBytes := make([]byte, 12)
	_, _ = rand.Read(idBytes)
	// base32 encoding is better for a sharable key ID as it's more human-readable and url safe
	// since it's just upper case chars and digits.
	id := base32.StdEncoding.EncodeToString(idBytes)[:20]

	secretBytes := make([]byte, 30)
	_, _ = rand.Read(secretBytes)
	secret := base64.StdEncoding.EncodeToString(secretBytes)[:40]

	auth.mu.Lock()
	auth.m[id] = secret
	auth.mu.Unlock()

	return &AccessKey{
		AccessKeyID: id,
		SecretKey:   secret,
	}
}

func (auth *Auth) GetSecretKeyByID(keyID string) (string, bool) {
	auth.mu.RLock()
	defer auth.mu.RUnlock()

	secret, ok := auth.m[keyID]
	return secret, ok
}
