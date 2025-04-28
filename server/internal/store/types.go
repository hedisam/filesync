package store

import "time"

type ObjectMetadata struct {
	Key            string
	ObjectID       string
	SHA256Checksum string
	Size           int64
	MTime          int64
	CreatedAt      time.Time
	CompletedAt    *time.Time
}
