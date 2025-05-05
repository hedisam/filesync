package ops

import (
	"time"
)

type Op string

const (
	OpCreated  Op = "op_created"
	OpRemoved  Op = "op_removed"
	OpModified Op = "op_modified"
)

type FileOp struct {
	Path      string    `json:"path"`
	Op        Op        `json:"op"`
	Timestamp time.Time `json:"timestamp"`
}
