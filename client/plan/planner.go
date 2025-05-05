package plan

import (
	"maps"

	"github.com/sirupsen/logrus"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/index"
	"github.com/hedisam/filesync/client/ops"
)

type Planner struct {
	logger *logrus.Logger
}

func NewPlanner(logger *logrus.Logger) *Planner {
	return &Planner{
		logger: logger,
	}
}

func (p *Planner) Generate(localSnapshot map[string]*index.FileMetadata, serverSnapshot map[string]*restapi.File) *Plan {
	if serverSnapshot != nil {
		return p.generateWithServerSnapshot(localSnapshot, serverSnapshot)
	}

	var requests []PlanRequest

	for filePath, localFile := range localSnapshot {
		switch localFile.Op {
		case ops.OpCreated, ops.OpModified:
			requests = append(requests, &uploadRequest{
				fileMetadata: localFile,
			})
		case ops.OpRemoved:
			requests = append(requests, &deleteRequest{
				filePath: filePath,
			})
		default:
			p.logger.WithFields(logrus.Fields{
				"op":        localFile.Op,
				"file_name": filePath,
			}).Warn("Unknown file operation while generating plan, dropping")
			continue
		}
	}

	return &Plan{
		Requests: requests,
	}
}

func (p *Planner) generateWithServerSnapshot(localSnapshot map[string]*index.FileMetadata, serverSnapshot map[string]*restapi.File) *Plan {
	var requests []PlanRequest

	// delete the removed ops; we simply compare the local snapshot with the server's and plan a deletion
	// if a file doesn't exist locally but exists on the server
	maps.DeleteFunc(localSnapshot, func(_ string, md *index.FileMetadata) bool {
		return md.Op == ops.OpRemoved
	})

	for fileName, localFile := range localSnapshot {
		if localFile.Op != ops.OpCreated && localFile.Op != ops.OpModified {
			p.logger.WithFields(logrus.Fields{
				"op":        localFile.Op,
				"file_name": fileName,
			}).Warn("Unknown file operation while generating plan with server snapshot, dropping")
			continue
		}

		remoteFile, ok := serverSnapshot[fileName]
		if !ok || localFile.SHA256 != remoteFile.SHA256Checksum {
			requests = append(requests, &uploadRequest{
				fileMetadata: localFile,
			})
		}
	}
	for filePath := range serverSnapshot {
		_, ok := localSnapshot[filePath]
		if !ok {
			requests = append(requests, &deleteRequest{
				filePath: filePath,
			})
		}
	}

	return &Plan{
		Requests: requests,
	}
}
