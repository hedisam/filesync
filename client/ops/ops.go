package ops

type Op int

const (
	OpCreated Op = iota
	OpRemoved
	OpModified
)
