package log

import "github.com/reconquest/barely"

type (
	verbosity    int
)

const (
	VerbosityQuiet verbosity = iota
	VerbosityNormal
	VerbosityDebug
	VerbosityTrace
)

var (
	Conf      Config
	statusBar *barely.StatusBar
)

type Config struct {
	Verbose  verbosity
}
