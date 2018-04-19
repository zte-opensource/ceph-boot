package status

import "github.com/reconquest/barely"

type (
	verbosity int
	outputFormat int
)

const (
	OutputFormatText outputFormat = iota
	OutputFormatJSON
)

const (
	VerbosityQuiet verbosity = iota
	VerbosityNormal
	VerbosityDebug
	VerbosityTrace
)

var (
	Conf Config
	statusBar *barely.StatusBar
)

type Config struct {
	Verbose verbosity
	Format outputFormat
	Theme string
	HasStdin bool
}
