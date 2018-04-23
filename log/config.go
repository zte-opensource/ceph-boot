package log

import (
	"github.com/zte-opensource/ceph-boot/statusbar"
)

type (
	verbosity int
)

const (
	VerbosityQuiet verbosity = iota
	VerbosityNormal
	VerbosityDebug
	VerbosityTrace
)

var (
	Conf      Config
	statusBar *statusbar.StatusBar
)

type Config struct {
	Verbose verbosity
}
