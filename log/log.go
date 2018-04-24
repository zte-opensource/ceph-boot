package log

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

type (
	verbosity int
)

const (
	VerbosityQuiet verbosity = iota
	VerbosityFatal
	VerbosityError
	VerbosityWarn
	VerbosityInfo
	VerbosityDebug
)

var (
	logger *log.Logger
)

func init() {
	formatter := &log.TextFormatter{
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	}

	logger = &log.Logger{
		Out:       os.Stderr,
		Formatter: formatter,
		Hooks:     make(log.LevelHooks),
		Level:     log.WarnLevel,
	}
}

func SetupLogger(v verbosity) error {
	logger.SetLevel(log.WarnLevel)
	switch {
	case v >= VerbosityDebug:
		logger.SetLevel(log.DebugLevel)
	case v >= VerbosityInfo:
		logger.SetLevel(log.InfoLevel)
	case v >= VerbosityWarn:
		logger.SetLevel(log.WarnLevel)
	case v >= VerbosityError:
		logger.SetLevel(log.ErrorLevel)
	case v >= VerbosityFatal:
		logger.SetLevel(log.FatalLevel)
	}

	return nil
}

func Debugf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Debugf(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Debugln(args ...interface{}) {
	Debugf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Infof(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Infof(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Infoln(args ...interface{}) {
	Infof("%s", fmt.Sprint(serializeErrors(args)...))
}

func Warningf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Warningf(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Warningln(args ...interface{}) {
	Warningf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Errorf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Errorf(`%s`, fmt.Sprintf(format, args...))
}

func Errorln(args ...interface{}) {
	Errorf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Fatalf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Fatalf(`%s`, fmt.Sprintf(format, args...))

	os.Exit(1)
}

func Fatalln(args ...interface{}) {
	Fatalf("%s", fmt.Sprint(serializeErrors(args)...))
}

func serializeErrors(args []interface{}) []interface{} {
	for i, arg := range args {
		if err, ok := arg.(error); ok {
			args[i] = fmt.Sprint(err)
		}
	}

	return args
}
