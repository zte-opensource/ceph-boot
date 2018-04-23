package log

import (
	"fmt"
	"os"

	"github.com/kovetskiy/lorg"
)

var (
	Logger = lorg.NewLog()
)

func SetLoggerVerbosity(v verbosity) {
	Conf.Verbose = v

	Logger.SetLevel(lorg.LevelWarning)

	switch {
	case v >= VerbosityTrace:
		Logger.SetLevel(lorg.LevelTrace)

	case v >= VerbosityDebug:
		Logger.SetLevel(lorg.LevelDebug)

	case v >= VerbosityNormal:
		Logger.SetLevel(lorg.LevelInfo)
	}
}

func Tracef(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Tracef(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Traceln(args ...interface{}) {
	Tracef("%s", fmt.Sprint(serializeErrors(args)...))
}

func Debugf(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Debugf(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Debugln(args ...interface{}) {
	Debugf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Infof(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Infof(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Infoln(args ...interface{}) {
	Infof("%s", fmt.Sprint(serializeErrors(args)...))
}

func Warningf(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Warningf(`%s`, fmt.Sprintf(format, args...))

	DrawStatus()
}

func Warningln(args ...interface{}) {
	Warningf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Errorf(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Errorf(`%s`, fmt.Sprintf(format, args...))
}

func Errorln(args ...interface{}) {
	Errorf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Fatalf(format string, args ...interface{}) {
	args = serializeErrors(args)

	Logger.Fatalf(`%s`, fmt.Sprintf(format, args...))

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
