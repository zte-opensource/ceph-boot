package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/hierr-go"
	"github.com/reconquest/loreley"
	"github.com/zte-opensource/ceph-boot/writer"
)

type (
	verbosity int
)

type (
	outputFormat int
)

const (
	outputFormatText outputFormat = iota
	outputFormatJSON
)

const (
	verbosityQuiet verbosity = iota
	verbosityNormal
	verbosityDebug
	verbosityTrace
)

var (
	logger  = lorg.NewLog()
	verbose = verbosityNormal
	format  = outputFormatText

	loggerFormattingBasicLength = 0
)

func SetLoggerOutputFormat(format outputFormat) {
	if format == outputFormatJSON {
		logger.SetOutput(writer.NewJsonWriter(
			"stderr",
			"",
			os.Stderr,
		))
	}
}

func SetLoggerVerbosity(level verbosity) {
	logger.SetLevel(lorg.LevelWarning)

	switch {
	case level >= verbosityTrace:
		logger.SetLevel(lorg.LevelTrace)

	case level >= verbosityDebug:
		logger.SetLevel(lorg.LevelDebug)

	case level >= verbosityNormal:
		logger.SetLevel(lorg.LevelInfo)
	}
}

func SetLoggerStyle(style lorg.Formatter) {
	testLogger := lorg.NewLog()

	testLogger.SetFormat(style)

	buffer := &bytes.Buffer{}
	testLogger.SetOutput(buffer)

	testLogger.Debug(``)

	loggerFormattingBasicLength = len(strings.TrimSuffix(
		loreley.TrimStyles(buffer.String()),
		"\n",
	))

	logger.SetFormat(style)
	logger.SetIndentLines(true)
}

func Tracef(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Tracef(`%s`, wrapLines(format, args...))

	DrawStatus()
}

func Traceln(args ...interface{}) {
	Tracef("%s", fmt.Sprint(serializeErrors(args)...))
}

func Debugf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Debugf(`%s`, wrapLines(format, args...))

	DrawStatus()
}

func Debugln(args ...interface{}) {
	Debugf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Infof(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Infof(`%s`, wrapLines(format, args...))

	DrawStatus()
}

func Infoln(args ...interface{}) {
	Infof("%s", fmt.Sprint(serializeErrors(args)...))
}

func Warningf(format string, args ...interface{}) {
	args = serializeErrors(args)

	if verbose <= verbosityQuiet {
		return
	}

	logger.Warningf(`%s`, wrapLines(format, args...))

	DrawStatus()
}

func Warningln(args ...interface{}) {
	Warningf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Errorf(format string, args ...interface{}) {
	args = serializeErrors(args)

	logger.Errorf(`%s`, wrapLines(format, args...))
}

func Errorln(args ...interface{}) {
	Errorf("%s", fmt.Sprint(serializeErrors(args)...))
}

func Fatalf(format string, args ...interface{}) {
	args = serializeErrors(args)

	ClearStatus()

	logger.Fatalf(`%s`, wrapLines(format, args...))

	exit(1)
}

func Fatalln(args ...interface{}) {
	Fatalf("%s", fmt.Sprint(serializeErrors(args)...))
}

func wrapLines(format string, values ...interface{}) string {
	contents := fmt.Sprintf(format, values...)
	contents = strings.TrimSuffix(contents, "\n")
	contents = strings.Replace(
		contents,
		"\n",
		"\n"+strings.Repeat(" ", loggerFormattingBasicLength),
		-1,
	)

	return contents
}

func serializeErrors(args []interface{}) []interface{} {
	for i, arg := range args {
		if err, ok := arg.(error); ok {
			args[i] = serializeError(err)
		}
	}

	return args
}

func serializeError(err error) string {
	if format == outputFormatText {
		return fmt.Sprint(err)
	}

	if hierarchicalError, ok := err.(hierr.Error); ok {
		serializedError := fmt.Sprint(hierarchicalError.Nested)
		switch nested := hierarchicalError.Nested.(type) {
		case error:
			serializedError = serializeError(nested)

		case []hierr.NestedError:
			serializeErrorParts := []string{}

			for _, nestedPart := range nested {
				serializedPart := fmt.Sprint(nestedPart)
				switch part := nestedPart.(type) {
				case error:
					serializedPart = serializeError(part)

				case string:
					serializedPart = part
				}

				serializeErrorParts = append(
					serializeErrorParts,
					serializedPart,
				)
			}

			serializedError = strings.Join(serializeErrorParts, "; ")
		}

		return hierarchicalError.Message + ": " + serializedError
	}

	return err.Error()
}
