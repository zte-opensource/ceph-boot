package main

import (
	"os"
	"sync"

	"github.com/reconquest/barely"
	"github.com/reconquest/hierr-go"
	"github.com/reconquest/loreley"
)

var (
	statusBar *barely.StatusBar
)

func SetupStatusBar(theme string, hasStdin bool) {
	barLock := &sync.Mutex{}

	barStyle, err := getStatusBarTheme(theme)
	if err != nil {
		Errorln(hierr.Errorf(
			err,
			`can't use given status bar style`,
		))
	}

	if loreley.HasTTY(int(os.Stderr.Fd())) {
		statusBar = barely.NewStatusBar(barStyle.Template)
		statusBar.SetLock(barLock)
	} else {
		statusBar = nil
	}

	if hasStdin && loreley.HasTTY(int(os.Stdin.Fd())) {
		statusBar = nil
	}
}

func SetStatus(status interface{}) {
	if statusBar == nil {
		return
	}

	ClearStatus()

	statusBar.SetStatus(status)

	DrawStatus()
}

func shouldDrawStatus() bool {
	if statusBar == nil {
		return false
	}

	if format != outputFormatText {
		return false
	}

	if verbose <= verbosityQuiet {
		return false
	}

	return true
}

func DrawStatus() {
	if !shouldDrawStatus() {
		return
	}

	err := statusBar.Render(os.Stderr)
	if err != nil {
		Errorf(
			"%s", hierr.Errorf(
				err,
				`can't draw status bar`,
			),
		)
	}
}

func ClearStatus() {
	if !shouldDrawStatus() {
		return
	}

	statusBar.Clear(os.Stderr)
}
