package status

import (
	"os"
	"sync"

	"github.com/reconquest/barely"
	"github.com/reconquest/hierr-go"
	"github.com/reconquest/loreley"
)

func SetupStatusBar() {
	barLock := &sync.Mutex{}

	barStyle, err := getStatusBarTheme(Conf.Theme)
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

	if Conf.HasStdin && loreley.HasTTY(int(os.Stdin.Fd())) {
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

	if Conf.Format != OutputFormatText {
		return false
	}

	if Conf.Verbose <= VerbosityQuiet {
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
