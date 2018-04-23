package log

import (
	"os"
	"sync"

	"github.com/zte-opensource/ceph-boot/color"
	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/statusbar"
)

func SetupLogger(verbose verbosity) error {
	Logger.SetIndentLines(true)

	SetLoggerVerbosity(verbose)

	return nil
}

func SetupStatusBar(theme string) error {
	barLock := &sync.Mutex{}

	barStyle, err := getStatusBarTheme(theme)
	if err != nil {
		Errorln(hierr.Errorf(
			err,
			`can't use given log bar style`,
		))
		return err
	}

	if color.HasTTY(int(os.Stderr.Fd())) {
		statusBar = statusbar.NewStatusBar(barStyle.Template)
		statusBar.SetLock(barLock)
	} else {
		statusBar = nil
	}

	return nil
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
				`can't draw log bar`,
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
