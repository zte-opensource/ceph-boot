package log

import (
	"fmt"

	"github.com/zte-opensource/ceph-boot/color"
)

const (
	ThemeDefault = `default`
	ThemeDark    = `dark`
	ThemeLight   = `light`
)

var (
	statusBarThemeTemplate = `{bg %d}{fg %d}` +
		`{bold}` +
		`{if eq .Phase "lock"}{bg %d}  LOCK{end}` +
		`{if eq .Phase "connect"}{bg %[3]d}  CONNECT{end}` +
		`{if eq .Phase "exec"}{bg %d}  EXEC{end}` +
		`{if eq .Phase "wait"}{bg %d}  WAIT{end}` +
		`{if eq .Phase "upload"}{bg %d}  UPLOAD{end}` +
		`{nobold} ` +
		`{from "" %d} ` +
		`{fg %d}{bold}{printf "%%4d" .Success}{nobold}{fg %d}` +
		`/{printf "%%4d" .Total} ` +
		`{if .Fails}{fg %d}✗ {.Fails}{end} ` +
		`{from "" %d}` +
		`{if eq .Phase "upload"}{fg %d} ` +
		`{printf "%%9s/%%s" .Written .Bytes} ` +
		`{end}`

	statusBarThemes = map[string]string{
		ThemeDark: fmt.Sprintf(
			statusBarThemeTemplate,
			99, 7, 22, 1, 1, 25, 237, 46, 15, 214, -1, 140,
		),

		ThemeLight: fmt.Sprintf(
			statusBarThemeTemplate,
			99, 7, 22, 1, 1, 64, 254, 106, 16, 9, -1, 140,
		),

		ThemeDefault: fmt.Sprintf(
			statusBarThemeTemplate,
			234, 255, 22, 1, 1, 19, 245, 85, 255, 160, -1, 140,
		),
	}
)

func getStatusBarTheme(theme string) (*color.Style, error) {
	if format, ok := statusBarThemes[theme]; ok {
		theme = format
	}

	style, err := color.CompileWithReset(theme, nil)
	if err != nil {
		return nil, err
	}

	return style, nil
}
