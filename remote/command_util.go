package remote

import (
	"strings"
)

func wrapCommandIntoShell(command string, shell string, args []string) string {
	if shell == "" {
		return command
	}

	command = strings.Replace(shell, `{}`, command, -1)

	if len(args) == 0 {
		return command
	}

	var escapedArgs []string
	for _, arg := range args {
		escapedArgs = append(escapedArgs, escapeCommandArgumentStrict(arg))
	}

	return command + " _ " + strings.Join(escapedArgs, " ")
}

func joinCommand(command []string) string {
	var escapedParts []string

	for _, part := range command {
		escapedParts = append(escapedParts, escapeCommandArgument(part))
	}

	return strings.Join(escapedParts, ` `)
}

func escapeCommandArgument(argument string) string {
	argument = strings.Replace(argument, `'`, `'\''`, -1)

	return argument
}

func escapeCommandArgumentStrict(argument string) string {
	escaper := strings.NewReplacer(
		`\`, `\\`,
		"`", "\\`",
		`"`, `\"`,
		`'`, `'\''`,
		`$`, `\$`,
	)

	escaper.Replace(argument)

	return `"` + argument + `"`
}
