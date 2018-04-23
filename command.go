package main

import (
	"fmt"
	"strings"

	"github.com/mattn/go-shellwords"
	"github.com/zte-opensource/ceph-boot/hierr"
)

type RawCommand struct {
	command   []string
	args      []string
	shell     string
	directory string
	sudo      bool
}

func (raw *RawCommand) ParseCommand() (command []string, err error) {
	commandline := joinCommand(raw.command)

	if raw.directory != "" {
		commandline = fmt.Sprintf("cd %s && { %s; }",
			escapeCommandArgumentStrict(raw.directory),
			commandline,
		)
	}

	if len(raw.shell) != 0 {
		commandline = wrapCommandIntoShell(
			commandline,
			raw.shell,
			raw.args,
		)
	}

	if raw.sudo {
		sudoCommand := []string{"sudo", "-n", "-E", "-H"}
		commandline = joinCommand(sudoCommand) + " " + commandline
	}

	command, err = shellwords.Parse(commandline)
	if err != nil {
		return nil, hierr.Errorf(
			err, "unparsable command line: %s", commandline,
		)
	}

	return
}

func wrapCommandIntoShell(command string, shell string, args []string) string {
	if shell == "" {
		return command
	}

	command = strings.Replace(shell, `{}`, command, -1)

	if len(args) == 0 {
		return command
	}

	escapedArgs := []string{}
	for _, arg := range args {
		escapedArgs = append(escapedArgs, escapeCommandArgumentStrict(arg))
	}

	return command + " _ " + strings.Join(escapedArgs, " ")
}

func joinCommand(command []string) string {
	escapedParts := []string{}

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
