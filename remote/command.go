package remote

import (
	"fmt"
	"io"
	"strings"
	"golang.org/x/crypto/ssh"

	"github.com/mattn/go-shellwords"
	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/runcmd"
)

type Command struct {
	sudo    bool
	shell   string
	command string

	EscapedCommand []string
}

type CommandExecution struct {
	C *Command

	Node   *Node
	worker runcmd.CmdWorker

	Stdin  io.WriteCloser
	Stdout io.WriteCloser
	Stderr io.WriteCloser

	ExitCode int
}

type CommandResult struct {
	execution *CommandExecution
	err       error
}

var (
	defaultRemoteExecutionShell = "bash -c '{}'"
)

func New(sudo bool, shell string, command string) (*Command, error) {
	c := &Command{
		sudo:    sudo,
		shell:   shell,
		command: command,
	}

	if err := c.escapeCommand(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Command) escapeCommand() error {
	commandLine := escapeCommandLine(c.command)

	if c.sudo {
		commandLine = "sudo -n -E -H " + commandLine
	}

	command, err := shellwords.Parse(commandLine)
	if err != nil {
		return hierr.Errorf(
			err, "unparsable command line: %s", commandLine,
		)
	}

	c.EscapedCommand = command

	return nil
}

func (e *CommandExecution) Wait() error {
	err := e.worker.Wait()
	if err != nil {
		_ = e.Stdout.Close()
		_ = e.Stderr.Close()
		if sshErrors, ok := err.(*ssh.ExitError); ok {
			e.ExitCode = sshErrors.Waitmsg.ExitStatus()

			return fmt.Errorf(
				`%s had failed to evaluate command, `+
					`remote command exited with non-zero code: %d`,
				e.Node.String(),
				e.ExitCode,
			)
		}

		return hierr.Errorf(
			err,
			`%s failed to finish execution, unexpected error`,
			e.Node.String(),
		)
	}

	err = e.Stdout.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stdout`,
			e.Node.String(),
		)
	}

	err = e.Stderr.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stderr`,
			e.Node.String(),
		)
	}

	return nil
}

func escapeCommandLine(commandLine string) string {
	escaper := strings.NewReplacer(
		`'`, `'\''`,
	)

	escaper.Replace(commandLine)
	return fmt.Sprintf("/bin/bash -c '%s'", commandLine)
}
