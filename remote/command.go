package remote

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"sync"

	"github.com/mattn/go-shellwords"
	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/writer"
	"github.com/zte-opensource/runcmd"
)

type Command struct {
	directory string
	shell     string
	sudo      bool
	command   []string
	args      []string

	EscapedCommand []string

	Node   *Node
	worker runcmd.CmdWorker

	Stdin  io.WriteCloser
	Stdout io.WriteCloser
	Stderr io.WriteCloser

	ExitCode int
}

type CommandResult struct {
	C *Command

	Err error
}

var (
	defaultRemoteExecutionShell = "bash -c '{}'"
)

func New(
	directory string,
	sudo bool,
	shell string,
	command []string,
	args []string,
) (*Command, error) {
	c := &Command{
		directory: directory,
		sudo:      sudo,
		shell:     shell,
		command:   command,
		args:      args,
	}

	if err := c.escapeCommand(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Command) escapeCommand() error {
	commandLine := joinCommand(c.command)

	if c.directory != "" {
		commandLine = fmt.Sprintf("cd %s && { %s; }",
			escapeCommandArgumentStrict(c.directory),
			commandLine,
		)
	}

	if len(c.shell) != 0 {
		commandLine = wrapCommandIntoShell(
			commandLine,
			c.shell,
			c.args,
		)
	}

	if c.sudo {
		sudoCommand := []string{"sudo", "-n", "-E", "-H"}
		commandLine = joinCommand(sudoCommand) + " " + commandLine
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

func (c *Command) Run(
	node *Node,
	prefix string,
	logLock sync.Locker,
	outputLock sync.Locker,
) error {
	if err := c.escapeCommand(); err != nil {
		return err
	}

	worker := node.runner.Command(c.EscapedCommand[0], c.EscapedCommand[1:]...)

	stdoutBackend := io.Writer(os.Stdout)
	stderrBackend := io.Writer(os.Stderr)

	var stdout io.WriteCloser
	var stderr io.WriteCloser

	if prefix == "" {
		stdout = writer.NewLineFlushWriteCloser(
			writer.NewNopWriteCloser(stdoutBackend),
			logLock,
			false)
		stderr = writer.NewLineFlushWriteCloser(
			writer.NewNopWriteCloser(stderrBackend),
			logLock,
			false)
	} else {
		stdout = writer.NewLineFlushWriteCloser(
			writer.NewPrefixWriteCloser(
				writer.NewNopWriteCloser(stdoutBackend),
				prefix,
			),
			logLock,
			true,
		)
		stderr = writer.NewLineFlushWriteCloser(
			writer.NewPrefixWriteCloser(
				writer.NewNopWriteCloser(stderrBackend),
				prefix,
			),
			logLock,
			true,
		)
	}

	stdout = log.NewStatusBarWriteCloser(stdout)
	stderr = log.NewStatusBarWriteCloser(stderr)

	// node level stdout/stderr lock
	if outputLock != (*sync.Mutex)(nil) {
		sharedLock := writer.NewSharedLock(outputLock, 2)

		stdout = writer.NewLockedWriter(stdout, sharedLock)
		stderr = writer.NewLockedWriter(stderr, sharedLock)
	}

	stdin, err := worker.StdinPipe()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get stdin from remote command`,
		)
	}

	worker.SetStdout(stdout)
	worker.SetStderr(stderr)

	c.Node = node
	c.worker = worker

	c.Stdin = stdin
	c.Stdout = stdout
	c.Stderr = stderr

	err = worker.Start()

	return err
}

func (c *Command) Wait() error {
	err := c.worker.Wait()
	if err != nil {
		_ = c.Stdout.Close()
		_ = c.Stderr.Close()
		if sshErrors, ok := err.(*ssh.ExitError); ok {
			c.ExitCode = sshErrors.Waitmsg.ExitStatus()

			return fmt.Errorf(
				`%s had failed to evaluate command, `+
					`remote command exited with non-zero code: %d`,
				c.Node.String(),
				c.ExitCode,
			)
		}

		return hierr.Errorf(
			err,
			`%s failed to finish execution, unexpected error`,
			c.Node.String(),
		)
	}

	err = c.Stdout.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stdout`,
			c.Node.String(),
		)
	}

	err = c.Stderr.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stderr`,
			c.Node.String(),
		)
	}

	return nil
}
