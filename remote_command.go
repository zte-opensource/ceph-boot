package main

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"

	"github.com/reconquest/hierr-go"
	"github.com/zte-opensource/runcmd"
)

type RemoteCommand struct {
	node   *Node
	worker runcmd.CmdWorker

	stdin  io.WriteCloser
	stdout io.WriteCloser
	stderr io.WriteCloser

	exitCode int
}

type RemoteCommandResult struct {
	rc *RemoteCommand

	err error
}

func (rc *RemoteCommand) Wait() error {
	err := rc.worker.Wait()
	if err != nil {
		_ = rc.stdout.Close()
		_ = rc.stderr.Close()
		if sshErrors, ok := err.(*ssh.ExitError); ok {
			rc.exitCode = sshErrors.Waitmsg.ExitStatus()

			return fmt.Errorf(
				`%s had failed to evaluate command, `+
					`remote command exited with non-zero code: %d`,
				rc.node.String(),
				rc.exitCode,
			)
		}

		return hierr.Errorf(
			err,
			`%s failed to finish execution, unexpected error`,
			rc.node.String(),
		)
	}

	err = rc.stdout.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stdout`,
			rc.node.String(),
		)
	}

	err = rc.stderr.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stderr`,
			rc.node.String(),
		)
	}

	return nil
}
