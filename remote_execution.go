package main

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"reflect"

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
	session *RemoteCommand

	err error
}

type RemoteExecution struct {
	stdin io.WriteCloser
	nodes []*Node
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

func (execution *RemoteExecution) Wait() error {
	tracef(`waiting %d nodes to finish`, len(execution.nodes))

	results := make(chan *RemoteCommandResult, 0)
	for _, node := range execution.nodes {
		go func(session *RemoteCommand) {
			results <- &RemoteCommandResult{session, session.Wait()}
		}(node.remoteCommand)
	}

	executionErrors := fmt.Errorf(
		`commands are exited with non-zero code`,
	)

	var (
		status = &struct {
			Phase   string
			Total   int
			Fails   int
			Success int
		}{
			Phase: `wait`,
			Total: len(execution.nodes),
		}

		exitCodes = map[int]int{}
	)

	setStatus(status)

	for range execution.nodes {
		result := <-results
		if result.err != nil {
			exitCodes[result.session.exitCode]++

			executionErrors = hierr.Push(
				executionErrors,
				hierr.Errorf(
					result.err,
					`%s has finished with error`,
					result.session.node.String(),
				),
			)

			status.Fails++
			status.Total--

			tracef(
				`%s finished with exit code: '%d'`,
				result.session.node.String(),
				result.session.exitCode,
			)

			continue
		}

		status.Success++

		tracef(
			`%s has successfully finished execution`,
			result.session.node.String(),
		)
	}

	if status.Fails > 0 {
		if status.Fails == len(execution.nodes) {
			exitCodesValue := reflect.ValueOf(exitCodes)

			topError := fmt.Errorf(
				`commands are failed on all %d nodes`,
				len(execution.nodes),
			)

			for _, key := range exitCodesValue.MapKeys() {
				topError = hierr.Push(
					topError,
					fmt.Sprintf(
						`code %d (%d nodes)`,
						key.Int(),
						exitCodesValue.MapIndex(key).Int(),
					),
				)
			}

			return topError
		}

		return hierr.Errorf(
			executionErrors,
			`commands are failed on %d out of %d nodes`,
			status.Fails,
			len(execution.nodes),
		)
	}

	return nil
}
