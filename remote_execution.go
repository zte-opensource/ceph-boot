package main

import (
	"fmt"
	"io"
	"reflect"

    "golang.org/x/crypto/ssh"

    "github.com/reconquest/hierr-go"
    "github.com/zte-opensource/runcmd"
)

type remoteExecutionNode struct {
    node    *distributedLockNode
    command runcmd.CmdWorker

    stdin  io.WriteCloser
    stdout io.WriteCloser
    stderr io.WriteCloser

    exitCode int
}

type remoteExecution struct {
	stdin io.WriteCloser
	nodes map[*distributedLockNode]*remoteExecutionNode
}

type remoteExecutionResult struct {
	node *remoteExecutionNode

	err error
}

func (node *remoteExecutionNode) wait() error {
    err := node.command.Wait()
    if err != nil {
        _ = node.stdout.Close()
        _ = node.stderr.Close()
        if sshErrors, ok := err.(*ssh.ExitError); ok {
            node.exitCode = sshErrors.Waitmsg.ExitStatus()

            return fmt.Errorf(
                `%s had failed to evaluate command, `+
                    `remote command exited with non-zero code: %d`,
                node.node.String(),
                node.exitCode,
            )
        }

        return hierr.Errorf(
            err,
            `%s failed to finish execution, unexpected error`,
            node.node.String(),
        )
    }

    err = node.stdout.Close()
    if err != nil {
        return hierr.Errorf(
            err,
            `%s can't close stdout`,
            node.node.String(),
        )
    }

    err = node.stderr.Close()
    if err != nil {
        return hierr.Errorf(
            err,
            `%s can't close stderr`,
            node.node.String(),
        )
    }

    return nil
}

func (node *remoteExecutionNode) String() string {
    return node.node.String()
}

func (execution *remoteExecution) wait() error {
	tracef(`waiting %d nodes to finish`, len(execution.nodes))

	results := make(chan *remoteExecutionResult, 0)
	for _, node := range execution.nodes {
		go func(node *remoteExecutionNode) {
			results <- &remoteExecutionResult{node, node.wait()}
		}(node)
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
			exitCodes[result.node.exitCode]++

			executionErrors = hierr.Push(
				executionErrors,
				hierr.Errorf(
					result.err,
					`%s has finished with error`,
					result.node.node.String(),
				),
			)

			status.Fails++
			status.Total--

			tracef(
				`%s finished with exit code: '%d'`,
				result.node.node.String(),
				result.node.exitCode,
			)

			continue
		}

		status.Success++

		tracef(
			`%s has successfully finished execution`,
			result.node.node.String(),
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
