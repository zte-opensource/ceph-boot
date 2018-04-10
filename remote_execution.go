package main

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/crypto/ssh"

	"github.com/reconquest/hierr-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/prefixwriter-go"
	"github.com/zte-opensource/runcmd"
	"github.com/mattn/go-shellwords"
)

type remoteNodesMap map[*distributedLockNode]*remoteExecutionNode

type remoteNodes struct {
	*sync.Mutex

	nodes remoteNodesMap
}

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

type remoteExecutionRunner struct {
	command   []string
	args      []string
	shell     string
	directory string
	sudo      bool
	serial    bool
}

var (
	sudoCommand = []string{"sudo", "-n", "-E", "-H"}
)

func (nodes *remoteNodes) Set(
	node *distributedLockNode,
	remote *remoteExecutionNode,
) {
	nodes.Lock()
	defer nodes.Unlock()

	nodes.nodes[node] = remote
}

func runRemoteExecution(
	lockedNodes *distributedLock,
	command []string,
	setupCallback func(*remoteExecutionNode),
	serial bool,
) (*remoteExecution, error) {
	var (
		stdins = []io.WriteCloser{}

		logLock    = &sync.Mutex{}
		stdinsLock = &sync.Mutex{}
		outputLock = &sync.Mutex{}

		nodes = &remoteNodes{&sync.Mutex{}, remoteNodesMap{}}
	)

	if !serial {
		outputLock = nil
	}

	var (
		status = &struct {
			sync.Mutex

			Phase   string
			Total   int
			Fails   int
			Success int
		}{
			Phase: `exec`,
			Total: len(lockedNodes.nodes),
		}
	)

	setStatus(status)

	type nodeErr struct {
		err  error
		node *distributedLockNode
	}

	errors := make(chan *nodeErr, 0)
	for _, node := range lockedNodes.nodes {
		go func(node *distributedLockNode) {
			pool.run(func() {
				tracef(
					"%s",
					hierr.Errorf(
						command,
						"%s starting command",
						node.String(),
					).Error(),
				)

				remoteNode, err := runRemoteExecutionNode(
					node,
					command,
					logLock,
					outputLock,
				)
				if err != nil {
					errors <- &nodeErr{err, node}

					status.Lock()
					defer status.Unlock()

					status.Total--
					status.Fails++

					return
				}

				if setupCallback != nil {
					setupCallback(remoteNode)
				}

				remoteNode.command.SetStdout(remoteNode.stdout)
				remoteNode.command.SetStderr(remoteNode.stderr)

				err = remoteNode.command.Start()
				if err != nil {
					errors <- &nodeErr{
						hierr.Errorf(
							err,
							`can't start remote command`,
						),
						node,
					}

					status.Lock()
					defer status.Unlock()

					status.Total--
					status.Fails++

					return
				}

				nodes.Set(node, remoteNode)

				stdinsLock.Lock()
				defer stdinsLock.Unlock()

				stdins = append(stdins, remoteNode.stdin)

				status.Lock()
				defer status.Unlock()

				status.Success++

				errors <- nil
			})
		}(node)
	}

	for range lockedNodes.nodes {
		err := <-errors
		if err != nil {
			return nil, hierr.Errorf(
				err.err,
				`%s remote execution failed`,
				err.node,
			)
		}
	}

	return &remoteExecution{
		stdin: &multiWriteCloser{stdins},

		nodes: nodes.nodes,
	}, nil
}

func runRemoteExecutionNode(
	node *distributedLockNode,
	command []string,
	logLock sync.Locker,
	outputLock sync.Locker,
) (*remoteExecutionNode, error) {
	remoteCommand := node.runner.Command(command[0], command[1:]...)

	stdoutBackend := io.Writer(os.Stdout)
	stderrBackend := io.Writer(os.Stderr)

	if format == outputFormatJSON {
		stdoutBackend = &jsonOutputWriter{
			stream: `stdout`,
			node:   node.String(),

			output: os.Stdout,
		}

		stderrBackend = &jsonOutputWriter{
			stream: `stderr`,
			node:   node.String(),

			output: os.Stderr,
		}
	}

	var stdout io.WriteCloser
	var stderr io.WriteCloser
	switch {
	case verbose == verbosityQuiet || format == outputFormatJSON:
		stdout = lineflushwriter.New(nopCloser{stdoutBackend}, logLock, false)
		stderr = lineflushwriter.New(nopCloser{stderrBackend}, logLock, false)

	case verbose == verbosityNormal:
		stdout = lineflushwriter.New(
			prefixwriter.New(
				nopCloser{stdoutBackend},
				node.address.domain+" ",
			),
			logLock,
			true,
		)

		stderr = lineflushwriter.New(
			prefixwriter.New(
				nopCloser{stderrBackend},
				node.address.domain+" ",
			),
			logLock,
			true,
		)

	default:
		stdout = lineflushwriter.New(
			prefixwriter.New(
				newDebugWriter(logger),
				"{cmd} <stdout> "+node.String()+" ",
			),
			logLock,
			false,
		)

		stderr = lineflushwriter.New(
			prefixwriter.New(
				newDebugWriter(logger),
				"{cmd} <stderr> "+node.String()+" ",
			),
			logLock,
			false,
		)
	}

	stdout = &statusBarUpdateWriter{stdout}
	stderr = &statusBarUpdateWriter{stderr}

	if outputLock != (*sync.Mutex)(nil) {
		sharedLock := newSharedLock(outputLock, 2)

		stdout = newLockedWriter(stdout, sharedLock)
		stderr = newLockedWriter(stderr, sharedLock)
	}

	stdin, err := remoteCommand.StdinPipe()
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't get stdin from remote command`,
		)
	}

	return &remoteExecutionNode{
		node:    node,
		command: remoteCommand,

		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}, nil
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

func (runner *remoteExecutionRunner) run(
	cluster *distributedLock,
	setupCallback func(*remoteExecutionNode),
) (*remoteExecution, error) {
	commandline := joinCommand(runner.command)

	if runner.directory != "" {
		commandline = fmt.Sprintf("cd %s && { %s; }",
			escapeCommandArgumentStrict(runner.directory),
			commandline,
		)
	}

	if len(runner.shell) != 0 {
		commandline = wrapCommandIntoShell(
			commandline,
			runner.shell,
			runner.args,
		)
	}

	if runner.sudo {
		commandline = joinCommand(sudoCommand) + " " + commandline
	}

	command, err := shellwords.Parse(commandline)
	if err != nil {
		return nil, hierr.Errorf(
			err, "unparsable command line: %s", commandline,
		)
	}

	return runRemoteExecution(cluster, command, setupCallback, runner.serial)
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
