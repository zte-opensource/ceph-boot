package main

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"golang.org/x/crypto/ssh"

	"github.com/mattn/go-shellwords"
	"github.com/reconquest/hierr-go"
	"github.com/zte-opensource/runcmd"
)

type CommandSession struct {
	node    *Node
	command runcmd.CmdWorker

	stdin  io.WriteCloser
	stdout io.WriteCloser
	stderr io.WriteCloser

	exitCode int
}

type remoteExecution struct {
	stdin io.WriteCloser
	nodes []*Node
}

type remoteExecutionResult struct {
	session *CommandSession

	err error
}

type rawCommand struct {
	command   []string
	args      []string
	shell     string
	directory string
	sudo      bool
	serial    bool
}

func (raw *rawCommand) parseCommand() (command []string, err error) {
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

func (session *CommandSession) wait() error {
	err := session.command.Wait()
	if err != nil {
		_ = session.stdout.Close()
		_ = session.stderr.Close()
		if sshErrors, ok := err.(*ssh.ExitError); ok {
			session.exitCode = sshErrors.Waitmsg.ExitStatus()

			return fmt.Errorf(
				`%s had failed to evaluate command, `+
					`remote command exited with non-zero code: %d`,
				session.node.String(),
				session.exitCode,
			)
		}

		return hierr.Errorf(
			err,
			`%s failed to finish execution, unexpected error`,
			session.node.String(),
		)
	}

	err = session.stdout.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stdout`,
			session.node.String(),
		)
	}

	err = session.stderr.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't close stderr`,
			session.node.String(),
		)
	}

	return nil
}

func (execution *remoteExecution) wait() error {
	tracef(`waiting %d nodes to finish`, len(execution.nodes))

	results := make(chan *remoteExecutionResult, 0)
	for _, node := range execution.nodes {
		go func(session *CommandSession) {
			results <- &remoteExecutionResult{session, session.wait()}
		}(node.session)
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
