package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/reconquest/hierr-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/writer"
	"github.com/zte-opensource/runcmd"
)

const (
	longConnectionWarningTimeout = 2 * time.Second
	lockAcquiredString           = `acquired`
	lockBusyString               = `locked`

	heartbeatPing = "PING"
)

type Node struct {
	address address
	runner  runcmd.Runner

	hbio          *heartbeatIO
	remoteCommand *RemoteCommand
}

type heartbeatIO struct {
	stdin  io.WriteCloser
	stdout io.Reader
}

func NewNode(address address) *Node {
	return &Node{
		address: address,
	}
}

func (node *Node) String() string {
	return node.address.String()
}

func (node *Node) Lock(filename string) error {
	lockCommandLine := []string{
		"sh", "-c", fmt.Sprintf(
			`flock -nx %s -c 'printf "%s\n" && cat' || printf "%s\n"`,
			filename, lockAcquiredString, lockBusyString,
		),
	}

	logMutex := &sync.Mutex{}

	log.Traceln(hierr.Errorf(
		lockCommandLine,
		`%s running lock command`,
		node,
	))

	lockCommand := node.runner.Command(
		lockCommandLine[0],
		lockCommandLine[1:]...,
	)

	stdout, err := lockCommand.StdoutPipe()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get control stdout pipe from lock process`,
		)
	}

	stderr := lineflushwriter.New(
		writer.NewPrefixWriteCloser(
			writer.NewDebugWriteCloser(log.Logger),
			fmt.Sprintf("%s {flock} <stderr> ", node.String()),
		),
		logMutex,
		true,
	)

	lockCommand.SetStderr(stderr)

	stdin, err := lockCommand.StdinPipe()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get control stdin pipe to lock process`,
		)
	}

	err = lockCommand.Start()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't start lock command: '%s`,
			node, lockCommandLine,
		)
	}

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't read lock status line from lock process`,
			node,
		)
	}

	switch strings.TrimSpace(line) {
	case lockAcquiredString:
		// pass

	case lockBusyString:
		return fmt.Errorf(
			`%s can't acquire lock, `+
				`lock already obtained by another process `+
				`or unavailable`,
			node,
		)

	default:
		return fmt.Errorf(
			`%s unexpected reply string encountered `+
				`instead of '%s' or '%s': '%s'`,
			node, lockAcquiredString, lockBusyString,
			line,
		)
	}

	log.Tracef(`lock acquired: '%s' on '%s'`, node, filename)

	node.hbio = &heartbeatIO{
		stdin:  stdin,
		stdout: stdout,
	}

	return nil
}

// heartbeat runs infinite process of sending test messages to the connected
// node. All heartbeats to all nodes are connected to each other, so if one
// heartbeat routine exits, all heartbeat routines will exit, because in that
// case orgalorg can't guarantee global lock.
func (node *Node) Heartbeat(
	period time.Duration,
	canceler *sync.Cond,
) {
	abort := make(chan struct{}, 0)

	// Internal go-routine for listening abort broadcast and finishing current
	// heartbeat process.
	go func() {
		canceler.L.Lock()
		canceler.Wait()
		canceler.L.Unlock()

		abort <- struct{}{}
	}()

	// Finish finishes current go-routine and send abort broadcast to all
	// connected go-routines.
	finish := func(code int) {
		canceler.L.Lock()
		canceler.Broadcast()
		canceler.L.Unlock()

		<-abort

		if remote, ok := node.runner.(*runcmd.Remote); ok {
			log.Tracef("%s closing connection", node.String())
			err := remote.CloseConnection()
			if err != nil {
				log.Warningf(
					"%s",
					hierr.Errorf(
						err,
						"%s error while closing connection",
						node.String(),
					),
				)
			}
		}

		exit(code)
	}

	ticker := time.Tick(period)

	// Infinite loop of heartbeating. It will send heartbeat message, wait
	// fraction of send timeout time and try to receive heartbeat response.
	// If no response received, heartbeat process aborts.
	for {
		_, err := io.WriteString(node.hbio.stdin, heartbeatPing+"\n")
		if err != nil {
			log.Errorf(
				"%s",
				hierr.Errorf(
					err,
					`%s can't send heartbeat`,
					node.String(),
				),
			)

			finish(2)
		}

		select {
		case <-abort:
			return

		case <-ticker:
			// pass
		}

		ping, err := bufio.NewReader(node.hbio.stdout).ReadString('\n')
		if err != nil {
			log.Errorf(
				"%s",
				hierr.Errorf(
					err,
					`%s can't receive heartbeat`,
					node.String(),
				),
			)

			finish(2)
		}

		if strings.TrimSpace(ping) != heartbeatPing {
			log.Errorf(
				`%s received unexpected heartbeat ping: '%s'`,
				node.String(),
				ping,
			)

			finish(2)
		}

		log.Tracef(`%s heartbeat`, node.String())
	}
}

func (node *Node) Connect(runnerFactory runnerFactory) error {
	log.Tracef(`connecting to address: '%s'`, node.address)

	done := make(chan struct{}, 0)

	go func() {
		select {
		case <-done:
			return

		case <-time.After(longConnectionWarningTimeout):
			log.Warningf(
				"still connecting to address after %s: %s",
				longConnectionWarningTimeout,
				node.address,
			)

			<-done
		}
	}()

	defer func() {
		done <- struct{}{}
	}()

	// to establish a ssh connection and return a instance of runcmd.Runner, which
	// can be used to start remote execution sessions
	runner, err := runnerFactory(node.address)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't connect to address: %s`,
			node.address,
		)
	}

	node.runner = runner

	return nil
}

func (node *Node) CreateRemoteCommand(
	command []string,
	logLock sync.Locker,
	outputLock sync.Locker,
) (*RemoteCommand, error) {
	worker := node.runner.Command(command[0], command[1:]...)

	stdoutBackend := io.Writer(os.Stdout)
	stderrBackend := io.Writer(os.Stderr)

	var stdout io.WriteCloser
	var stderr io.WriteCloser
	switch {
	case log.Conf.Verbose == log.VerbosityQuiet:
		stdout = lineflushwriter.New(
			writer.NewNopWriteCloser(stdoutBackend),
			logLock,
			false)
		stderr = lineflushwriter.New(
			writer.NewNopWriteCloser(stderrBackend),
			logLock,
			false)

	case log.Conf.Verbose == log.VerbosityNormal:
		stdout = lineflushwriter.New(
			writer.NewPrefixWriteCloser(
				writer.NewNopWriteCloser(stdoutBackend),
				node.address.domain+" ",
			),
			logLock,
			true,
		)

		stderr = lineflushwriter.New(
			writer.NewPrefixWriteCloser(
				writer.NewNopWriteCloser(stderrBackend),
				node.address.domain+" ",
			),
			logLock,
			true,
		)

	default:
		stdout = lineflushwriter.New(
			writer.NewPrefixWriteCloser(
				writer.NewDebugWriteCloser(log.Logger),
				"{cmd} <stdout> "+node.String()+" ",
			),
			logLock,
			false,
		)

		stderr = lineflushwriter.New(
			writer.NewPrefixWriteCloser(
				writer.NewDebugWriteCloser(log.Logger),
				"{cmd} <stderr> "+node.String()+" ",
			),
			logLock,
			false,
		)
	}

	stdout = log.NewStatusBarWriteCloser(stdout)
	stderr = log.NewStatusBarWriteCloser(stderr)

	// node level output lock
	if outputLock != (*sync.Mutex)(nil) {
		sharedLock := writer.NewSharedLock(outputLock, 2)

		stdout = writer.NewLockedWriter(stdout, sharedLock)
		stderr = writer.NewLockedWriter(stderr, sharedLock)
	}

	stdin, err := worker.StdinPipe()
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't get stdin from remote command`,
		)
	}

	worker.SetStdout(stdout)
	worker.SetStderr(stderr)

	return &RemoteCommand{
		node:   node,
		worker: worker,

		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}, nil
}
