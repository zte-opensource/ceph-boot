package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/reconquest/hierr-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/prefixwriter-go"
	"github.com/zte-opensource/runcmd"
)

const (
	longConnectionWarningTimeout = 2 * time.Second
	lockAcquiredString           = `acquired`
	lockLockedString             = `locked`

	heartbeatPing = "PING"
)

type Node struct {
	address address
	runner  runcmd.Runner

	hbio    *heartbeatIO
	session *CommandSession
}

type heartbeatIO struct {
	stdin  io.WriteCloser
	stdout io.Reader
}

func NewNode(addr address) *Node {
	return &Node{
		address: addr,
	}
}

func (node *Node) String() string {
	return node.address.String()
}

func (node *Node) Lock(filename string) error {
	lockCommandLine := []string{
		"sh", "-c", fmt.Sprintf(
			`flock -nx %s -c 'printf "%s\n" && cat' || printf "%s\n"`,
			filename, lockAcquiredString, lockLockedString,
		),
	}

	logMutex := &sync.Mutex{}

	traceln(hierr.Errorf(
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
		prefixwriter.New(
			newDebugWriter(logger),
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

	case lockLockedString:
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
			node, lockAcquiredString, lockLockedString,
			line,
		)
	}

	tracef(`lock acquired: '%s' on '%s'`, node, filename)

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
			tracef("%s closing connection", node.String())
			err := remote.CloseConnection()
			if err != nil {
				warningf(
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
			errorf(
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
			errorf(
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
			errorf(
				`%s received unexpected heartbeat ping: '%s'`,
				node.String(),
				ping,
			)

			finish(2)
		}

		tracef(`%s heartbeat`, node.String())
	}
}

func (node *Node) Connect(runnerFactory runnerFactory) error {
	tracef(`connecting to address: '%s'`, node.address)

	done := make(chan struct{}, 0)

	go func() {
		select {
		case <-done:
			return

		case <-time.After(longConnectionWarningTimeout):
			warningf(
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
