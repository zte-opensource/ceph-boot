package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reconquest/hierr-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/prefixwriter-go"
	"github.com/zte-opensource/runcmd"
)

const (
	longConnectionWarningTimeout = 2 * time.Second
	lockAcquiredString = `acquired`
	lockLockedString   = `locked`
)

type Cluster struct {
	nodes []*Node
}

type Node struct {
	address address
	runner  runcmd.Runner
	session *CommandSession

	hbIO *HeartbeatIO
}

type HeartbeatIO struct {
	stdin  io.WriteCloser
	stdout io.Reader
}

func (node *Node) String() string {
	return node.address.String()
}

func (node *Node) lock(filename string) error {
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

	node.hbIO = &HeartbeatIO{
		stdin:  stdin,
		stdout: stdout,
	}

	return nil
}

// connectToCluster tries to acquire atomic file lock on each of
// specified remote nodes. lockFile is used to specify target lock file, it
// must exist on every node. runnerFactory will be used to make connection
// to remote node. If noLockFail is given, then only warning will be printed
// if lock process has been failed.
func connectToCluster(
	lockFile string,
	runnerFactory runnerFactory,
	addresses []address,
	noLock bool,
	noLockFail bool,
	noConnFail bool,
	heartbeat func(*Node),
) (*Cluster, error) {
	var (
		cluster = &Cluster{}

		errors = make(chan error, 0)

		nodeAddMutex = &sync.Mutex{}
	)

	status := &struct {
		Phase   string
		Total   int64
		Fails   int64
		Success int64
	}{
		Phase: `lock`,
		Total: int64(len(addresses)),
	}

	if noLock {
		status.Phase = `connect`
	}

	setStatus(status)

	for _, nodeAddress := range addresses {
		go func(nodeAddress address) {
			pool.run(func() {
				failed := false

				node, err := connectToNode(runnerFactory, nodeAddress)
				if err != nil {
					atomic.AddInt64(&status.Fails, 1)
					atomic.AddInt64(&status.Total, -1)

					if noConnFail {
						failed = true
						warningln(err)
					} else {
						errors <- err
						return
					}
				} else {
					if !noLock {
						err = node.lock(lockFile)
						if err != nil {
							if noLockFail {
								warningln(err)
							} else {
								errors <- err
								return
							}
						} else {
							go heartbeat(node)
						}
					}
				}

				textStatus := "established"
				if failed {
					textStatus = "failed"
				} else {
					atomic.AddInt64(&status.Success, 1)

					nodeAddMutex.Lock()
					defer nodeAddMutex.Unlock()

					cluster.nodes = append(cluster.nodes, node)
				}

				debugf(
					`%4d/%d (%d failed) connection %s: %s`,
					status.Success,
					status.Total,
					status.Fails,
					textStatus,
					nodeAddress,
				)

				errors <- nil
			})
		}(nodeAddress)
	}

	erronous := 0
	topError := hierr.Push(`can't connect to nodes`)
	for range addresses {
		err := <-errors
		if err != nil {
			erronous++

			topError = hierr.Push(topError, err)
		}
	}

	if erronous > 0 {
		return nil, hierr.Push(
			fmt.Errorf(
				`connection to %d of %d nodes failed`,
				erronous,
				len(addresses),
			),
			topError,
		)
	}

	return cluster, nil
}

func connectToNode(
	runnerFactory runnerFactory,
	address address,
) (*Node, error) {
	tracef(`connecting to address: '%s'`, address)

	done := make(chan struct{}, 0)

	go func() {
		select {
		case <-done:
			return

		case <-time.After(longConnectionWarningTimeout):
			warningf(
				"still connecting to address after %s: %s",
				longConnectionWarningTimeout,
				address,
			)

			<-done
		}
	}()

	defer func() {
		done <- struct{}{}
	}()

	// to establish a ssh connection and return a instance of runcmd.Runner, which
	// can be used to start remote execution sessions
	runner, err := runnerFactory(address)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't connect to address: %s`,
			address,
		)
	}

	return &Node{
		address: address,
		runner:  runner,
	}, nil
}
