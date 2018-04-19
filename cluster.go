package main

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reconquest/hierr-go"
	"github.com/zte-opensource/ceph-boot/status"
	"github.com/zte-opensource/ceph-boot/writer"
)

type ClusterConfig struct {
	addresses    []address
	lockFile     string
	noLock       bool
	noLockFail   bool
	noConnFail   bool
	hbInterval   time.Duration
	hbCancelCond *sync.Cond
}

type Cluster struct {
	config ClusterConfig
	nodes  []*Node
}

func NewCluster(config *ClusterConfig) *Cluster {
	return &Cluster{config: *config}
}

// connectToCluster tries to acquire atomic file lock on each of
// specified remote nodes. lockFile is used to specify target lock file, it
// must exist on every node. runnerFactory will be used to make connection
// to remote node. If noLockFail is given, then only warning will be printed
// if lock process has been failed.
func (cluster *Cluster) Connect(
	runnerFactory runnerFactory,
) error {
	config := cluster.config

	addresses := config.addresses
	lockFile := config.lockFile
	noLock := config.noLock
	noLockFail := config.noLockFail
	noConnFail := config.noConnFail
	hbInterval := config.hbInterval
	hbCancelCond := config.hbCancelCond

	errors := make(chan error, 0)
	nodeAddMutex := &sync.Mutex{}

	stat := &struct {
		Phase   string
		Total   int64
		Fails   int64
		Success int64
	}{
		Phase: `lock`,
		Total: int64(len(addresses)),
	}

	if noLock {
		stat.Phase = `connect`
	}

	status.SetStatus(stat)

	for _, nodeAddress := range addresses {
		go func(nodeAddress address) {
			pool.run(func() {
				failed := false

				node := NewNode(nodeAddress)

				err := node.Connect(runnerFactory)
				if err != nil {
					atomic.AddInt64(&stat.Fails, 1)
					atomic.AddInt64(&stat.Total, -1)

					if noConnFail {
						failed = true
						status.Warningln(err)
					} else {
						errors <- err
						return
					}
				} else {
					if !noLock {
						err = node.Lock(lockFile)
						if err != nil {
							if noLockFail {
								status.Warningln(err)
							} else {
								errors <- err
								return
							}
						} else {
							go node.Heartbeat(hbInterval, hbCancelCond)
						}
					}
				}

				textStatus := "established"
				if failed {
					textStatus = "failed"
				} else {
					atomic.AddInt64(&stat.Success, 1)

					nodeAddMutex.Lock()
					defer nodeAddMutex.Unlock()

					cluster.nodes = append(cluster.nodes, node)
				}

				status.Debugf(
					`%4d/%d (%d failed) connection %s: %s`,
					stat.Success,
					stat.Total,
					stat.Fails,
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
		return hierr.Push(
			fmt.Errorf(
				`connection to %d of %d nodes failed`,
				erronous,
				len(addresses),
			),
			topError,
		)
	}

	return nil
}

func (cluster *Cluster) RunCommand(
	command []string,
	setupCallback func(*RemoteCommand),
	serial bool,
) (*RemoteExecution, error) {
	var (
		stdins []io.WriteCloser

		logLock    = &sync.Mutex{}
		stdinsLock = &sync.Mutex{}
		outputLock = &sync.Mutex{}
	)

	if !serial {
		outputLock = nil
	}

	var (
		stat = &struct {
			sync.Mutex

			Phase   string
			Total   int
			Fails   int
			Success int
		}{
			Phase: `exec`,
			Total: len(cluster.nodes),
		}
	)

	status.SetStatus(stat)

	type nodeErr struct {
		err  error
		node *Node
	}

	errors := make(chan *nodeErr, 0)
	for _, node := range cluster.nodes {
		go func(node *Node) {
			pool.run(func() {
				status.Tracef(
					"%s",
					hierr.Errorf(
						command,
						"%s starting command",
						node.String(),
					).Error(),
				)

				// create runcmd.CmdWorker to prepare running command on remote node
				remoteCommand, err := node.CreateRemoteCommand(
					command,
					logLock,
					outputLock,
				)
				if err != nil {
					errors <- &nodeErr{err, node}

					stat.Lock()
					defer stat.Unlock()

					stat.Total--
					stat.Fails++

					return
				}

				if setupCallback != nil {
					setupCallback(remoteCommand)
				}

				remoteCommand.worker.SetStdout(remoteCommand.stdout)
				remoteCommand.worker.SetStderr(remoteCommand.stderr)

				// run command on remote node
				err = remoteCommand.worker.Start()
				if err != nil {
					errors <- &nodeErr{
						hierr.Errorf(
							err,
							`can't start remote command`,
						),
						node,
					}

					stat.Lock()
					defer stat.Unlock()

					stat.Total--
					stat.Fails++

					return
				}

				node.remoteCommand = remoteCommand

				stdinsLock.Lock()
				defer stdinsLock.Unlock()

				stdins = append(stdins, remoteCommand.stdin)

				stat.Lock()
				defer stat.Unlock()

				stat.Success++

				errors <- nil
			})
		}(node)
	}

	for range cluster.nodes {
		err := <-errors
		if err != nil {
			return nil, hierr.Errorf(
				err.err,
				`%s remote execution failed`,
				err.node,
			)
		}
	}

	return &RemoteExecution{
		stdin: writer.NewMultiWriteCloser(stdins),
		nodes: cluster.nodes,
	}, nil
}
