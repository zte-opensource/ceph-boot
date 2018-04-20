package main

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reconquest/hierr-go"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/writer"
	"reflect"
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
	stdin  io.WriteCloser
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

	log.SetStatus(stat)

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
						log.Warningln(err)
					} else {
						errors <- err
						return
					}
				} else {
					if !noLock {
						err = node.Lock(lockFile)
						if err != nil {
							if noLockFail {
								log.Warningln(err)
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

				log.Debugf(
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
	serial bool,
) error {
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

	log.SetStatus(stat)

	type nodeErr struct {
		err  error
		node *Node
	}

	errors := make(chan *nodeErr, 0)
	for _, node := range cluster.nodes {
		go func(node *Node) {
			pool.run(func() {
				log.Tracef(
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
			return hierr.Errorf(
				err.err,
				`%s remote execution failed`,
				err.node,
			)
		}
	}

	cluster.stdin = writer.NewMultiWriteCloser(stdins)

	return nil
}

func (cluster *Cluster) Wait() error {
	log.Tracef(`waiting %d nodes to finish`, len(cluster.nodes))

	results := make(chan *RemoteCommandResult, 0)
	for _, node := range cluster.nodes {
		go func(rc *RemoteCommand) {
			results <- &RemoteCommandResult{rc, rc.Wait()}
		}(node.remoteCommand)
	}

	executionErrors := fmt.Errorf(
		`commands are exited with non-zero code`,
	)

	var (
		stat = &struct {
			Phase   string
			Total   int
			Fails   int
			Success int
		}{
			Phase: `wait`,
			Total: len(cluster.nodes),
		}

		exitCodes = map[int]int{}
	)

	log.SetStatus(stat)

	for range cluster.nodes {
		result := <-results
		if result.err != nil {
			exitCodes[result.rc.exitCode]++

			executionErrors = hierr.Push(
				executionErrors,
				hierr.Errorf(
					result.err,
					`%s has finished with error`,
					result.rc.node.String(),
				),
			)

			stat.Fails++
			stat.Total--

			log.Tracef(
				`%s finished with exit code: '%d'`,
				result.rc.node.String(),
				result.rc.exitCode,
			)

			continue
		}

		stat.Success++

		log.Tracef(
			`%s has successfully finished execution`,
			result.rc.node.String(),
		)
	}

	if stat.Fails > 0 {
		if stat.Fails == len(cluster.nodes) {
			exitCodesValue := reflect.ValueOf(exitCodes)

			topError := fmt.Errorf(
				`commands are failed on all %d nodes`,
				len(cluster.nodes),
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
			stat.Fails,
			len(cluster.nodes),
		)
	}

	return nil
}
