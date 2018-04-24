package remote

import (
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/writer"
)

type Cluster struct {
	config Config

	Nodes []*Node
	Stdin io.WriteCloser

	executions []*CommandExecution
}

func NewCluster(config Config) *Cluster {
	return &Cluster{config: config}
}

// Connect tries to acquire atomic file lock on each of
// specified remote nodes. lockFile is used to specify target lock file, it
// must exist on every node. runnerFactory will be used to make connection
// to remote node. If noLockFail is given, then only warning will be printed
// if lock process has been failed.
func (cluster *Cluster) Connect(
	runnerFactory RunnerFactory,
) error {
	config := cluster.config

	pool := config.Pool
	addresses := config.Addresses
	lockFile := config.LockFile
	noLock := config.NoLock
	noLockFail := config.NoLockFail
	noConnFail := config.NoConnFail
	hbInterval := config.HbInterval
	hbCancelCond := config.HbCancelCond

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
		go func(nodeAddress Address) {
			pool.Run(func() {
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

					cluster.Nodes = append(cluster.Nodes, node)
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
	c *Command,
	serial bool,
) error {
	var (
		stdins []io.WriteCloser

		logLock    = &sync.Mutex{}
		stdinsLock = &sync.Mutex{}
		outputLock = &sync.Mutex{}
		cmdsLock   = &sync.Mutex{}

		pool = cluster.config.Pool

		stat = &struct {
			sync.Mutex

			Phase   string
			Total   int
			Fails   int
			Success int
		}{
			Phase: `exec`,
			Total: len(cluster.Nodes),
		}
	)

	log.SetStatus(stat)

	if !serial {
		outputLock = nil
	}

	type nodeErr struct {
		err  error
		node *Node
	}

	errors := make(chan *nodeErr, 0)

	for _, node := range cluster.Nodes {
		go func(node *Node) {
			pool.Run(func() {
				log.Debugf(
					"%s",
					hierr.Errorf(
						c.EscapedCommand,
						"%s starting command",
						node.String(),
					).Error(),
				)

				e, err := node.Run(c, logLock, outputLock)
				if err != nil {
					errors <- &nodeErr{err, node}

					stat.Lock()
					defer stat.Unlock()

					stat.Total--
					stat.Fails++

					return
				}

				cmdsLock.Lock()
				defer cmdsLock.Unlock()

				cluster.executions = append(cluster.executions, e)

				stdinsLock.Lock()
				defer stdinsLock.Unlock()

				stdins = append(stdins, e.Stdin)

				stat.Lock()
				defer stat.Unlock()

				stat.Success++

				errors <- nil
			})
		}(node)
	}

	for range cluster.Nodes {
		err := <-errors
		if err != nil {
			return hierr.Errorf(
				err.err,
				`%s remote execution failed`,
				err.node,
			)
		}
	}

	cluster.Stdin = writer.NewMultiWriteCloser(stdins)

	return nil
}

func (cluster *Cluster) Wait() error {
	log.Debugf(`waiting %d nodes to finish`, len(cluster.Nodes))

	results := make(chan *CommandResult, 0)
	for _, e := range cluster.executions {
		go func(e *CommandExecution) {
			results <- &CommandResult{e, e.Wait()}
		}(e)
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
			Total: len(cluster.Nodes),
		}

		exitCodes = map[int]int{}
	)

	log.SetStatus(stat)

	for range cluster.Nodes {
		result := <-results
		if result.err != nil {
			exitCodes[result.execution.ExitCode]++

			executionErrors = hierr.Push(
				executionErrors,
				hierr.Errorf(
					result.err,
					`%s has finished with error`,
					result.execution.Node.String(),
				),
			)

			stat.Fails++
			stat.Total--

			log.Debugf(
				`%s finished with exit code: '%d'`,
				result.execution.Node.String(),
				result.execution.ExitCode,
			)

			continue
		}

		stat.Success++

		log.Debugf(
			`%s has successfully finished execution`,
			result.execution.Node.String(),
		)
	}

	if stat.Fails > 0 {
		if stat.Fails == len(cluster.Nodes) {
			exitCodesValue := reflect.ValueOf(exitCodes)

			topError := fmt.Errorf(
				`commands are failed on all %d nodes`,
				len(cluster.Nodes),
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
			len(cluster.Nodes),
		)
	}

	return nil
}
