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

	Execution []*Command
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

		pool = cluster.config.Pool
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
			Total: len(cluster.Nodes),
		}
	)

	log.SetStatus(stat)

	type nodeErr struct {
		err  error
		node *Node
	}

	errors := make(chan *nodeErr, 0)
	executionLock := &sync.Mutex{}

	for _, node := range cluster.Nodes {
		go func(node *Node) {
			pool.Run(func() {
				log.Tracef(
					"%s",
					hierr.Errorf(
						c.EscapedCommand,
						"%s starting command",
						node.String(),
					).Error(),
				)

				prefix := ""
				if log.Conf.Verbose != log.VerbosityQuiet {
					prefix = node.address.Domain + " "
				}

				err := c.Run(node, prefix, logLock, outputLock)
				if err != nil {
					errors <- &nodeErr{err, node}

					stat.Lock()
					defer stat.Unlock()

					stat.Total--
					stat.Fails++

					return
				}

				executionLock.Lock()
				defer executionLock.Unlock()

				cluster.Execution = append(cluster.Execution, c)

				stdinsLock.Lock()
				defer stdinsLock.Unlock()

				stdins = append(stdins, c.Stdin)

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
	log.Tracef(`waiting %d nodes to finish`, len(cluster.Execution))

	results := make(chan *CommandResult, 0)
	for _, c := range cluster.Execution {
		go func(c *Command) {
			results <- &CommandResult{c, c.Wait()}
		}(c)
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
			Total: len(cluster.Execution),
		}

		exitCodes = map[int]int{}
	)

	log.SetStatus(stat)

	for range cluster.Execution {
		result := <-results
		if result.Err != nil {
			exitCodes[result.C.ExitCode]++

			executionErrors = hierr.Push(
				executionErrors,
				hierr.Errorf(
					result.Err,
					`%s has finished with error`,
					result.C.Node,
				),
			)

			stat.Fails++
			stat.Total--

			log.Tracef(
				`%s finished with exit code: '%d'`,
				result.C.Node,
				result.C.ExitCode,
			)

			continue
		}

		stat.Success++

		log.Tracef(
			`%s has successfully finished execution`,
			result.C.Node,
		)
	}

	if stat.Fails > 0 {
		if stat.Fails == len(cluster.Execution) {
			exitCodesValue := reflect.ValueOf(exitCodes)

			topError := fmt.Errorf(
				`commands are failed on all %d nodes`,
				len(cluster.Execution),
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
			len(cluster.Execution),
		)
	}

	return nil
}
