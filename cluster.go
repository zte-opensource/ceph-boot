package main

import (
	"fmt"
	"github.com/reconquest/hierr-go"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterConfig struct {
	lockFile string
	noLock bool
	noLockFail bool
	noConnFail bool
	hbInterval time.Duration
	hbCancelCond *sync.Cond
}

type Cluster struct {
	config ClusterConfig
	nodes []*Node
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
	addresses []address,
) error {
	config := cluster.config

	lockFile := config.lockFile
	noLock := config.noLock
	noLockFail := config.noLockFail
	noConnFail := config.noConnFail
	hbInterval := config.hbInterval
	hbCancelCond := config.hbCancelCond

	errors := make(chan error, 0)
	nodeAddMutex := &sync.Mutex{}

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

				node := NewNode(nodeAddress)

				err := node.Connect(runnerFactory)
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
						err = node.Lock(lockFile)
						if err != nil {
							if noLockFail {
								warningln(err)
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

func (cluster *Cluster) RunCommand() {

}
