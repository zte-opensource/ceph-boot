package main

import (
	"fmt"
	"github.com/reconquest/hierr-go"
	"sync"
	"sync/atomic"
)

type Cluster struct {
	nodes []*Node
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
