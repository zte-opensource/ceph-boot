package remote

import (
	"sync"
	"time"
)

type Config struct {
	Pool         *ThreadPool
	Addresses    []Address
	LockFile     string
	HbInterval   time.Duration
	HbCancelCond *sync.Cond
}
