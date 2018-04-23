package remote

import (
	"sync"
	"time"
)

type Config struct {
	Pool         *ThreadPool
	Addresses    []Address
	LockFile     string
	NoLock       bool
	NoLockFail   bool
	NoConnFail   bool
	HbInterval   time.Duration
	HbCancelCond *sync.Cond
}
