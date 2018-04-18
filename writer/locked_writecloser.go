package writer

import (
	"io"
	"sync"
)

type SharedLock struct {
	sync.Locker

	holder *struct {
		sync.Locker

		clients int
		locked  bool
	}
}

func NewSharedLock(lock sync.Locker, clients int) *SharedLock {
	return &SharedLock{
		Locker: lock,

		holder: &struct {
			sync.Locker

			clients int
			locked  bool
		}{
			Locker: &sync.Mutex{},

			clients: clients,
			locked:  false,
		},
	}
}

func (slock *SharedLock) Lock() {
	slock.holder.Lock()
	defer slock.holder.Unlock()

	if !slock.holder.locked {
		slock.Lock()

		slock.holder.locked = true
	}
}

func (slock *SharedLock) Unlock() {
	slock.holder.Lock()
	defer slock.holder.Unlock()

	slock.holder.clients--

	if slock.holder.clients == 0 && slock.holder.locked {
		slock.Unlock()

		slock.holder.locked = false
	}
}

type LockedWriter struct {
	sync.Locker

	writer io.WriteCloser
}

func NewLockedWriter(
	writer io.WriteCloser,
	lock sync.Locker,
) *LockedWriter {
	return &LockedWriter{
		Locker: lock,
		writer: writer,
	}
}

func (writer *LockedWriter) Write(data []byte) (int, error) {
	writer.Lock()

	return writer.writer.Write(data)
}

func (writer *LockedWriter) Close() error {
	writer.Unlock()

	return writer.writer.Close()
}
