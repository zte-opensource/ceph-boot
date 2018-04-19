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

func (l *SharedLock) Lock() {
	l.holder.Lock()
	defer l.holder.Unlock()

	if !l.holder.locked {
		l.Lock()

		l.holder.locked = true
	}
}

func (l *SharedLock) Unlock() {
	l.holder.Lock()
	defer l.holder.Unlock()

	l.holder.clients--

	if l.holder.clients == 0 && l.holder.locked {
		l.Unlock()

		l.holder.locked = false
	}
}

type LockedWriter struct {
	io.WriteCloser
	sync.Locker
}

func NewLockedWriter(
	w io.WriteCloser,
	lock sync.Locker,
) *LockedWriter {
	return &LockedWriter{
		WriteCloser: w,
		Locker:      lock,
	}
}

func (w *LockedWriter) Write(data []byte) (int, error) {
	w.Lock()

	return w.Write(data)
}

func (w *LockedWriter) Close() error {
	w.Unlock()

	return w.Close()
}
