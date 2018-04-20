package writer

import (
	"io"
	"sync"
)

type SharedLock struct {
	lock sync.Locker

	holder *struct {
		sync.Locker

		clients int
		locked  bool
	}
}

func NewSharedLock(l sync.Locker, clients int) *SharedLock {
	return &SharedLock{
		lock: l,

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
		l.lock.Lock()

		l.holder.locked = true
	}
}

func (l *SharedLock) Unlock() {
	l.holder.Lock()
	defer l.holder.Unlock()

	l.holder.clients--

	if l.holder.clients == 0 && l.holder.locked {
		l.lock.Unlock()

		l.holder.locked = false
	}
}

type LockedWriter struct {
	writer io.WriteCloser
	sync.Locker
}

func NewLockedWriter(w io.WriteCloser, l sync.Locker) *LockedWriter {
	return &LockedWriter{
		writer: w,
		Locker: l,
	}
}

func (w *LockedWriter) Write(data []byte) (int, error) {
	w.Lock()

	return w.writer.Write(data)
}

func (w *LockedWriter) Close() error {
	w.Unlock()

	return w.writer.Close()
}
