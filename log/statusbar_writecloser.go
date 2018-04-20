package log

import (
	"io"
)

type StatusBarWriteCloser struct {
	writer io.WriteCloser
}

func NewStatusBarWriteCloser(w io.WriteCloser) *StatusBarWriteCloser {
	return &StatusBarWriteCloser{
		w,
	}
}

func (w *StatusBarWriteCloser) Write(data []byte) (int, error) {
	ClearStatus()

	written, err := w.writer.Write(data)

	DrawStatus()

	return written, err
}

func (w *StatusBarWriteCloser) Close() error {
	return w.writer.Close()
}
