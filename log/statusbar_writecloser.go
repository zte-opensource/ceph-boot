package log

import (
	"io"
)

type StatusBarWriteCloser struct {
	io.WriteCloser
}

func NewStatusBarWriteCloser(writer io.WriteCloser) *StatusBarWriteCloser {
	return &StatusBarWriteCloser{
		writer,
	}
}

func (writer *StatusBarWriteCloser) Write(data []byte) (int, error) {
	ClearStatus()

	written, err := writer.Write(data)

	DrawStatus()

	return written, err
}

func (writer *StatusBarWriteCloser) Close() error {
	return writer.Close()
}
