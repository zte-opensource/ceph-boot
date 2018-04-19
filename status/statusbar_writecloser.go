package status

import (
	"io"
)

type StatusBarWriteCloser struct {
	writer io.WriteCloser
}

func NewStatusBarWriteCloser(writer io.WriteCloser) *StatusBarWriteCloser {
	return &StatusBarWriteCloser{
		writer: writer,
	}
}

func (writer *StatusBarWriteCloser) Write(data []byte) (int, error) {
	ClearStatus()

	written, err := writer.writer.Write(data)

	DrawStatus()

	return written, err
}

func (writer *StatusBarWriteCloser) Close() error {
	return writer.writer.Close()
}
