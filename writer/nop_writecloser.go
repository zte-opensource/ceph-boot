package writer

import (
	"io"
)

type NopWriteCloser struct {
	io.Writer
}

func NewNopWriteCloser(writer io.Writer) *NopWriteCloser {
	return &NopWriteCloser{
		writer,
	}
}

func (writer *NopWriteCloser) Close() error {
	return nil
}
