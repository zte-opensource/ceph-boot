package writer

import (
	"io"
)

type NopWriteCloser struct {
	io.Writer
}

func NewNopWriteCloser(w io.Writer) *NopWriteCloser {
	return &NopWriteCloser{
		w,
	}
}

func (w *NopWriteCloser) Close() error {
	return nil
}
