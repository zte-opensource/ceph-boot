package writer

import (
	"fmt"
	"io"
	"strings"
)

type MultiWriteCloser struct {
	writers []io.WriteCloser
}

func NewMultiWriteCloser(writers []io.WriteCloser) *MultiWriteCloser {
	return &MultiWriteCloser{
		writers,
	}
}

func (m *MultiWriteCloser) Write(data []byte) (int, error) {
	var errs []string

	for _, writer := range m.writers {
		_, err := writer.Write(data)
		if err != nil && err != io.EOF {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return 0, fmt.Errorf(
			"%d errors: %s",
			len(errs),
			strings.Join(errs, "; "),
		)
	}

	return len(data), nil
}

func (m *MultiWriteCloser) Close() error {
	var errs []string

	for _, closer := range m.writers {
		err := closer.Close()
		if err != nil && err != io.EOF {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"%d errors: %s",
			len(errs),
			strings.Join(errs, "; "),
		)
	}

	return nil
}
