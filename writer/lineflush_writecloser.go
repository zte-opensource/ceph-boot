package writer

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"
)

// LineFlushWriteCloser implements writer, that will proxy to specified `backend` writer only
// complete lines, e.g. that ends in newline. This writer is thread-safe.
type LineFlushWriteCloser struct {
	sync.Locker
	backend io.WriteCloser
	buffer  *bytes.Buffer

	ensureNewline bool
}

// NewLineFlushWriteCloser returns new NewLineFlushWriteCloser, that will proxy data to the `backend` writer,
// thread-safety is guaranteed via `lock`. Optionally, writer can ensure, that
// last line of output ends with newline, if `ensureNewline` is true.
func NewLineFlushWriteCloser(
	w io.WriteCloser,
	l sync.Locker,
	ensureNewline bool,
) *LineFlushWriteCloser {
	return &LineFlushWriteCloser{
		Locker:  l,
		backend: w,
		buffer:  &bytes.Buffer{},

		ensureNewline: ensureNewline,
	}
}

// Writer writes data into NewLineFlushWriteCloser.
//
// Signature matches with io.Writer's Write().
func (w *LineFlushWriteCloser) Write(data []byte) (int, error) {
	w.Lock()
	written, err := w.buffer.Write(data)
	w.Unlock()
	if err != nil {
		return written, err
	}

	var (
		reader = bufio.NewReader(w.buffer)

		eofEncountered = false
	)

	for !eofEncountered {
		w.Lock()
		line, err := reader.ReadString('\n')

		if err != nil {
			if err != io.EOF {
				w.Unlock()
				return 0, err
			}

			eofEncountered = true
		}

		var target io.Writer
		if eofEncountered {
			target = w.buffer
		} else {
			target = w.backend
		}

		_, err = io.WriteString(target, line)

		w.Unlock()
		if err != nil {
			return 0, err
		}
	}

	return written, nil
}

// Close flushes all remaining data and closes underlying backend writer.
// If `ensureNewLine` was specified and remaining data does not ends with
// newline, then newline will be added.
//
// Signature matches with io.WriteCloser's Close().
func (w *LineFlushWriteCloser) Close() error {
	if w.ensureNewline && w.buffer.Len() > 0 {
		if !strings.HasSuffix(w.buffer.String(), "\n") {
			_, err := w.buffer.WriteString("\n")
			if err != nil {
				return err
			}
		}
	}

	if w.buffer.Len() > 0 {
		_, err := w.backend.Write(w.buffer.Bytes())
		if err != nil {
			return err
		}
	}

	return w.backend.Close()
}
