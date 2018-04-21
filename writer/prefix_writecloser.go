package writer

import (
	"bytes"
	"io"
)

// Writer writes specified data, prepending prefix to each new line.
type PrefixWriteCloser struct {
	backend io.WriteCloser
	prefix  string

	streamStarted  bool
	lineIncomplete bool
}

// New creates new Writer, that will use `writer` as backend
// and will prepend `prefix` to each line.
func NewPrefixWriteCloser(w io.WriteCloser, prefix string) *PrefixWriteCloser {
	return &PrefixWriteCloser{
		backend: w,
		prefix:  prefix,
	}
}

// Writer writes data into Writer.
//
// Signature matches with io.Writer's Write().
func (w *PrefixWriteCloser) Write(data []byte) (int, error) {
	var (
		reader         = bytes.NewBuffer(data)
		eofEncountered = false
	)

	for !eofEncountered {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return 0, err
			}

			eofEncountered = true
		}

		if line == "" {
			continue
		}

		if !w.streamStarted || !w.lineIncomplete {
			line = w.prefix + line

			w.streamStarted = true
		}

		w.lineIncomplete = eofEncountered

		_, err = w.backend.Write([]byte(line))
		if err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

// Close closes underlying backend writer.
//
// Signature matches with io.WriteCloser's Close().
func (w *PrefixWriteCloser) Close() error {
	return w.backend.Close()
}
