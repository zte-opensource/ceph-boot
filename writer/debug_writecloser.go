package writer

import (
	"strings"

	"github.com/kovetskiy/lorg"
)

type DebugWriteCloser struct {
	log *lorg.Log
}

func NewDebugWriteCloser(log *lorg.Log) *DebugWriteCloser {
	return &DebugWriteCloser{
		log: log,
	}
}

func (w *DebugWriteCloser) Write(data []byte) (int, error) {
	w.log.Debug(strings.TrimSuffix(string(data), "\n"))

	return len(data), nil
}

func (w *DebugWriteCloser) Close() error {
	return nil
}
