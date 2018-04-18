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

func (writer *DebugWriteCloser) Write(data []byte) (int, error) {
	writer.log.Debug(strings.TrimSuffix(string(data), "\n"))

	return len(data), nil
}

func (writer *DebugWriteCloser) Close() error {
	return nil
}
