package writer

import (
	"encoding/json"
	"io"
)

type JsonWriter struct {
	writer io.Writer
	stream string
	node   string
}

func NewJsonWriter(stream string, node string, w io.Writer) *JsonWriter {
	return &JsonWriter{
		writer: w,
		stream: stream,
		node:   node,
	}
}

func (w *JsonWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	message := map[string]interface{}{
		"stream": w.stream,
	}

	if w.node == "" {
		message["node"] = nil
	} else {
		message["node"] = w.node
	}

	message["body"] = string(data)

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return 0, err
	}

	_, err = w.writer.Write(append(jsonMessage, '\n'))
	if err != nil {
		return 0, err
	}

	return len(data), nil
}
