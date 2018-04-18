package writer

import (
	"encoding/json"
	"io"
)

type JsonWriter struct {
	stream string
	node   string
	backend io.Writer
}

func NewJsonWriter(stream string, node string, backendWriter io.Writer) *JsonWriter {
	return &JsonWriter{
		stream: stream,
		node: node,
		backend: backendWriter,
	}
}

func (writer *JsonWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	message := map[string]interface{}{
		"stream": writer.stream,
	}

	if writer.node == "" {
		message["node"] = nil
	} else {
		message["node"] = writer.node
	}

	message["body"] = string(data)

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return 0, err
	}

	_, err = writer.backend.Write(append(jsonMessage, '\n'))
	if err != nil {
		return 0, err
	}

	return len(data), nil
}
