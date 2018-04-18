package main

import "io"

type statusBarUpdateWriter struct {
	writer io.WriteCloser
}

func (writer *statusBarUpdateWriter) Write(data []byte) (int, error) {
	ClearStatus()

	written, err := writer.writer.Write(data)

	DrawStatus()

	return written, err
}

func (writer *statusBarUpdateWriter) Close() error {
	return writer.writer.Close()
}
