package main

type (
	CallbackWriter func([]byte) (int, error)
)

func (writer CallbackWriter) Write(data []byte) (int, error) {
	return writer(data)
}
