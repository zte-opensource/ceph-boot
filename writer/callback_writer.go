package writer

type (
	CallbackWriter func([]byte) (int, error)
)

func (w CallbackWriter) Write(data []byte) (int, error) {
	return w(data)
}
