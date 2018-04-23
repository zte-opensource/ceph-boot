package remote

type ThreadPool struct {
	available chan struct{}

	Size int
}

func NewThreadPool(size int) *ThreadPool {
	available := make(chan struct{}, size)
	for i := 0; i < size; i++ {
		available <- struct{}{}
	}

	return &ThreadPool{
		available,
		size,
	}
}

func (pool *ThreadPool) Run(task func()) {
	<-pool.available

	defer func() {
		pool.available <- struct{}{}
	}()

	task()
}
