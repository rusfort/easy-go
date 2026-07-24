package eg

import (
	"sync"
	"time"
)

type QueueObject struct {
	operation func() (any, error)
	out       chan any
	errOut    chan error
}

type Queue struct {
	mtx           *sync.Mutex
	timeoutMillis int64

	lastRequestByInstance map[string]int64
	queuesByInstance      map[string]chan *QueueObject
	terminatorsByInstance map[string]chan struct{}
}

func NewRateLimitedQueue(timeoutMillis int64) *Queue {
	q := &Queue{
		timeoutMillis:         timeoutMillis,
		mtx:                   &sync.Mutex{},
		lastRequestByInstance: make(map[string]int64),
		queuesByInstance:      make(map[string]chan *QueueObject),
		terminatorsByInstance: make(map[string]chan struct{}),
	}

	return q
}

func workQueue(q *Queue, instance string, terminator <-chan struct{}) {
	for {
		now := time.Now().UnixMilli()

		q.mtx.Lock()
		lr, ok := q.lastRequestByInstance[instance]
		in, inOk := q.queuesByInstance[instance]
		q.mtx.Unlock()

		if ok {
			if q.timeoutMillis > now-lr {
				time.Sleep(time.Duration(q.timeoutMillis - (now - lr)))
			}
		}

		if !inOk {
			continue
		}

		select {
		case <-terminator:
			{
				return
			}
		case o := <-in:
			{
				q.mtx.Lock()
				q.lastRequestByInstance[instance] = time.Now().UnixMilli()
				q.mtx.Unlock()

				go func() {
					result, err := o.operation()
					o.out <- result
					o.errOut <- err
				}()
				continue
			}
		}
	}
}

func PushToQueue[R any](q *Queue, instance string, operation func() (any, error), out chan R, errOut chan error) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	terminator, ok := q.terminatorsByInstance[instance]
	if !ok {
		terminator = make(chan struct{})
		q.terminatorsByInstance[instance] = terminator
	}

	queue, ok := q.queuesByInstance[instance]
	if !ok {
		queue = make(chan *QueueObject)
		q.queuesByInstance[instance] = queue
		go workQueue(q, instance, terminator)
	}

	result := make(chan any)
	go func() {
		queue <- &QueueObject{
			operation: operation,
			out:       result,
			errOut:    errOut,
		}

		res, ok := (<-result).(R)
		if !ok {
			var empty R
			out <- empty
		} else {
			out <- res
		}
	}()
}

func (q *Queue) TerminateQueue(instance string) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	terminator, ok := q.terminatorsByInstance[instance]
	if ok {
		terminator <- struct{}{}
	}

	_, ok = q.queuesByInstance[instance]
	if ok {
		delete(q.queuesByInstance, instance)
	}

	_, ok = q.lastRequestByInstance[instance]
	if ok {
		delete(q.lastRequestByInstance, instance)
	}
}
