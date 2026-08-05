package eg

import (
	"sync"
	"time"
)

type QueueObject struct {
	operation func() (any, error)
	out       *Chan[any]
	errOut    *Chan[error]
}

type Queue struct {
	mtx           *sync.Mutex
	timeoutMillis int64

	lastRequestByInstance map[string]int64
	queuesByInstance      map[string]*Chan[*QueueObject]
	terminatorsByInstance map[string]*Chan[struct{}]
}

func NewRateLimitedQueue(timeoutMillis int64) *Queue {
	q := &Queue{
		timeoutMillis:         timeoutMillis,
		mtx:                   &sync.Mutex{},
		lastRequestByInstance: make(map[string]int64),
		queuesByInstance:      make(map[string]*Chan[*QueueObject]),
		terminatorsByInstance: make(map[string]*Chan[struct{}]),
	}

	return q
}

func workQueue(q *Queue, instance string, terminator *Chan[struct{}]) {
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

		ChanSelect(Selector{
			terminator: Returner,
			in: func(qo any) {
				q.mtx.Lock()
				q.lastRequestByInstance[instance] = time.Now().UnixMilli()
				q.mtx.Unlock()

				o, ok := qo.(*QueueObject)
				if !ok {
					return
				}

				go func() {
					result, err := o.operation()
					o.out.Write(result)
					o.errOut.Write(err)
				}()
			},
		})
	}
}

func PushToQueue[R any](q *Queue, instance string, operation func() (any, error), out *Chan[R], errOut *Chan[error]) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	terminator, ok := q.terminatorsByInstance[instance]
	if !ok {
		terminator = NewChan[struct{}]()
		q.terminatorsByInstance[instance] = terminator
	}

	queue, ok := q.queuesByInstance[instance]
	if !ok {
		queue = NewChan[*QueueObject]()
		q.queuesByInstance[instance] = queue
		go workQueue(q, instance, terminator)
	}

	result := NewChan[any]()
	go func() {
		queue.Write(&QueueObject{
			operation: operation,
			out:       result,
			errOut:    errOut,
		})

		res, ok := (result.Read()).(R)
		if !ok {
			var empty R
			out.Write(empty)
		} else {
			out.Write(res)
		}
	}()
}

func (q *Queue) TerminateQueue(instance string) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	terminator, ok := q.terminatorsByInstance[instance]
	if ok {
		terminator.Write(struct{}{})
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
