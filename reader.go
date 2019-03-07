package disruptor

import (
	"runtime"
	"sync/atomic"
	"time"
)

type Reader struct {
	read     *Cursor
	written  *Cursor
	upstream Barrier
	consumer Consumer
	ready    int32
}

func NewReader(read, written *Cursor, upstream Barrier, consumer Consumer) *Reader {
	return &Reader{
		read:     read,
		written:  written,
		upstream: upstream,
		consumer: consumer,
		ready:    0,
	}
}

func (this *Reader) Start() {
	atomic.StoreInt32(&this.ready, 1)
	go this.receive()
}
func (this *Reader) Stop() {
	atomic.StoreInt32(&this.ready, 0)
}

func (this *Reader) receive() {
	previous := this.read.Load()
	idling, gating := 0, 0

	for {
		lower := previous + 1
		upper := this.upstream.Read(lower)

		if lower <= upper {
			this.consumer.Consume(lower, upper)
			this.read.Store(upper)
			previous = upper
		} else if upper = this.written.Load(); lower <= upper {
			time.Sleep(time.Microsecond)
			// Gating--TODO: wait strategy (provide gating count to wait strategy for phased backoff)
			gating++
			idling = 0
		} else if atomic.LoadInt32(&this.ready) == 1 {
			time.Sleep(time.Millisecond)
			// Idling--TODO: wait strategy (provide idling count to wait strategy for phased backoff)
			idling++
			gating = 0
		} else {
			break
		}

		// sleeping increases the batch size which reduces number of writes required to store the sequence
		// reducing the number of writes allows the CPU to optimize the pipeline without prediction failures
		runtime.Gosched() // LockSupport.parkNanos(1L); http://bit.ly/1xiDINZ
	}
}
