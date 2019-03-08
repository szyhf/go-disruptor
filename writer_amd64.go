package disruptor

import (
	"sync/atomic"
)

func (this *Writer) Commit(lower, upper int64) {
	atomic.StoreInt64(&this.written.sequence, upper)
}
