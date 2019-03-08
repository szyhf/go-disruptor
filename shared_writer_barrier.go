package disruptor

import (
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

type SharedWriterBarrier struct {
	written   *Cursor
	committed []int32
	commiting int32 // 自旋锁
	capacity  int64
	mask      int64
	shift     uint8
}

func NewSharedWriterBarrier(written *Cursor, capacity int64) *SharedWriterBarrier {
	assertPowerOfTwo(capacity)

	return &SharedWriterBarrier{
		written:   written,
		committed: prepareCommitBuffer(capacity),
		capacity:  capacity,
		mask:      capacity - 1,
		shift:     uint8(math.Log2(float64(capacity))),
	}
}
func prepareCommitBuffer(capacity int64) []int32 {
	buffer := make([]int32, capacity)
	for i := range buffer {
		buffer[i] = int32(InitialSequenceValue)
	}
	return buffer
}

func (this *SharedWriterBarrier) Read(lower int64) int64 {
	shift, mask := this.shift, this.mask
	upper := this.written.Load()

	// 自旋锁
	for {
		if atomic.CompareAndSwapInt32(&this.commiting, 0, 1) {
			for ; lower <= upper; lower++ {
				if this.committed[lower&mask] != int32(lower>>shift) {
					// 解锁
					atomic.StoreInt32(&this.commiting, 0)
					return lower - 1
				}
			}
			// 解锁
			atomic.StoreInt32(&this.commiting, 0)
			break
		} else {
			time.Sleep(time.Millisecond)
			runtime.Gosched()
		}
	}

	return upper
}
