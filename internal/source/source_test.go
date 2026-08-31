package source

import (
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"streammux/internal/ffmpeg"
)

func newTestSource(noData, black time.Duration) *Source {
	s := New(Config{
		Name:          "srcA",
		Priority:      1,
		URL:           "rtmp://127.0.0.1:1935/streams/srcA",
		Logger:        log.New(),
		NoDataTimeout: noData,
		BlackTimeout:  black,
	})
	return s
}

func TestSourceStartsDownAndGoesUpOnData(t *testing.T) {
	s := newTestSource(time.Minute, time.Minute)
	if s.State() != StateDown {
		t.Fatalf("initial state = %v, want down", s.State())
	}
	s.recompute()
	if s.State() != StateDown {
		t.Fatalf("state after recompute with no data = %v, want down", s.State())
	}
	atomic.StoreInt64(&s.lastDataNs, time.Now().UnixNano())
	s.recompute()
	if s.State() != StateUp {
		t.Fatalf("state with fresh data = %v, want up", s.State())
	}
}

func TestSourceDownFromNoData(t *testing.T) {
	s := newTestSource(50*time.Millisecond, time.Minute)
	atomic.StoreInt64(&s.lastDataNs, time.Now().Add(-200*time.Millisecond).UnixNano())
	s.recompute()
	if s.State() != StateDown {
		t.Fatalf("state with stale data = %v, want down", s.State())
	}
}

func TestSourceDownFromBlack(t *testing.T) {
	s := newTestSource(time.Minute, 50*time.Millisecond)
	atomic.StoreInt64(&s.lastDataNs, time.Now().UnixNano()) // fresh data, but black
	atomic.StoreInt32(&s.black, 1)
	atomic.StoreInt64(&s.blackSince, time.Now().Add(-200*time.Millisecond).UnixNano())
	s.recompute()
	if s.State() != StateDown {
		t.Fatalf("state with black = %v, want down", s.State())
	}
	// picture resumes
	atomic.StoreInt32(&s.black, 0)
	atomic.StoreInt64(&s.blackSince, 0)
	s.recompute()
	if s.State() != StateUp {
		t.Fatalf("state after black clears = %v, want up", s.State())
	}
}

// TestCacheSnapshots verifies the warm-start cache tracks the right tags.
func TestCacheSnapshots(t *testing.T) {
	s := newTestSource(time.Minute, time.Minute)
	rec := &receiver{src: s}
	rec.emit(ffmpeg.RawTag{Type: 18, Data: []byte("meta")})
	rec.emit(ffmpeg.RawTag{Type: 9, Data: []byte{0x17, 0x00, 0x01}})           // avc seq header
	rec.emit(ffmpeg.RawTag{Type: 8, Data: []byte{0xaf, 0x00, 0x02}})           // aac seq header
	rec.emit(ffmpeg.RawTag{Type: 9, Ts: 1000, Data: []byte{0x17, 0x01, 0x03}}) // keyframe

	c := s.Snapshot()
	if c.Meta == nil || c.AvcSeq == nil || c.AudioSeq == nil || c.LastKey == nil {
		t.Fatalf("cache incomplete: %+v", c)
	}
	if c.LastKey.Ts != 1000 {
		t.Errorf("lastKey ts = %d, want 1000", c.LastKey.Ts)
	}
}
