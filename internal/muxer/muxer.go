package muxer

import (
	"context"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"

	"streammux/internal/ffmpeg"
	"streammux/internal/source"
)

// Source is the minimal view of an incoming stream needed by the muxer. It is
// satisfied by *source.Source.
type Source interface {
	Name() string
	Tags() <-chan ffmpeg.RawTag
	Snapshot() source.Cache
}

// Muxer consumes every source's tag feed, forwards only the active one to the
// publisher, and switches sources atomically at a keyframe using each source's
// cached sequence header + keyframe (stream copy, no re-encoding).
type Muxer struct {
	pub     Publisher
	sources map[string]Source
	logger  *log.Logger

	mu         sync.Mutex
	active     atomic.Value // string
	outClockNS atomic.Int64 // last emitted output timestamp (ms)
}

// New creates a muxer over the given sources.
func New(pub Publisher, sources []Source, logger *log.Logger) *Muxer {
	if logger == nil {
		logger = log.StandardLogger()
	}
	m := &Muxer{pub: pub, sources: map[string]Source{}, logger: logger}
	m.active.Store("")
	for _, s := range sources {
		m.sources[s.Name()] = s
	}
	return m
}

// Active returns the currently selected source name ("" = none / output down).
func (m *Muxer) Active() string { return m.active.Load().(string) }

// SetActive selects the source to forward. An empty name takes the output
// offline (closes the publisher). Reactivating from "" starts a fresh publish
// session with a reset timestamp clock.
func (m *Muxer) SetActive(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == m.Active() {
		return
	}
	wasDown := m.Active() == "" && name != ""
	if name == "" {
		_ = m.pub.Close()
		m.logger.Warn("all sources down; stopping output")
	}
	m.active.Store(name)
	if wasDown {
		m.outClockNS.Store(0)
		if err := m.pub.Ensure(); err != nil {
			m.logger.WithError(err).Warn("downstream re-connect")
		}
	}
}

// Run starts one drain loop per source and blocks until ctx is cancelled.
func (m *Muxer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range m.sources {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.drain(ctx, s)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	_ = m.pub.Close()
}

func (m *Muxer) drain(ctx context.Context, s Source) {
	st := &srcState{}
	for {
		select {
		case <-ctx.Done():
			return
		case tag, ok := <-s.Tags():
			if !ok {
				return
			}
			m.forward(s, st, tag)
		}
	}
}

type srcState struct {
	started bool
	offset  int64
}

// forward applies keyframe-gated switching and timestamp rebasing for one tag of
// a source. It is called only by that source's single drain goroutine.
func (m *Muxer) forward(s Source, st *srcState, tag ffmpeg.RawTag) {
	if m.Active() != s.Name() {
		st.started = false
		return
	}

	if !st.started {
		cache := s.Snapshot()
		key := cache.LastKey
		if key == nil {
			if !(tag.IsVideo() && tag.Keyframe()) {
				return // wait for a keyframe before starting
			}
			key = &tag
		}
		start := uint32(m.outClockNS.Load())
		st.offset = int64(start) - int64(key.Ts)
		m.emitWarm(cache, start)
		m.write(key.Type, uint32(int64(key.Ts)+st.offset), key.Data)
		st.started = true
		if key == &tag {
			return
		}
	}

	m.write(tag.Type, uint32(int64(tag.Ts)+st.offset), tag.Data)
}

// emitWarm sends the metadata and codec sequence headers for the (new) active
// source so the downstream decoder reconfigures at the switch.
func (m *Muxer) emitWarm(cache source.Cache, at uint32) {
	if cache.Meta != nil {
		m.write(cache.Meta.Type, at, cache.Meta.Data)
	}
	if cache.AvcSeq != nil {
		m.write(cache.AvcSeq.Type, at, cache.AvcSeq.Data)
	}
	if cache.AudioSeq != nil {
		m.write(cache.AudioSeq.Type, at, cache.AudioSeq.Data)
	}
}

// write forwards one tag to the publisher, clamping its timestamp to be
// monotonic (never go backwards) and advancing the output clock.
func (m *Muxer) write(typ byte, ts uint32, data []byte) {
	var cur int64
	for {
		cur = m.outClockNS.Load()
		if int64(ts) >= cur {
			break
		}
		ts = uint32(cur)
	}
	m.outClockNS.Store(int64(ts))

	var err error
	switch typ {
	case 9:
		err = m.pub.WriteVideo(ts, data)
	case 8:
		err = m.pub.WriteAudio(ts, data)
	case 18:
		err = m.pub.WriteMeta(ts, data)
	}
	if err != nil {
		m.logger.WithError(err).Warn("publisher write")
	}
}
