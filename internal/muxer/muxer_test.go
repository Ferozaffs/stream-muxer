package muxer

import (
	"sync"
	"testing"

	"streammux/internal/ffmpeg"
	"streammux/internal/source"
)

type fakeSrc struct {
	name  string
	tags  chan ffmpeg.RawTag
	cache source.Cache
}

func newFakeSrc(name string) *fakeSrc {
	return &fakeSrc{name: name, tags: make(chan ffmpeg.RawTag, 16)}
}
func (f *fakeSrc) Name() string               { return f.name }
func (f *fakeSrc) Tags() <-chan ffmpeg.RawTag { return f.tags }
func (f *fakeSrc) Snapshot() source.Cache     { return f.cache }
func (f *fakeSrc) setCache(c source.Cache)    { f.cache = c }

func tagPtr(t ffmpeg.RawTag) *ffmpeg.RawTag { return &t }

type writeRecord struct {
	typ  byte
	ts   uint32
	data string
}

type fakePub struct {
	mu     sync.Mutex
	writes []writeRecord
	closed bool
}

func (p *fakePub) Ensure() error                           { return nil }
func (p *fakePub) WriteMeta(ts uint32, data []byte) error  { p.add(18, ts, data); return nil }
func (p *fakePub) WriteVideo(ts uint32, data []byte) error { p.add(9, ts, data); return nil }
func (p *fakePub) WriteAudio(ts uint32, data []byte) error { p.add(8, ts, data); return nil }
func (p *fakePub) Close() error                            { p.closed = true; return nil }

func (p *fakePub) add(typ byte, ts uint32, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, writeRecord{typ, ts, string(data)})
}

func (p *fakePub) snapshot() []writeRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]writeRecord, len(p.writes))
	copy(out, p.writes)
	return out
}

func keyTag(ts uint32) ffmpeg.RawTag {
	return ffmpeg.RawTag{Type: 9, Ts: ts, Data: []byte{0x17, 0x01, 'K'}}
}
func intTag(ts uint32) ffmpeg.RawTag {
	return ffmpeg.RawTag{Type: 9, Ts: ts, Data: []byte{0x27, 0x01, 'I'}}
}
func seqTag(ts uint32) ffmpeg.RawTag {
	return ffmpeg.RawTag{Type: 9, Ts: ts, Data: []byte{0x17, 0x00, 'S'}}
}

var (
	keyBody = string(keyTag(0).Data)
	intBody = string(intTag(0).Data)
	seqBody = string(seqTag(0).Data)
)

func TestMuxerWarmStartOnKeyframe(t *testing.T) {
	a := newFakeSrc("a")
	a.setCache(source.Cache{
		AvcSeq:  tagPtr(seqTag(500)),
		LastKey: tagPtr(keyTag(1000)),
	})
	pub := &fakePub{}
	m := New(pub, []Source{a}, nil)
	m.SetActive("a")

	m.forward(a, &srcState{}, intTag(1001))

	w := pub.snapshot()
	want := []writeRecord{
		{9, 0, seqBody}, // seq header rebased to 0
		{9, 0, keyBody}, // cached keyframe at rebased start
		{9, 1, intBody}, // live inter frame rebased
	}
	if len(w) != len(want) {
		t.Fatalf("got %d writes, want %d: %+v", len(w), len(want), w)
	}
	for i := range want {
		if w[i] != want[i] {
			t.Errorf("write %d = %+v, want %+v", i, w[i], want[i])
		}
	}
}

func TestMuxerGateOnKeyframe(t *testing.T) {
	a := newFakeSrc("a") // empty cache -> no cached keyframe
	pub := &fakePub{}
	m := New(pub, []Source{a}, nil)
	m.SetActive("a")
	st := &srcState{}

	// non-keyframe first -> nothing emitted
	m.forward(a, st, intTag(100))
	if got := pub.snapshot(); len(got) != 0 {
		t.Fatalf("expected no writes before keyframe, got %+v", got)
	}

	// keyframe arrives -> warm start on it
	m.forward(a, st, keyTag(500))
	w := pub.snapshot()
	if len(w) != 1 || w[0].typ != 9 || w[0].ts != 0 || w[0].data != keyBody {
		t.Fatalf("expected single rebased keyframe, got %+v", w)
	}
}

func TestMuxerSwitchDropsInactive(t *testing.T) {
	a, b := newFakeSrc("a"), newFakeSrc("b")
	b.setCache(source.Cache{LastKey: tagPtr(keyTag(1000))})
	pub := &fakePub{}
	m := New(pub, []Source{a, b}, nil)
	m.SetActive("a")
	sta, stb := &srcState{}, &srcState{}

	// a active: warm start + forward
	m.forward(a, sta, keyTag(50))
	m.forward(a, sta, intTag(60))
	pre := len(pub.snapshot())
	if pre < 2 {
		t.Fatalf("expected a to have been forwarded, got %d writes", pre)
	}

	// switch to b
	m.SetActive("b")
	m.forward(a, sta, intTag(70)) // a is now inactive -> dropped
	afterDropped := len(pub.snapshot())
	if afterDropped != pre {
		t.Fatalf("expected a tag dropped after switch, got %d", afterDropped)
	}

	m.forward(b, stb, intTag(1001)) // b becomes active via keyframe gate
	w := pub.snapshot()
	if len(w) <= afterDropped {
		t.Fatalf("expected b to be forwarded after switch")
	}
}

func TestMuxerAllDownClosesPublisher(t *testing.T) {
	a := newFakeSrc("a")
	pub := &fakePub{}
	m := New(pub, []Source{a}, nil)
	m.SetActive("a")
	m.SetActive("")
	if !pub.closed {
		t.Fatal("expected publisher closed on all-down")
	}
}
