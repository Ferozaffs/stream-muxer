package ingest

import (
	"bytes"
	"sync"

	log "github.com/sirupsen/logrus"
	flvtag "github.com/yutopp/go-flv/tag"
)

// pubsub owns one published stream and fans out its FLV tags to subscribers.
type pubsub struct {
	r      *relay
	name   string
	logger *log.Logger

	pub  *pub
	subs []*sub
	mu   sync.Mutex
}

func (ps *pubsub) deregister() {
	ps.mu.Lock()
	subs := ps.subs
	ps.subs = nil
	ps.mu.Unlock()
	for _, s := range subs {
		// Force the subscriber's connection closed so its reader gets EOF and
		// reconnects. Without this, a stopped-then-restarted source leaves a
		// subscriber bound to the dead session, never seeing the re-published one.
		if s.closeConn != nil {
			_ = s.closeConn()
		}
		_ = s.close()
	}
	ps.r.removePubsub(ps.name)
}

func (ps *pubsub) newPub() *pub {
	p := &pub{ps: ps}
	ps.pub = p
	return p
}

func (ps *pubsub) newSub() *sub {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	s := &sub{}
	ps.subs = append(ps.subs, s)
	return s
}

// pub is the publish side. It caches the metadata, codec sequence header and
// most recent key frame so a subscriber that joins mid-stream can start cleanly
// without waiting a full GOP.
type pub struct {
	ps      *pubsub
	meta    *flvtag.FlvTag
	avcSeq  *flvtag.FlvTag
	lastKey *flvtag.FlvTag
}

func (p *pub) publish(tag *flvtag.FlvTag) {
	ps := p.ps

	switch d := tag.Data.(type) {
	case *flvtag.AudioData:
		ps.mu.Lock()
		subs := ps.subs
		ps.mu.Unlock()
		for _, s := range subs {
			_ = s.onEvent(cloneView(tag))
		}

	case *flvtag.ScriptData:
		p.meta = cloneView(tag)
		ps.mu.Lock()
		subs := ps.subs
		ps.mu.Unlock()
		for _, s := range subs {
			if s.gotMeta {
				continue
			}
			_ = s.onEvent(cloneView(tag))
			s.gotMeta = true
		}

	case *flvtag.VideoData:
		if d.AVCPacketType == flvtag.AVCPacketTypeSequenceHeader {
			p.avcSeq = cloneView(tag)
		}
		if d.FrameType == flvtag.FrameTypeKeyFrame {
			p.lastKey = cloneView(tag)
		}

		ps.mu.Lock()
		subs := ps.subs
		ps.mu.Unlock()
		for _, s := range subs {
			if !s.initialized {
				if !s.gotMeta && p.meta != nil {
					_ = s.onEvent(cloneView(p.meta))
				}
				if p.avcSeq != nil {
					_ = s.onEvent(cloneView(p.avcSeq))
				}
				if p.avcSeq != nil && p.lastKey != nil {
					_ = s.onEvent(cloneView(p.lastKey))
				}
				s.gotMeta = true
				s.gotSeq = true
				s.initialized = true
				continue
			}
			_ = s.onEvent(cloneView(tag))
		}
	}
}

type pubEvent func(tag *flvtag.FlvTag) error

// sub is the subscribe side. It rebases timestamps after the warm-start burst
// so the first cached key frame is delivered at timestamp 0.
type sub struct {
	initialized bool
	gotMeta     bool
	gotSeq      bool
	closed      bool
	lastTs      uint32
	emit        pubEvent
	closeConn   func() error
}

func (s *sub) onEvent(tag *flvtag.FlvTag) error {
	if s.closed {
		return nil
	}
	if tag.Timestamp != 0 && s.lastTs == 0 {
		s.lastTs = tag.Timestamp
	}
	tag.Timestamp -= s.lastTs
	return s.emit(tag)
}

func (s *sub) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

// cloneView copies the tag view and its byte payload so the underlying reader
// is not consumed by a concurrent subscriber.
func cloneView(flv *flvtag.FlvTag) *flvtag.FlvTag {
	v := *flv
	switch d := v.Data.(type) {
	case *flvtag.AudioData:
		c := *d
		c.Data = bytes.NewBuffer(d.Data.(*bytes.Buffer).Bytes())
		v.Data = &c
	case *flvtag.VideoData:
		c := *d
		c.Data = bytes.NewBuffer(d.Data.(*bytes.Buffer).Bytes())
		v.Data = &c
	case *flvtag.ScriptData:
		c := *d
		v.Data = &c
	}
	return &v
}
