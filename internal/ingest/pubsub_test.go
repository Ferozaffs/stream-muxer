package ingest

import (
	"bytes"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	flvtag "github.com/yutopp/go-flv/tag"
)

func vid(ts uint32, pkt flvtag.AVCPacketType, ft flvtag.FrameType) *flvtag.FlvTag {
	return &flvtag.FlvTag{
		TagType:   flvtag.TagTypeVideo,
		Timestamp: ts,
		Data: &flvtag.VideoData{
			FrameType:     ft,
			CodecID:       flvtag.CodecIDAVC,
			AVCPacketType: pkt,
			Data:          bytes.NewBufferString("frame"),
		},
	}
}

func audio(ts uint32) *flvtag.FlvTag {
	return &flvtag.FlvTag{
		TagType:   flvtag.TagTypeAudio,
		Timestamp: ts,
		Data: &flvtag.AudioData{
			SoundFormat: flvtag.SoundFormatAAC,
			SoundRate:   flvtag.SoundRate44kHz,
			SoundSize:   flvtag.SoundSize16Bit,
			SoundType:   flvtag.SoundTypeStereo,
			Data:        bytes.NewBufferString("audio"),
		},
	}
}

func script(ts uint32) *flvtag.FlvTag {
	return &flvtag.FlvTag{
		TagType:   flvtag.TagTypeScriptData,
		Timestamp: ts,
		Data:      &flvtag.ScriptData{},
	}
}

type record struct {
	kind  flvtag.TagType
	ts    uint32
	bytes string
}

func collect(s *sub) *[]record {
	recs := &[]record{}
	s.emit = func(tag *flvtag.FlvTag) error {
		switch d := tag.Data.(type) {
		case *flvtag.VideoData:
			b := new(bytes.Buffer)
			b.ReadFrom(d.Data)
			kind := d.AVCPacketType
			*recs = append(*recs, record{flvtag.TagTypeVideo, tag.Timestamp, "pkt=" + fmt.Sprint(kind) + " f=" + fmt.Sprint(d.FrameType)})
		case *flvtag.AudioData:
			b := new(bytes.Buffer)
			b.ReadFrom(d.Data)
			*recs = append(*recs, record{flvtag.TagTypeAudio, tag.Timestamp, b.String()})
		case *flvtag.ScriptData:
			*recs = append(*recs, record{flvtag.TagTypeScriptData, tag.Timestamp, ""})
		}
		return nil
	}
	return recs
}

func TestPubSubFanOut(t *testing.T) {
	l := log.New()
	r := newRelay([]string{"srcA"}, l)
	ps, err := r.newPubsub("srcA")
	if err != nil {
		t.Fatal(err)
	}
	p := ps.newPub()
	s := ps.newSub()
	recs := collect(s)

	p.publish(script(1000))
	p.publish(vid(1000, flvtag.AVCPacketTypeSequenceHeader, flvtag.FrameTypeVideoInfoCommandFrame))
	p.publish(vid(2000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeKeyFrame))
	p.publish(vid(3000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeInterFrame))
	p.publish(audio(2500))

	if len(*recs) != 5 {
		for _, r := range *recs {
			t.Logf("got %+v", r)
		}
		t.Fatalf("expected 5 tags, got %d", len(*recs))
	}
	want := []record{
		{flvtag.TagTypeScriptData, 0, ""},
		{flvtag.TagTypeVideo, 0, "pkt=0 f=5"},    // cached seq header
		{flvtag.TagTypeVideo, 1000, "pkt=1 f=1"}, // key frame
		{flvtag.TagTypeVideo, 2000, "pkt=1 f=2"}, // inter frame
		{flvtag.TagTypeAudio, 1500, "audio"},     // rebased relative to first script ts=1000
	}
	for i := range want {
		if (*recs)[i] != want[i] {
			t.Errorf("tag %d = %+v, want %+v", i, (*recs)[i], want[i])
		}
	}
}

func TestPubSubWarmStart(t *testing.T) {
	l := log.New()
	r := newRelay(nil, l)
	ps, _ := r.newPubsub("srcB")
	p := ps.newPub()

	p.publish(vid(1000, flvtag.AVCPacketTypeSequenceHeader, flvtag.FrameTypeKeyFrame))
	p.publish(vid(2000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeKeyFrame))
	p.publish(vid(3000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeInterFrame))

	s := ps.newSub()
	recs := collect(s)
	p.publish(vid(4000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeKeyFrame))
	p.publish(vid(5000, flvtag.AVCPacketTypeNALU, flvtag.FrameTypeInterFrame))

	// warm-start burst: seq header + last keyframe (2000). The live 4000 frame is
	// dropped (replaced by the cached keyframe), so 5000 arrives as the first live
	// frame, rebased against lastTs (1000) -> 4000.
	if len(*recs) != 3 {
		t.Fatalf("expected 3 tags after warm start, got %d", len(*recs))
	}
	if (*recs)[0].kind != flvtag.TagTypeVideo {
		t.Errorf("first warm-start tag kind = %v", (*recs)[0].kind)
	}
	if (*recs)[2].ts != 4000 {
		t.Errorf("live inter frame ts = %d, want 4000", (*recs)[2].ts)
	}
}

func TestRelayValidation(t *testing.T) {
	l := log.New()
	r := newRelay([]string{"srcA"}, l)
	if err := r.validate("srcB"); err == nil {
		t.Fatal("expected disallowed key to be rejected")
	}
	if err := r.validate("srcA"); err != nil {
		t.Fatalf("expected allowed key to pass, got %v", err)
	}
}

func TestRelayDeregisterAllowsRepublish(t *testing.T) {
	l := log.New()
	r := newRelay(nil, l)
	ps, err := r.newPubsub("srcA")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.newPubsub("srcA"); err == nil {
		t.Fatal("expected duplicate publish to fail")
	}
	ps.deregister()
	if _, err := r.newPubsub("srcA"); err != nil {
		t.Fatalf("expected republish after deregister, got %v", err)
	}
}

func TestPubsubDeregisterClosesSubscribers(t *testing.T) {
	l := log.New()
	r := newRelay(nil, l)
	ps, _ := r.newPubsub("srcA")
	s := ps.newSub()
	called := false
	s.closeConn = func() error {
		called = true
		return nil
	}
	ps.deregister()
	if !called {
		t.Fatal("expected subscriber closeConn to be invoked on deregister")
	}
}
