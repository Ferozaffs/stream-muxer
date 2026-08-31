package ffmpeg

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func flvHeader(v uint8, flags uint8, ver uint8, dataOffset uint32) []byte {
	h := []byte{'F', 'L', 'V', ver, flags, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(h[5:9], dataOffset)
	return h
}

func tagBytes(t uint8, ts uint32, data []byte) []byte {
	out := make([]byte, 11)
	out[0] = t
	sz := uint32(len(data))
	out[1] = byte(sz >> 16 & 0xff)
	out[2] = byte(sz >> 8 & 0xff)
	out[3] = byte(sz & 0xff)
	out[4] = byte(ts >> 16 & 0xff)
	out[5] = byte(ts >> 8 & 0xff)
	out[6] = byte(ts & 0xff)
	out[7] = byte(ts >> 24 & 0xff)
	out = append(out, data...)
	// previous tag size
	ps := make([]byte, 4)
	binary.BigEndian.PutUint32(ps, uint32(len(data)+11))
	return append(out, ps...)
}

func TestReadFLV(t *testing.T) {
	keyframe := []byte{0x17, 0x01, 0x00}       // frameType=1 codec=7 avc nalu
	seq := []byte{0x17, 0x00, 0x00}            // frameType=5 codec=7 avc seq header
	audio := []byte{0xaf, 0x01, 0x11}          // aac
	meta := []byte{0x02, 0x00, 0x05, 'o', 'n'} // arbitrary amf bytes

	var stream bytes.Buffer
	stream.Write(flvHeader(1, 0x05, 1, 9))
	stream.Write([]byte{0, 0, 0, 0}) // initial previous tag size
	stream.Write(tagBytes(9, 0, seq))
	stream.Write(tagBytes(9, 1000, keyframe))
	stream.Write(tagBytes(8, 500, audio))
	stream.Write(tagBytes(18, 250, meta))

	var got []RawTag
	if err := ReadFLV(&stream, func(tag RawTag) { got = append(got, tag) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 tags, got %d", len(got))
	}
	if got[0].Type != 9 || got[0].Ts != 0 || !got[0].SeqHeader() {
		t.Errorf("tag0 = %+v, want seq header ts=0", got[0])
	}
	if !got[1].Keyframe() {
		t.Errorf("tag1 should be a keyframe")
	}
	if got[1].Ts != 1000 {
		t.Errorf("tag1 ts = %d, want 1000", got[1].Ts)
	}
	if !got[2].IsAudio() {
		t.Errorf("tag2 should be audio")
	}
	if !got[3].IsMeta() {
		t.Errorf("tag3 should be meta/script")
	}
}

func TestReadFLVBadSignature(t *testing.T) {
	err := ReadFLV(bytes.NewReader([]byte("NOPE")), func(RawTag) {})
	if err == nil {
		t.Fatal("expected error for bad signature")
	}
}
