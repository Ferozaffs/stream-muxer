// Package ffmpeg wraps the ffmpeg binary for live ingestion: capturing a source
// as an FLV byte stream (stream copy, never re-encoding) and probing downward
// for black-detect availability.
package ffmpeg

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
)

// Bin is the ffmpeg executable name. Override via env FFMPEG_BIN.
var Bin = func() string {
	if v := os.Getenv("FFMPEG_BIN"); v != "" {
		return v
	}
	return "ffmpeg"
}()

// Capture builds an ffmpeg command that pulls <url> and writes an FLV stream to
// stdout using stream copy (-c copy), i.e. no re-encoding.
func Capture(url string) *exec.Cmd {
	return exec.Command(Bin,
		"-hide_banner", "-loglevel", "error",
		"-i", url,
		"-c", "copy",
		"-f", "flv", "pipe:1",
	)
}

// Probe builds an ffmpeg command that decodes a tiny downscale of <url> and runs
// blackdetect, writing diagnostics to stderr and discarding frames. It is cheap:
// it decodes at ~scale to 64px.
func Probe(url string) *exec.Cmd {
	return exec.Command(Bin,
		"-hide_banner", "-loglevel", "info",
		"-i", url,
		"-vf", "scale=64:-1,blackdetect=d=1:pix_th=0.10",
		"-an", "-f", "null", "-",
	)
}

// RawTag is one FLV tag body with enough metadata to remux straight into an RTMP
// message and to detect key frames / sequence headers.
type RawTag struct {
	Type uint8 // tag/flv type: audio=8, video=9, script=18
	Ts   uint32
	Data []byte // raw FLV tag body (for video/audio includes the leading header byte)
}

// IsVideo reports whether the tag is an FLV video tag.
func (t RawTag) IsVideo() bool { return t.Type == 9 }

// IsAudio reports whether the tag is an FLV audio tag.
func (t RawTag) IsAudio() bool { return t.Type == 8 }

// IsMeta reports whether the tag is an FLV script/metadata tag.
func (t RawTag) IsMeta() bool { return t.Type == 18 }

// Keyframe reports whether a video tag is a keyframe (IDR). It inspects the
// leading FLV video body byte. For AVC it also accepts the sequence header.
func (t RawTag) Keyframe() bool {
	if !t.IsVideo() || len(t.Data) == 0 {
		return false
	}
	frameType := t.Data[0] >> 4 & 0x0f
	if frameType == 1 || frameType == 4 {
		return true
	}
	if frameType == 5 && len(t.Data) > 1 && t.Data[0]&0x0f == 7 {
		return t.Data[1] == 0 // video command frame + AVC seq header
	}
	return false
}

// FrameType returns the FLV frame type byte of a video tag, or 0 for non-video.
func (t RawTag) FrameType() uint8 {
	if !t.IsVideo() || len(t.Data) == 0 {
		return 0
	}
	return t.Data[0] >> 4 & 0x0f
}

// SeqHeader reports whether a video tag is an AVC sequence header.
func (t RawTag) SeqHeader() bool {
	return t.IsVideo() && len(t.Data) > 1 && t.Data[0]&0x0f == 7 && t.Data[1] == 0
}

// AACSeqHeader reports whether an audio tag is an AAC sequence header.
func (t RawTag) AACSeqHeader() bool {
	return t.IsAudio() && len(t.Data) > 1 && t.Data[0]>>4&0x0f == 10 && t.Data[1] == 0
}

// Emitter is called for each parsed tag.
type Emitter func(RawTag)

// ReadFLV parses a raw FLV stream from r (as emitted by `ffmpeg -f flv pipe:1`)
// and invokes emit for every tag. It returns any non-EOF error.
func ReadFLV(r io.Reader, emit Emitter) error {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if !bytes.Equal(header[0:3], []byte{'F', 'L', 'V'}) {
		return fmt.Errorf("not an FLV stream: bad signature")
	}
	// dataOffset points past the FLV flags; a 4-byte previous-tag-size follows.
	if _, err := io.ReadFull(r, make([]byte, 4)); err != nil {
		return err
	}

	for {
		th := make([]byte, 11)
		if _, err := io.ReadFull(r, th); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		tagType := th[0]
		dataSize := uint32(th[1])<<16 | uint32(th[2])<<8 | uint32(th[3])
		ts := uint32(th[7])<<24 | uint32(th[4])<<16 | uint32(th[5])<<8 | uint32(th[6])
		// streamID (th[8:11]) is zero for FLV and ignored.

		data := make([]byte, dataSize)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		// previous tag size, discard
		if _, err := io.ReadFull(r, make([]byte, 4)); err != nil {
			return err
		}

		emit(RawTag{Type: tagType, Ts: ts, Data: data})
	}
}
