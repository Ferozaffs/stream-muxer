package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	flvtag "github.com/yutopp/go-flv/tag"
	"github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"
)

var _ rtmp.Handler = (*handler)(nil)

// handler implements the per-connection RTMP handler: it accepts publishers
// (publish) and players (play), bridging one through the relay to the other.
type handler struct {
	rtmp.DefaultHandler
	relay  *relay
	logger *log.Logger

	conn *rtmp.Conn
	pub  *pub
	sub  *sub
}

func (h *handler) OnServe(conn *rtmp.Conn) {
	h.conn = conn
}

func (h *handler) OnPublish(_ *rtmp.StreamContext, _ uint32, cmd *rtmpmsg.NetStreamPublish) error {
	if err := h.relay.validate(cmd.PublishingName); err != nil {
		return err
	}
	ps, err := h.relay.newPubsub(cmd.PublishingName)
	if err != nil {
		return err
	}
	h.pub = ps.newPub()
	h.logger.WithField("stream", cmd.PublishingName).Info("stream published")
	return nil
}

func (h *handler) OnPlay(ctx *rtmp.StreamContext, _ uint32, cmd *rtmpmsg.NetStreamPlay) error {
	ps, err := h.relay.getPubsub(cmd.StreamName)
	if err != nil {
		return err
	}
	s := ps.newSub()
	s.emit = h.writeTag(ctx.StreamID)
	if h.conn != nil {
		s.closeConn = func() error { return h.conn.Close() }
	}
	h.sub = s
	h.logger.WithField("stream", cmd.StreamName).Info("stream played")
	return nil
}

func (h *handler) OnSetDataFrame(timestamp uint32, data *rtmpmsg.NetStreamSetDataFrame) error {
	r := bytes.NewReader(data.Payload)
	var script flvtag.ScriptData
	if err := flvtag.DecodeScriptData(r, &script); err != nil {
		h.logger.WithError(err).Debug("decode script data")
		return nil
	}
	if h.pub != nil {
		h.pub.publish(&flvtag.FlvTag{
			TagType:   flvtag.TagTypeScriptData,
			Timestamp: timestamp,
			Data:      &script,
		})
	}
	return nil
}

func (h *handler) OnAudio(timestamp uint32, payload io.Reader) error {
	var audio flvtag.AudioData
	if err := flvtag.DecodeAudioData(payload, &audio); err != nil {
		return err
	}
	body := new(bytes.Buffer)
	if _, err := io.Copy(body, audio.Data); err != nil {
		return err
	}
	audio.Data = body
	if h.pub != nil {
		h.pub.publish(&flvtag.FlvTag{
			TagType:   flvtag.TagTypeAudio,
			Timestamp: timestamp,
			Data:      &audio,
		})
	}
	return nil
}

func (h *handler) OnVideo(timestamp uint32, payload io.Reader) error {
	var video flvtag.VideoData
	if err := flvtag.DecodeVideoData(payload, &video); err != nil {
		return err
	}
	body := new(bytes.Buffer)
	if _, err := io.Copy(body, video.Data); err != nil {
		return err
	}
	video.Data = body
	if h.pub != nil {
		h.pub.publish(&flvtag.FlvTag{
			TagType:   flvtag.TagTypeVideo,
			Timestamp: timestamp,
			Data:      &video,
		})
	}
	return nil
}

func (h *handler) OnClose() {
	if h.pub != nil {
		h.logger.WithField("stream", h.pub.ps.name).Info("stream closed")
		h.pub.ps.deregister()
	}
	if h.sub != nil {
		_ = h.sub.close()
	}
}

// writeTag re-encodes a FLV tag into RTMP messages and writes them to the
// subscriber's connection.
func (h *handler) writeTag(streamID uint32) func(*flvtag.FlvTag) error {
	return func(tag *flvtag.FlvTag) error {
		buf := new(bytes.Buffer)
		ctx := context.Background()
		switch tag.Data.(type) {
		case *flvtag.AudioData:
			d := tag.Data.(*flvtag.AudioData)
			if err := flvtag.EncodeAudioData(buf, d); err != nil {
				return err
			}
			return h.conn.Write(ctx, 5, tag.Timestamp, &rtmp.ChunkMessage{
				StreamID: streamID,
				Message:  &rtmpmsg.AudioMessage{Payload: buf},
			})
		case *flvtag.VideoData:
			d := tag.Data.(*flvtag.VideoData)
			if err := flvtag.EncodeVideoData(buf, d); err != nil {
				return err
			}
			return h.conn.Write(ctx, 6, tag.Timestamp, &rtmp.ChunkMessage{
				StreamID: streamID,
				Message:  &rtmpmsg.VideoMessage{Payload: buf},
			})
		case *flvtag.ScriptData:
			d := tag.Data.(*flvtag.ScriptData)
			if err := flvtag.EncodeScriptData(buf, d); err != nil {
				return err
			}
			amdBuf := new(bytes.Buffer)
			amfEnc := rtmpmsg.NewAMFEncoder(amdBuf, rtmpmsg.EncodingTypeAMF0)
			if err := rtmpmsg.EncodeBodyAnyValues(amfEnc, &rtmpmsg.NetStreamSetDataFrame{
				Payload: buf.Bytes(),
			}); err != nil {
				return err
			}
			return h.conn.Write(ctx, 8, tag.Timestamp, &rtmp.ChunkMessage{
				StreamID: streamID,
				Message: &rtmpmsg.DataMessage{
					Name:     "@setDataFrame",
					Encoding: rtmpmsg.EncodingTypeAMF0,
					Body:     amdBuf,
				},
			})
		default:
			return fmt.Errorf("unhandled tag type: %T", tag.Data)
		}
	}
}
