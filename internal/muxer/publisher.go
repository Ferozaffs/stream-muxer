// Package muxer turns the active source's FLV tags into a single RTMP publish,
// switching between sources at keyframes with stream copy (no re-encoding).
package muxer

import (
	"bytes"
	"fmt"
	"net/url"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"
)

// Publisher writes tags to a single downstream RTMP endpoint.
type Publisher interface {
	// Ensure connects (or reconnects) the publisher. It is idempotent.
	Ensure() error
	WriteMeta(ts uint32, data []byte) error
	WriteVideo(ts uint32, data []byte) error
	WriteAudio(ts uint32, data []byte) error
	Close() error
}

// rtmpPublisher publishes to an RTMP server via a go-rtmp client.
type rtmpPublisher struct {
	rawURL string
	addr   string
	app    string
	key    string
	logger *log.Logger

	mu     sync.Mutex
	conn   *rtmp.ClientConn
	stream *rtmp.Stream
}

// NewRTMPPublisher parses url (rtmp://host:port/app/key) and returns a Publisher.
func NewRTMPPublisher(rawURL string, logger *log.Logger) (Publisher, error) {
	if logger == nil {
		logger = log.StandardLogger()
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse rtmp url: %w", err)
	}
	p := &rtmpPublisher{rawURL: rawURL, logger: logger}
	if u.Scheme != "rtmp" && u.Scheme != "rtmps" {
		return nil, fmt.Errorf("unsupported url scheme: %q", u.Scheme)
	}
	p.addr = u.Host
	path := trimSlashes(u.Path)
	seg := splitPath(path)
	if len(seg) < 2 {
		return nil, fmt.Errorf("rtmp url must be rtmp://host:port/app/key")
	}
	p.app = seg[0]
	p.key = seg[1]
	return p, nil
}

func (p *rtmpPublisher) Ensure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stream != nil {
		return nil
	}
	return p.connectLocked()
}

func (p *rtmpPublisher) connectLocked() error {
	// Close any stale connection first.
	p.discardLocked()

	handler := &captureHandlerBase{}
	conn, err := rtmp.Dial("rtmp", p.addr, &rtmp.ConnConfig{
		Handler: handler,
		Logger:  p.logger,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	if err := conn.Connect(&rtmpmsg.NetConnectionConnect{
		Command: rtmpmsg.NetConnectionConnectCommand{
			App:      p.app,
			FlashVer: "FMLE/3.0 (compatible; streammux)",
			TCURL:    p.rawURL,
		},
	}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("connect: %w", err)
	}
	stream, err := conn.CreateStream(nil, 128)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("create stream: %w", err)
	}
	if err := stream.Publish(&rtmpmsg.NetStreamPublish{PublishingName: p.key}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("publish: %w", err)
	}
	p.conn = conn
	p.stream = stream
	p.logger.WithField("url", p.rawURL).Info("published to downstream")
	return nil
}

func (p *rtmpPublisher) discardLocked() {
	if p.stream != nil {
		_ = p.stream.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.stream = nil
	p.conn = nil
}

func (p *rtmpPublisher) WriteMeta(ts uint32, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stream == nil {
		return p.connectLocked()
	}
	amdBuf := new(bytes.Buffer)
	amfEnc := rtmpmsg.NewAMFEncoder(amdBuf, rtmpmsg.EncodingTypeAMF0)
	if err := rtmpmsg.EncodeBodyAnyValues(amfEnc, &rtmpmsg.NetStreamSetDataFrame{Payload: data}); err != nil {
		return err
	}
	if err := p.stream.Write(8, ts, &rtmpmsg.DataMessage{
		Name:     "@setDataFrame",
		Encoding: rtmpmsg.EncodingTypeAMF0,
		Body:     amdBuf,
	}); err != nil {
		p.discardLocked()
		return err
	}
	return nil
}

func (p *rtmpPublisher) WriteVideo(ts uint32, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stream == nil {
		return p.connectLocked()
	}
	if err := p.stream.Write(6, ts, &rtmpmsg.VideoMessage{Payload: bytes.NewReader(data)}); err != nil {
		p.discardLocked()
		return err
	}
	return nil
}

func (p *rtmpPublisher) WriteAudio(ts uint32, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stream == nil {
		return p.connectLocked()
	}
	if err := p.stream.Write(5, ts, &rtmpmsg.AudioMessage{Payload: bytes.NewReader(data)}); err != nil {
		p.discardLocked()
		return err
	}
	return nil
}

func (p *rtmpPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Drop the current publish session (kills the output), but allow Ensure to
	// reconnect later so we can resume when a source comes back up.
	p.discardLocked()
	return nil
}

// captureHandlerBase is the client-side handler; server status responses are
// ignored (DefaultHandler no-ops them).
type captureHandlerBase struct {
	rtmp.DefaultHandler
}

func splitPath(p string) []string {
	out := []string{}
	cur := ""
	for _, r := range p {
		if r == '/' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func trimSlashes(p string) string {
	for len(p) > 0 && p[0] == '/' {
		p = p[1:]
	}
	for len(p) > 0 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	return p
}
