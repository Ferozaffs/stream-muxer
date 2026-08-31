package ingest

import (
	"fmt"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/yutopp/go-rtmp"
)

// Config configures the ingest relay server.
type Config struct {
	Addr    string   // e.g. ":1935"
	Allowed []string // optional allowlist of publish keys; empty means allow all
}

// Server is an RTMP relay: it receives OBS publishes and re-serves each stream
// to players (FFmpeg pulls) without transcoding.
type Server struct {
	relay  *relay
	srv    *rtmp.Server
	ln     net.Listener
	logger *log.Logger
}

// NewServer binds the RTMP listener and wires up the handler.
func NewServer(cfg Config, logger *log.Logger) (*Server, error) {
	if logger == nil {
		logger = log.StandardLogger()
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	relay := newRelay(cfg.Allowed, logger)
	srv := rtmp.NewServer(&rtmp.ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *rtmp.ConnConfig) {
			h := &handler{relay: relay, logger: logger}
			return conn, &rtmp.ConnConfig{
				Handler: h,
				ControlState: rtmp.StreamControlStateConfig{
					DefaultBandwidthWindowSize: 6 * 1024 * 1024 / 8,
				},
				Logger: logger,
			}
		},
	})
	return &Server{relay: relay, srv: srv, ln: ln, logger: logger}, nil
}

// Serve blocks until the listener is closed.
func (s *Server) Serve() error {
	return s.srv.Serve(s.ln)
}

// Addr returns the concrete listening address.
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

// Close stops the listener.
func (s *Server) Close() error {
	return s.ln.Close()
}
