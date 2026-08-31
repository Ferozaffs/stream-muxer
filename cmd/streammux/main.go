package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"streammux/internal/controller"
	"streammux/internal/ingest"
	"streammux/internal/muxer"
	"streammux/internal/source"
)

func main() {
	var (
		relayAddr   = flag.String("addr", env("STREAMMUX_ADDR", ":1935"), "RTMP ingest listen address")
		downstream  = flag.String("out", env("STREAMMUX_DOWNSTREAM", ""), "RTMP publish URL, e.g. rtmp://host/app/key")
		sourcesFlag = flag.String("sources", env("STREAMMUX_SOURCES", ""), "comma-separated name:priority, e.g. srcA:10,srcB:5")
		noData      = flag.Duration("no-data", duration("STREAMMUX_NO_DATA", 10*time.Second), "no-frames timeout before a source is down")
		black       = flag.Duration("black", duration("STREAMMUX_BLACK", 10*time.Second), "black-frames timeout before a source is down")
		promote     = flag.Duration("promote", duration("STREAMMUX_PROMOTE", 2*time.Second), "how long a higher-priority source must stay up before preempting")
		statusAddr  = flag.String("status", env("STREAMMUX_STATUS", ":8080"), "HTTP status listen address (empty to disable)")
	)
	flag.Parse()

	logger := log.New()
	logger.SetLevel(log.InfoLevel)
	logger.SetOutput(os.Stdout)

	if *downstream == "" {
		logger.Fatal("downstream RTMP URL is required (-out / STREAMMUX_DOWNSTREAM)")
	}
	cfg, err := parseSources(*sourcesFlag)
	if err != nil {
		logger.WithError(err).Fatal("parse sources")
	}
	if len(cfg) == 0 {
		logger.Fatal("at least one source is required (-sources / STREAMMUX_SOURCES)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. ingest relay accepts OBS publishes on the configured stream names.
	relay, err := ingest.NewServer(ingest.Config{Addr: *relayAddr, Allowed: namesOf(cfg)}, logger)
	if err != nil {
		logger.WithError(err).Fatal("start ingest")
	}
	go func() { _ = relay.Serve() }()
	logger.Infof("ingest relay listening on %s (OBS keys: %s)", relay.Addr(), strings.Join(namesOf(cfg), ", "))

	// 2. build a source per configured name, pulling back from the relay on loopback.
	pubURL := fmt.Sprintf("rtmp://127.0.0.1:%d/streams", relayPort(relay))
	var srcs []*source.Source
	for _, sc := range cfg {
		src := source.New(source.Config{
			Name:          sc.name,
			Priority:      sc.priority,
			URL:           pubURL + "/" + sc.name,
			Logger:        logger,
			NoDataTimeout: *noData,
			BlackTimeout:  *black,
		})
		src.Start()
		srcs = append(srcs, src)
	}

	// 3. controller decides the active source; muxer publishes it downstream.
	pub, err := muxer.NewRTMPPublisher(*downstream, logger)
	if err != nil {
		logger.WithError(err).Fatal("downstream")
	}
	ctl := controller.New(controllerSources(srcs), controller.Config{PromoteAfter: *promote}, logger)
	mx := muxer.New(pub, muxerSources(srcs), logger)

	go ctl.Run(ctx)
	go muxWatch(ctx, ctl, mx)
	go func() {
		mx.Run(ctx)
	}()

	if *statusAddr != "" {
		go serveStatus(*statusAddr, ctl, srcs, logger)
	}

	logger.Info("stream muxer started")
	waitSignal()
	cancel()
	logger.Info("shutting down")
}

type srcCfg struct {
	name     string
	priority int
}

func parseSources(s string) ([]srcCfg, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("no sources specified")
	}
	var out []srcCfg
	for _, part := range splitComma(s) {
		namePrio := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(namePrio) != 2 {
			return nil, fmt.Errorf("source %q must be name:priority", part)
		}
		name := strings.TrimSpace(namePrio[0])
		if name == "" {
			return nil, fmt.Errorf("empty source name in %q", part)
		}
		prio, err := parsePriority(namePrio[1])
		if err != nil {
			return nil, err
		}
		out = append(out, srcCfg{name: name, priority: prio})
	}
	return out, nil
}

func parsePriority(s string) (int, error) {
	p := 0
	for _, r := range strings.TrimSpace(s) {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid priority %q", s)
		}
		p = p*10 + int(r-'0')
	}
	return p, nil
}

func splitComma(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func namesOf(cfgs []srcCfg) []string {
	out := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, c.name)
	}
	return out
}

// controllerSources adapts []*source.Source to controller.Source.
func controllerSources(srcs []*source.Source) []controller.Source {
	out := make([]controller.Source, 0, len(srcs))
	for _, s := range srcs {
		out = append(out, s)
	}
	return out
}

// muxerSources adapts []*source.Source to muxer.Source.
func muxerSources(srcs []*source.Source) []muxer.Source {
	out := make([]muxer.Source, 0, len(srcs))
	for _, s := range srcs {
		out = append(out, s)
	}
	return out
}

// muxWatch forwards controller decisions to the muxer (including all-down).
func muxWatch(ctx context.Context, ctl *controller.Controller, mx *muxer.Muxer) {
	mx.SetActive(ctl.Current())
	for {
		select {
		case <-ctx.Done():
			return
		case name, ok := <-ctl.Active():
			if !ok {
				return
			}
			mx.SetActive(name)
		}
	}
}

func serveStatus(addr string, ctl *controller.Controller, srcs []*source.Source, logger *log.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		view := ctl.View(now)
		payload := struct {
			Active string                 `json:"active"`
			Now    string                 `json:"now"`
			Source map[string]interface{} `json:"sources"`
		}{
			Active: ctl.Current(),
			Now:    now.Format(time.RFC3339),
			Source: map[string]interface{}{},
		}
		for _, src := range srcs {
			payload.Source[src.Name()] = struct {
				source.Status
				Up       bool  `json:"up"`
				Stable   bool  `json:"stable"`
				UpForMs  int64 `json:"upForMs"`
				Priority int   `json:"priority"`
			}{
				Status:   src.Status(),
				Up:       view[src.Name()].Up,
				Stable:   view[src.Name()].Stable,
				UpForMs:  view[src.Name()].UpForMs,
				Priority: src.Priority(),
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	})
	logger.Infof("status HTTP on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.WithError(err).Warn("status server")
	}
}

func waitSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}

func relayPort(relay *ingest.Server) int {
	addr := relay.Addr()
	if ta, ok := addr.(*net.TCPAddr); ok {
		return ta.Port
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 1935
	}
	p, _ := strconv.Atoi(port)
	return p
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func duration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
