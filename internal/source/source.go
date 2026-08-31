// Package source models a single incoming stream. It spawns an ffmpeg capture
// (stream-copy FLV) that feeds the muxer, and a lightweight black-detect probe.
// Together they drive an up/down availability signal where "up" means actual
// frames are arriving (not black / PTS advancing), not merely that the RTMP
// server is reachable.
package source

import (
	"bufio"
	"context"

	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"streammux/internal/ffmpeg"
)

// State of a source's availability.
type State int

const (
	StateDown State = iota
	StateUp
)

func (s State) String() string {
	if s == StateUp {
		return "up"
	}
	return "down"
}

// Config configures one source.
type Config struct {
	Name     string
	Priority int
	URL      string // RTMP pull URL served by the ingest relay

	Logger *log.Logger

	NoDataTimeout  time.Duration // no frames for this long -> down (default 10s)
	BlackTimeout   time.Duration // black for this long -> down (default 10s)
	ReconnectDelay time.Duration // delay before retrying a dead capture (default 3s)
	TagsBuffer     int           // per-source tag channel buffer
}

func (c *Config) fill() {
	if c.NoDataTimeout <= 0 {
		c.NoDataTimeout = 10 * time.Second
	}
	if c.BlackTimeout <= 0 {
		c.BlackTimeout = 10 * time.Second
	}
	if c.ReconnectDelay <= 0 {
		c.ReconnectDelay = 3 * time.Second
	}
	if c.TagsBuffer <= 0 {
		c.TagsBuffer = 64
	}
	if c.Logger == nil {
		c.Logger = log.StandardLogger()
	}
}

// Cache is a snapshot of the tags needed to warm-start a downstream decoder:
// metadata, AVC/AAC sequence headers and the most recent key frame.
type Cache struct {
	Meta     *ffmpeg.RawTag
	AvcSeq   *ffmpeg.RawTag
	AudioSeq *ffmpeg.RawTag
	LastKey  *ffmpeg.RawTag
}

// Stat is a point-in-time availability report for one source.
type Stat struct {
	Name   string
	Up     bool
	At     time.Time
	NoData bool
	Black  bool
}

// Source is a running capture + availability monitor for one incoming stream.
type Source struct {
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	tags    chan ffmpeg.RawTag
	cacheMu sync.RWMutex
	cache   Cache

	procMu sync.Mutex
	procs  []*exec.Cmd

	// atomics
	state      int32 // State value
	lastDataNs int64
	black      int32
	blackSince int64

	changes chan Stat
}

// New creates a Source from config. Call Start to begin capture and probing.
func New(cfg Config) *Source {
	cfg.fill()
	ctx, cancel := context.WithCancel(context.Background())
	return &Source{
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		tags:    make(chan ffmpeg.RawTag, cfg.TagsBuffer),
		changes: make(chan Stat, 16),
		state:   int32(StateDown),
		// lastData starts at epoch so an idle/dataless source is Down until the
		// first real frame arrives.
		lastDataNs: 0,
	}
}

// Start launches capture + probe + state monitor. It returns immediately.
func (s *Source) Start() {
	go s.captureLoop()
	go s.probeLoop()
	go s.monitorLoop()
}

// Stop shuts down the source and all subprocesses.
func (s *Source) Stop() {
	s.cancel()
	s.procMu.Lock()
	for _, c := range s.procs {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
	s.procMu.Unlock()
	<-s.done
}

// Tags returns the per-source tag channel consumed by the muxer.
func (s *Source) Tags() <-chan ffmpeg.RawTag { return s.tags }

// State returns the current availability state.
func (s *Source) State() State { return State(atomic.LoadInt32(&s.state)) }

// Status is a diagnostic snapshot of a source's availability inputs.
type Status struct {
	State      State `json:"state"`
	NoData     bool  `json:"noData"`
	Black      bool  `json:"black"`
	LastDataMs int64 `json:"lastDataMs"` // age since last frame (0 = never)
	BlackMs    int64 `json:"blackMs"`    // age since black started (0 = not black)
}

// Status returns a diagnostic snapshot of the availability inputs.
func (s *Source) Status() Status {
	now := time.Now()
	st := Status{State: s.State()}
	ld := atomic.LoadInt64(&s.lastDataNs)
	if ld != 0 {
		if d := now.Sub(time.Unix(0, ld)); d > 0 {
			st.LastDataMs = d.Milliseconds()
		}
	} else {
		st.LastDataMs = -1 // never
	}
	if atomic.LoadInt32(&s.black) == 1 {
		st.Black = true
		if d := now.Sub(time.Unix(0, atomic.LoadInt64(&s.blackSince))); d > 0 {
			st.BlackMs = d.Milliseconds()
		}
	}
	st.NoData = st.LastDataMs < 0 || st.LastDataMs > s.cfg.NoDataTimeout.Milliseconds()
	return st
}

// Changes returns a channel of availability transitions.
func (s *Source) Changes() <-chan Stat { return s.changes }

// Name returns the source name.
func (s *Source) Name() string { return s.cfg.Name }

// Priority returns the configured priority (higher wins).
func (s *Source) Priority() int { return s.cfg.Priority }

// Snapshot returns a copy of the warm-start cache.
func (s *Source) Snapshot() Cache {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	return s.cache
}

// setState updates availability and emits a Stat on change.
func (s *Source) setState(up bool, noData, black bool) {
	new := StateDown
	if up {
		new = StateUp
	}
	old := State(atomic.SwapInt32(&s.state, int32(new)))
	if old == new {
		return
	}
	stat := Stat{Name: s.cfg.Name, Up: up, At: time.Now(), NoData: noData, Black: black}
	select {
	case s.changes <- stat:
	default:
	}
}

// monitorLoop recomputes availability periodically.
func (s *Source) monitorLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.recompute()
		}
	}
}

func (s *Source) recompute() {
	noData := time.Since(time.Unix(0, atomic.LoadInt64(&s.lastDataNs))) > s.cfg.NoDataTimeout

	blackNow := false
	if atomic.LoadInt32(&s.black) == 1 {
		if d := time.Since(time.Unix(0, atomic.LoadInt64(&s.blackSince))); d > s.cfg.BlackTimeout {
			blackNow = true
		}
	}

	s.setState(!noData && !blackNow, noData, blackNow)
}

// captureLoop keeps an ffmpeg capture running, feeding the tags channel and
// recording data freshness. It restarts the capture after failures.
func (s *Source) captureLoop() {
	defer close(s.tags)
	defer s.cancel()
	defer close(s.done)

	rec := &receiver{src: s}
	for {
		if s.ctx.Err() != nil {
			return
		}
		cmd := ffmpeg.Capture(s.cfg.URL)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			s.log().WithError(err).Warn("capture stdout pipe")
			s.sleep(s.cfg.ReconnectDelay)
			continue
		}
		unreg := s.register(cmd)

		if err := cmd.Start(); err != nil {
			s.log().WithError(err).Warn("start capture")
			unreg()
			s.sleep(s.cfg.ReconnectDelay)
			continue
		}

		err = ffmpeg.ReadFLV(stdout, rec.emit)
		_ = cmd.Wait()
		unreg()

		if s.ctx.Err() != nil {
			return
		}
		if err != nil {
			s.log().WithError(err).Warn("capture ended")
		}
		s.sleep(s.cfg.ReconnectDelay)
	}
}

func (s *Source) log() *log.Entry {
	return s.cfg.Logger.WithField("source", s.cfg.Name)
}

func (s *Source) sleep(d time.Duration) {
	select {
	case <-s.ctx.Done():
	case <-time.After(d):
	}
}

func (s *Source) register(cmd *exec.Cmd) func() {
	s.procMu.Lock()
	s.procs = append(s.procs, cmd)
	s.procMu.Unlock()
	return func() {
		s.procMu.Lock()
		for i, c := range s.procs {
			if c == cmd {
				s.procs = append(s.procs[:i], s.procs[i+1:]...)
				break
			}
		}
		s.procMu.Unlock()
	}
}

// receiver pushes parsed tags into the source's channel and cache.
type receiver struct {
	src *Source
}

func (r *receiver) emit(tag ffmpeg.RawTag) {
	s := r.src
	atomic.StoreInt64(&s.lastDataNs, time.Now().UnixNano())

	s.cacheMu.Lock()
	switch {
	case tag.IsMeta():
		cp := tag
		s.cache.Meta = &cp
	case tag.IsVideo() && tag.SeqHeader():
		cp := tag
		s.cache.AvcSeq = &cp
	case tag.IsAudio() && tag.AACSeqHeader():
		cp := tag
		s.cache.AudioSeq = &cp
	case tag.IsVideo() && tag.Keyframe():
		cp := tag
		s.cache.LastKey = &cp
	}
	s.cacheMu.Unlock()

	select {
	case s.tags <- tag:
	case <-s.ctx.Done():
	}
}

// probeLoop runs a black-detection ffmpeg and parses black_start / black_end
// diagnostics from its stderr. It restarts the probe after failures.
func (s *Source) probeLoop() {
	for {
		if s.ctx.Err() != nil {
			return
		}
		cmd := ffmpeg.Probe(s.cfg.URL)
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return
		}
		unreg := s.register(cmd)
		if err := cmd.Start(); err != nil {
			unreg()
			s.sleep(s.cfg.ReconnectDelay)
			continue
		}

		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "black_start:") {
				if atomic.CompareAndSwapInt32(&s.black, 0, 1) {
					atomic.StoreInt64(&s.blackSince, time.Now().UnixNano())
				}
				continue
			}
			if strings.Contains(line, "black_end:") {
				atomic.StoreInt32(&s.black, 0)
				atomic.StoreInt64(&s.blackSince, 0)
			}
		}
		_ = cmd.Wait()
		unreg()

		if s.ctx.Err() != nil {
			return
		}
		s.sleep(s.cfg.ReconnectDelay)
	}
}
