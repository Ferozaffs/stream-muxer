// Package controller resolves which source should be active based on priority
// and live availability, applying hysteresis so flapping sources don't thrash
// the switch.
package controller

import (
	"context"
	"sort"
	"sync"
	"time"

	"streammux/internal/source"
)

// Config for the failover controller.
type Config struct {
	// PromoteAfter is how long a higher-priority source must stay up before it
	// is allowed to preempt the current active source. This prevents a flapping
	// high-priority stream from constantly stealing the output.
	PromoteAfter time.Duration
}

func (c *Config) fill() {
	if c.PromoteAfter <= 0 {
		c.PromoteAfter = 2 * time.Second
	}
}

// Source is the minimal view of an incoming stream that the controller needs.
// It is satisfied by *source.Source.
type Source interface {
	Name() string
	Priority() int
	State() source.State
	Changes() <-chan source.Stat
}

// Controller tracks sources, computes the desired active source and emits
// decisions when the active source changes.
type Controller struct {
	cfg Config

	mu        sync.Mutex
	sources   map[string]*tracked
	active    string
	decisions chan string
	log       Logger
}

// Logger is a minimal logger interface to avoid a hard logrus dependency here.
type Logger interface {
	Infof(format string, args ...interface{})
}

type tracked struct {
	name    string
	src     Source
	up      bool
	upSince time.Time
}

// New creates a controller seeded with the current state of the sources.
func New(sources []Source, cfg Config, log Logger) *Controller {
	cfg.fill()
	if log == nil {
		log = nopLogger{}
	}
	c := &Controller{
		cfg:       cfg,
		sources:   make(map[string]*tracked, len(sources)),
		decisions: make(chan string, 16),
		log:       log,
	}
	now := time.Now()
	for _, s := range sources {
		t := &tracked{name: s.Name(), src: s, up: s.State() == source.StateUp, upSince: now}
		c.sources[s.Name()] = t
	}
	c.active = c.decide(now)
	if c.active != "" {
		c.log.Infof("initial active source: %s", c.active)
	}
	return c
}

// Run monitors source availability and emits decisions on Active() until ctx is
// cancelled.
func (c *Controller) Run(ctx context.Context) {
	defer close(c.decisions)
	fanIn := make(chan source.Stat, 32)
	var wg sync.WaitGroup
	for _, t := range c.sources {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case st, ok := <-t.src.Changes():
					if !ok {
						return
					}
					select {
					case fanIn <- st:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(fanIn)
	}()

	// Periodic re-evaluation: preemption depends on a source having been up for
	// PromoteAfter, which is a time-based condition that no event fires for. The
	// tick keeps calling decide() so a higher-priority source that stabilizes
	// later actually preempts.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.recalc(time.Now())
		case st, ok := <-fanIn:
			if !ok {
				return
			}
			c.recompute(st, time.Now())
		}
	}
}

// recompute applies a stat, then re-decides whether the active changed.
func (c *Controller) recompute(st source.Stat, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.sources[st.Name]
	if !ok {
		return
	}
	if st.Up && !t.up {
		t.up = true
		t.upSince = now
		c.log.Infof("source %s is up", st.Name)
	} else if !st.Up && t.up {
		t.up = false
		t.upSince = time.Time{}
		c.log.Infof("source %s is down (noData=%v black=%v)", st.Name, st.NoData, st.Black)
	}

	c.recalcLocked(now)
}

// recalc re-evaluates the active source without applying any stat. It is called
// periodically and after each stat to pick up time-based preemption.
func (c *Controller) recalc(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recalcLocked(now)
}

func (c *Controller) recalcLocked(now time.Time) {
	next := c.decide(now)
	if next != c.active {
		prev := c.active
		c.active = next
		c.log.Infof("active source changed %q -> %q", prev, next)
		select {
		case c.decisions <- next:
		default:
		}
	}
}

// decide selects the source that should be active. The current active is kept
// while it is up unless a higher-priority source has been up long enough to
// preempt. A down active falls back immediately to the highest up source, or
// to "" when none are up.
func (c *Controller) decide(now time.Time) string {
	stable := func(t *tracked) bool {
		return t.up && now.Sub(t.upSince) >= c.cfg.PromoteAfter
	}

	if c.active != "" {
		if cur, ok := c.sources[c.active]; ok && cur.up {
			// preempt only by a higher-priority (than active) source that is stable.
			best := ""
			for _, t := range c.sources {
				if t.priority() > cur.priority() && stable(t) {
					if best == "" || t.priority() > c.sources[best].priority() {
						best = t.name
					}
				}
			}
			if best != "" {
				return best
			}
			return c.active
		}
	}

	// active is down or unset: pick the highest up source (immediate failover).
	best := ""
	for _, t := range c.sources {
		if t.up {
			if best == "" || t.priority() > c.sources[best].priority() {
				best = t.name
			}
		}
	}
	return best
}

// Current returns the current decided active source name ("" means none up).
func (c *Controller) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

// SourceNames returns known source names sorted by priority descending.
func (c *Controller) SourceNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.sources))
	for _, t := range c.sources {
		names = append(names, t.name)
	}
	sort.Slice(names, func(i, j int) bool { return c.sources[names[i]].priority() > c.sources[names[j]].priority() })
	return names
}

// SourceView is the controller's diagnosed view of one source.
type SourceView struct {
	Up       bool  `json:"up"`
	Stable   bool  `json:"stable"`
	UpForMs  int64 `json:"upForMs"`
	Priority int   `json:"priority"`
}

// View returns the controller's decision inputs for every source.
func (c *Controller) View(now time.Time) map[string]SourceView {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]SourceView, len(c.sources))
	promote := c.cfg.PromoteAfter.Milliseconds()
	for name, t := range c.sources {
		v := SourceView{Up: t.up, Priority: t.priority()}
		if t.up {
			v.UpForMs = now.Sub(t.upSince).Milliseconds()
			v.Stable = v.UpForMs >= promote
		}
		out[name] = v
	}
	return out
}

// Active emits a decision whenever the active source changes.
func (c *Controller) Active() <-chan string {
	return c.decisions
}

func (t *tracked) priority() int { return t.src.Priority() }

type nopLogger struct{}

func (nopLogger) Infof(string, ...interface{}) {}
