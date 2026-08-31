package controller

import (
	"testing"
	"time"

	"streammux/internal/source"
)

// fakeSource is a controllable implementation of controller.Source (no ffmpeg).
type fakeSource struct {
	name     string
	priority int
	state    source.State
	changes  chan source.Stat
}

func newFake(name string, p int) *fakeSource {
	return &fakeSource{name: name, priority: p, state: source.StateDown, changes: make(chan source.Stat, 8)}
}

func (f *fakeSource) Name() string                { return f.name }
func (f *fakeSource) Priority() int               { return f.priority }
func (f *fakeSource) State() source.State         { return f.state }
func (f *fakeSource) Changes() <-chan source.Stat { return f.changes }

func (f *fakeSource) set(up bool) {
	if up {
		f.state = source.StateUp
	} else {
		f.state = source.StateDown
	}
	f.changes <- source.Stat{Name: f.name, Up: up, At: time.Now()}
}

func mustActive(t *testing.T, c *Controller, want string) {
	t.Helper()
	if got := c.Current(); got != want {
		t.Fatalf("active = %q, want %q", got, want)
	}
}

func TestControllerInitialAllDown(t *testing.T) {
	c0 := New([]Source{newFake("a", 3), newFake("b", 2), newFake("c", 1)}, Config{}, nil)
	mustActive(t, c0, "")
}

func TestControllerHighestPriorityUp(t *testing.T) {
	a, b := newFake("a", 3), newFake("b", 2)
	c0 := New([]Source{a, b}, Config{}, nil)

	a.set(true)
	c0.recompute(readStat(a), time.Now())
	mustActive(t, c0, "a")

	b.set(true)
	c0.recompute(readStat(b), time.Now())
	mustActive(t, c0, "a") // a has higher priority -> still a
}

func TestControllerPreemptAfterPromote(t *testing.T) {
	a, b := newFake("a", 1), newFake("b", 3)
	c0 := New([]Source{a, b}, Config{PromoteAfter: 500 * time.Millisecond}, nil)
	base := time.Now()

	a.set(true)
	c0.recompute(readStat(a), base)
	mustActive(t, c0, "a")

	// b comes up but not yet stable: a (lower priority) stays active.
	b.set(true)
	c0.recompute(readStat(b), base)
	mustActive(t, c0, "a")

	// later than PromoteAfter: b preempts even though a is still up.
	c0.recompute(source.Stat{Name: "b", Up: true}, base.Add(600*time.Millisecond))
	mustActive(t, c0, "b")
}

func TestControllerFailbackOnDown(t *testing.T) {
	a, b := newFake("a", 3), newFake("b", 2)
	c0 := New([]Source{a, b}, Config{}, nil)

	a.set(true)
	c0.recompute(readStat(a), time.Now())
	mustActive(t, c0, "a")

	a.set(false)
	c0.recompute(readStat(a), time.Now())
	mustActive(t, c0, "") // b hasn't come up

	b.set(true)
	c0.recompute(readStat(b), time.Now())
	mustActive(t, c0, "b")
}

func TestControllerRecalcPreemptsAfterPromote(t *testing.T) {
	a, b := newFake("a", 1), newFake("b", 3)
	c0 := New([]Source{a, b}, Config{PromoteAfter: 500 * time.Millisecond}, nil)
	base := time.Now()

	a.set(true)
	c0.recompute(readStat(a), base)

	// b comes up at the same instant (not yet stable) -> a stays active.
	b.set(true)
	c0.recompute(readStat(b), base)
	mustActive(t, c0, "a")

	// No new event arrives; time just passes. The periodic recalc (ticker) must
	// observe b is now stable and preempt.
	c0.recalc(base.Add(600 * time.Millisecond))
	mustActive(t, c0, "b")
}

func TestControllerAllDown(t *testing.T) {
	a := newFake("a", 3)
	c0 := New([]Source{a}, Config{}, nil)
	a.set(false)
	c0.recompute(readStat(a), time.Now())
	mustActive(t, c0, "")
}

func readStat(f *fakeSource) source.Stat {
	select {
	case st := <-f.changes:
		return st
	default:
		return source.Stat{Name: f.name, Up: f.state == source.StateUp, At: time.Now()}
	}
}
