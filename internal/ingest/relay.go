package ingest

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// relay is a publish/subscribe registry keyed by stream name. Each published
// stream can be consumed by any number of subscribers (FFmpeg pulls).
type relay struct {
	allowed map[string]bool // nil means accept any publish key
	streams map[string]*pubsub
	mu      sync.Mutex
	logger  *log.Logger
}

func newRelay(allowed []string, logger *log.Logger) *relay {
	r := &relay{
		streams: make(map[string]*pubsub),
		logger:  logger,
	}
	if len(allowed) > 0 {
		r.allowed = make(map[string]bool, len(allowed))
		for _, k := range allowed {
			r.allowed[k] = true
		}
	}
	return r
}

func (r *relay) validate(key string) error {
	if r.allowed == nil {
		return nil
	}
	if !r.allowed[key] {
		return fmt.Errorf("stream key %q not allowed", key)
	}
	return nil
}

func (r *relay) newPubsub(key string) (*pubsub, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.streams[key]; ok {
		return nil, fmt.Errorf("stream already published: %q", key)
	}
	ps := &pubsub{r: r, name: key, logger: r.logger}
	r.streams[key] = ps
	return ps, nil
}

func (r *relay) getPubsub(key string) (*pubsub, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps, ok := r.streams[key]
	if !ok {
		return nil, fmt.Errorf("no published stream: %q", key)
	}
	return ps, nil
}

func (r *relay) removePubsub(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.streams, key)
}
