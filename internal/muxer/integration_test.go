package muxer

import (
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"streammux/internal/ingest"
)

// TestRTMPPublisherAgainstServer runs the downstream publisher against a real
// in-process go-rtmp server (our ingest relay), validating the publish
// handshake (connect, createStream, publish) end to end.
func TestRTMPPublisherAgainstServer(t *testing.T) {
	log.SetLevel(log.WarnLevel) // silence go-rtmp's chatty info logs
	relay, err := ingest.NewServer(ingest.Config{Addr: "127.0.0.1:0", Allowed: []string{"key"}}, log.New())
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = relay.Serve() }()
	defer relay.Close()
	time.Sleep(200 * time.Millisecond)

	url := fmt.Sprintf("rtmp://%s/app/key", relay.Addr())
	pub, err := NewRTMPPublisher(url, log.New())
	if err != nil {
		t.Fatalf("NewRTMPPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Ensure(); err != nil {
		t.Fatalf("publish handshake failed: %v", err)
	}

	// Writing media must not error while connected.
	if err := pub.WriteVideo(0, []byte{0x17, 0x01, 0x01}); err != nil {
		t.Fatalf("WriteVideo: %v", err)
	}
	if err := pub.WriteAudio(0, []byte{0xaf, 0x01, 0x01}); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}

	// Close then re-Ensure must recover (we killed the connection in Close).
	if err := pub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pub.Ensure(); err != nil {
		t.Fatalf("re-Ensure after close: %v", err)
	}
}
