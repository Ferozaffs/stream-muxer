package main

import (
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"streammux/internal/ingest"
)

func main() {
	addr := flag.String("addr", ":1935", "RTMP listen address")
	keys := flag.String("keys", "", "comma-separated allowed publish keys (empty = allow all)")
	flag.Parse()

	logger := log.New()
	logger.SetLevel(log.InfoLevel)
	logger.SetOutput(os.Stdout)

	var allowed []string
	if *keys != "" {
		allowed = strings.Split(*keys, ",")
	}

	srv, err := ingest.NewServer(ingest.Config{Addr: *addr, Allowed: allowed}, logger)
	if err != nil {
		logger.WithError(err).Fatal("create ingest server")
	}
	logger.Infof("ingest listening on %s", srv.Addr())
	if err := srv.Serve(); err != nil {
		logger.WithError(err).Fatal("ingest server stopped")
	}
}
