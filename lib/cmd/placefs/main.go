// Command placefs wires the L1–L5 layered stack and mounts it as a FUSE
// filesystem. On-disk state lives under {hot}/.placefs and {cold}/.placefs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eliothedeman/place/lib/admin"
	"github.com/eliothedeman/place/lib/fuselayer"
	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/mover"
	"github.com/eliothedeman/place/lib/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	hot := flag.String("hot", "", "hot (fast) storage directory")
	cold := flag.String("cold", "", "cold (slow) storage directory")
	mountPoint := flag.String("mount", "", "FUSE mount point")
	dbPath := flag.String("db", "", "DB path (default {hot}/.placefs/db.bolt)")
	hotMax := flag.Int64("hot-max-bytes", 0, "max hot bytes before evict kicks in (0 disables eviction)")
	hotTarget := flag.Int64("hot-target-bytes", 0, "target hot bytes after eviction")
	tick := flag.Duration("tick", 30*time.Second, "mover tick interval")
	stripeSize := flag.Int64("stripe", index.DefaultStripeSize, "stripe size in bytes (power of two)")
	segMax := flag.Int64("segment-max", index.DefaultSegmentMaxSize, "segment rotation size in bytes")
	adminAddr := flag.String("admin-addr", ":9090", `host:port for /metrics + /healthz (empty disables the admin server)`)
	healthMaxGap := flag.Duration("health-max-tick-gap", 5*time.Minute, "max gap since last mover tick before /healthz reports unhealthy (0 disables the check)")
	shutdownGrace := flag.Duration("shutdown-grace", 30*time.Second, "max time to wait for FUSE unmount + mover stop on SIGTERM")
	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := flag.String("log-format", "text", "log format: text|json")
	debugPprof := flag.Bool("debug-pprof", false, "expose /debug/pprof on the admin server (off by default)")
	slowOpThreshold := flag.Duration("slow-op-threshold", 1*time.Second, "FUSE operations slower than this log at debug level (0 disables)")
	flag.Parse()

	logger, err := makeLogger(*logLevel, *logFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	slog.SetDefault(logger)

	if *hot == "" || *cold == "" || *mountPoint == "" {
		fmt.Fprintln(os.Stderr, "all of -hot, -cold, -mount are required")
		os.Exit(2)
	}
	logger.Info("starting", "hot", *hot, "cold", *cold, "mount", *mountPoint)
	for _, p := range []string{*hot, *cold, *mountPoint} {
		if err := os.MkdirAll(p, 0o700); err != nil {
			logger.Error("mkdir failed", "path", p, "err", err)
			os.Exit(1)
		}
	}

	idx, err := index.Open(index.Config{
		HotDir:         *hot,
		ColdDir:        *cold,
		DBPath:         *dbPath,
		StripeSize:     *stripeSize,
		SegmentMaxSize: *segMax,
	})
	if err != nil {
		logger.Error("index open failed", "err", err)
		os.Exit(1)
	}
	logger.Info("index open", "stripe", *stripeSize, "segment_max", *segMax)
	st, err := store.Open(idx)
	if err != nil {
		idx.Close()
		logger.Error("store open failed", "err", err)
		os.Exit(1)
	}
	logger.Info("store open")

	fuseMetrics := fuselayer.NewMetrics()

	mv := mover.Start(mover.Config{
		Index:          idx,
		HotMaxBytes:    *hotMax,
		HotTargetBytes: *hotTarget,
		Tick:           *tick,
		Logger:         logger,
	})

	startTime := time.Now()
	adminCtx, cancelAdmin := context.WithCancel(context.Background())
	defer cancelAdmin()
	if *adminAddr != "" {
		if _, err := admin.Serve(adminCtx, *adminAddr, admin.Deps{
			Index:            idx,
			Mover:            mv,
			Fuse:             fuseMetrics,
			HealthMaxTickGap: *healthMaxGap,
			StartTime:        startTime,
			Logger:           logger,
			EnablePprof:      *debugPprof,
		}); err != nil {
			mv.Stop()
			st.Close()
			logger.Error("admin server failed", "err", err)
			os.Exit(1)
		}
		logger.Info("admin server listening", "addr", *adminAddr, "pprof", *debugPprof)
	} else if *debugPprof {
		// pprof without admin server: stand-alone listener on :6060.
		go func() {
			if err := http.ListenAndServe("127.0.0.1:6060", nil); err != nil {
				logger.Error("pprof listener failed", "err", err)
			}
		}()
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "placefs",
			FsName: "placefs",
		},
	}
	root := fuselayer.NewRoot(st, fuselayer.Options{
		Metrics:         fuseMetrics,
		Logger:          logger,
		SlowOpThreshold: *slowOpThreshold,
	})
	server, err := fs.Mount(*mountPoint, root, opts)
	if err != nil {
		cancelAdmin()
		mv.Stop()
		st.Close()
		logger.Error("mount failed", "err", err)
		os.Exit(1)
	}
	logger.Info("mounted", "mount", *mountPoint, "hot", *hot, "cold", *cold)
	admin.MarkReady()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logger.Info("signal received, unmounting", "signal", sig.String(), "grace", *shutdownGrace)
		done := make(chan struct{})
		go func() {
			server.Unmount()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(*shutdownGrace):
			logger.Error("shutdown grace exceeded; forcing exit")
			os.Exit(1)
		}
	}()
	server.Wait()
	cancelAdmin()
	mv.Stop()
	if err := st.Close(); err != nil {
		logger.Error("close failed", "err", err)
	}
}

func makeLogger(level, format string) (*slog.Logger, error) {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "info":
		lv = slog.LevelInfo
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid -log-level %q (want debug|info|warn|error)", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "text", "":
		h = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("invalid -log-format %q (want text|json)", format)
	}
	return slog.New(h), nil
}
