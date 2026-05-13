// Command placefs wires the L1–L5 layered stack and mounts it as a FUSE
// filesystem. On-disk state lives under {hot}/.placefs and {cold}/.placefs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	flag.Parse()

	if *hot == "" || *cold == "" || *mountPoint == "" {
		fmt.Fprintln(os.Stderr, "all of -hot, -cold, -mount are required")
		os.Exit(2)
	}
	log.Printf("placefs: starting (hot=%s cold=%s mount=%s)", *hot, *cold, *mountPoint)
	for _, p := range []string{*hot, *cold, *mountPoint} {
		if err := os.MkdirAll(p, 0o700); err != nil {
			log.Fatalf("mkdir %s: %v", p, err)
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
		log.Fatalf("index open: %v", err)
	}
	log.Printf("placefs: index open (stripe=%d segment-max=%d)", *stripeSize, *segMax)
	st, err := store.Open(idx)
	if err != nil {
		idx.Close()
		log.Fatalf("store open: %v", err)
	}
	log.Printf("placefs: store open")

	mv := mover.Start(mover.Config{
		Index:          idx,
		HotMaxBytes:    *hotMax,
		HotTargetBytes: *hotTarget,
		Tick:           *tick,
	})

	startTime := time.Now()
	adminCtx, cancelAdmin := context.WithCancel(context.Background())
	defer cancelAdmin()
	if *adminAddr != "" {
		if _, err := admin.Serve(adminCtx, *adminAddr, admin.Deps{
			Index:            idx,
			Mover:            mv,
			HealthMaxTickGap: *healthMaxGap,
			StartTime:        startTime,
		}); err != nil {
			mv.Stop()
			st.Close()
			log.Fatalf("admin server: %v", err)
		}
		log.Printf("placefs: admin server on %s (/healthz /metrics)", *adminAddr)
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "placefs",
			FsName: "placefs",
		},
	}
	server, err := fs.Mount(*mountPoint, fuselayer.NewRoot(st), opts)
	if err != nil {
		cancelAdmin()
		mv.Stop()
		st.Close()
		log.Fatalf("mount: %v", err)
	}
	log.Printf("placefs mounted at %s (hot=%s cold=%s)", *mountPoint, *hot, *cold)
	admin.MarkReady()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("placefs: %s received, unmounting (grace=%s)", sig, *shutdownGrace)
		done := make(chan struct{})
		go func() {
			server.Unmount()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(*shutdownGrace):
			log.Printf("placefs: shutdown grace exceeded; forcing exit")
			os.Exit(1)
		}
	}()
	server.Wait()
	cancelAdmin()
	mv.Stop()
	if err := st.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
