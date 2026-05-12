package place

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	bolt "go.etcd.io/bbolt"
)

const defaultSegmentSize = 256 << 20 // 256MB

// checkOverlap returns an error if the mount point is a prefix of (or equal
// to) a backing storage path, or vice versa.
func checkOverlap(mountDir, hotPath, coldPath string) error {
	mount, err := filepath.Abs(mountDir)
	if err != nil {
		return err
	}
	mount = filepath.Clean(mount) + "/"
	for _, dir := range []struct{ name, path string }{
		{"hot", hotPath},
		{"cold", coldPath},
	} {
		p := filepath.Clean(dir.path) + "/"
		if strings.HasPrefix(p, mount) || strings.HasPrefix(mount, p) {
			return fmt.Errorf("mount %q overlaps with %s storage %q", mountDir, dir.name, dir.path)
		}
	}
	return nil
}

type Server struct {
	fuse    *fuse.Server
	meta    *Meta
	writer  *Writer
	compact *Compactor
	evict   *Evictor
	hotStor *Storage
	hotSegs *SegmentSet
	coldSegs *SegmentSet
	dbg     dbg

	metrics    *Metrics
	metricsCtx context.Context
	metricsCancel context.CancelFunc
	metricsWG  sync.WaitGroup

	closeOnce sync.Once
}

// MetricsHandler returns the HTTP handler that serves Prometheus exposition
// for this Server. Hand it to your http.ServeMux at /metrics.
func (s *Server) MetricsHandler() http.Handler {
	return s.metrics.Handler()
}

// RecoverStaleMount detects and cleans up a stale FUSE mount at the given
// path.
func RecoverStaleMount(path string) error {
	var st syscall.Stat_t
	err := syscall.Stat(path, &st)
	if err == nil {
		return nil
	}
	if err != syscall.ENOTCONN {
		return nil
	}
	log.Printf("place: detected stale mount at %s, recovering...", path)
	out, err := exec.Command("fusermount", "-u", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to recover stale mount at %s: %s: %w", path, out, err)
	}
	log.Printf("place: recovered stale mount at %s", path)
	return nil
}

func Mount(cfg Config) (*Server, error) {
	if err := RecoverStaleMount(cfg.MountDir); err != nil {
		return nil, err
	}

	hot, err := NewStorage(cfg.HotDir)
	if err != nil {
		return nil, err
	}
	cold, err := NewStorage(cfg.ColdDir)
	if err != nil {
		return nil, err
	}

	if err := checkOverlap(cfg.MountDir, hot.Path(), cold.Path()); err != nil {
		return nil, err
	}

	// Ensure .place/ dirs exist under hot (meta.db) and both tiers (segments).
	for _, p := range []string{
		filepath.Join(hot.Path(), ".place"),
	} {
		if err := os.MkdirAll(p, 0700); err != nil {
			return nil, err
		}
	}

	// Resolve DB path.
	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = filepath.Join(hot.Path(), ".place", "meta.db")
	}
	// Migration: rename the old .place.db if present.
	oldDB := filepath.Join(hot.Path(), ".place.db")
	if _, err := os.Stat(oldDB); err == nil {
		log.Printf("place: found legacy %s — removing (new meta.db is a different schema)", oldDB)
		_ = os.Remove(oldDB)
		_ = os.Remove(oldDB + ".lock")
	}

	meta, err := NewMeta(dbPath)
	if err != nil {
		return nil, err
	}

	d := dbg{on: cfg.Debug}
	if cfg.NoPassthrough {
		d.log("--no-passthrough is a no-op; userspace is the only path in this build")
	}

	hotSegs, err := NewSegmentSet(TierHot, hot.Path(), defaultSegmentSize)
	if err != nil {
		meta.Close()
		return nil, err
	}
	coldSegs, err := NewSegmentSet(TierCold, cold.Path(), defaultSegmentSize)
	if err != nil {
		meta.Close()
		return nil, err
	}
	// Wire each set's id allocator through bbolt so a post-crash restart
	// can't reissue an id whose .seg file was unlinked but whose detaching
	// tx rolled back. Must precede any newActive/RotateIfFull call.
	if err := hotSegs.AttachMeta(meta); err != nil {
		meta.Close()
		return nil, fmt.Errorf("attach meta to hot segs: %w", err)
	}
	if err := coldSegs.AttachMeta(meta); err != nil {
		meta.Close()
		return nil, fmt.Errorf("attach meta to cold segs: %w", err)
	}

	if cfg.RebuildMeta {
		log.Printf("place: rebuilding metadata from cold segments...")
		if err := rebuildMetaFromCold(meta, coldSegs); err != nil {
			meta.Close()
			return nil, fmt.Errorf("rebuild meta: %w", err)
		}
	}

	if err := reconcile(meta, hotSegs); err != nil {
		meta.Close()
		return nil, fmt.Errorf("reconcile hot: %w", err)
	}
	if err := reconcile(meta, coldSegs); err != nil {
		meta.Close()
		return nil, fmt.Errorf("reconcile cold: %w", err)
	}

	// Sweep up any inodes whose fm.Rel was left stale by pre-fix
	// DeleteFileTx (hardlink unlink of the primary path didn't repoint
	// Rel). Compaction now reads fm.Rel only after re-fetching via
	// inodeID, but record framing still uses the stored Rel — repair so
	// that name resolves back to the inode for clean accounting.
	if err := RepairOrphanRels(meta); err != nil {
		meta.Close()
		return nil, fmt.Errorf("repair orphan rels: %w", err)
	}

	evictAt := cfg.EvictAt
	if evictAt == 0 {
		evictAt = 0.9
	}
	evictTo := cfg.EvictTo
	if evictTo == 0 {
		evictTo = 0.8
	}
	replicateAfter := cfg.ReplicateAfter
	if replicateAfter <= 0 {
		replicateAfter = defaultReplicateAfter
	}
	replicateMaxBytes := cfg.ReplicateMaxBytes
	if replicateMaxBytes <= 0 {
		replicateMaxBytes = defaultReplicateMaxBytes
	}

	evict := NewEvictor(hot, meta, hotSegs, coldSegs, evictAt, evictTo, d)
	reader := NewReader(hotSegs, coldSegs, meta)
	writer := NewWriter(hotSegs, meta, evict)
	compact := NewCompactor(reader, hotSegs, coldSegs, meta, evict, replicateAfter, replicateMaxBytes, d)
	writer.SetCompactor(compact)

	metrics := NewMetrics()
	writer.SetMetrics(metrics)
	compact.SetMetrics(metrics)
	evict.SetMetrics(metrics)

	root := &placeRoot{
		hot:      hot,
		cold:     cold,
		hotSegs:  hotSegs,
		coldSegs: coldSegs,
		meta:     meta,
		writer:   writer,
		reader:   reader,
		compact:  compact,
		evict:    evict,
		dbg:      d,
		metrics:  metrics,
	}

	mopts := fuse.MountOptions{
		AllowOther: true,
		FsName:     "place",
		Name:       "place",
		Debug:      cfg.FuseDebug,
		MaxWrite:   1 << 20, // go-fuse's MAX_KERNEL_WRITE; raising it has no effect.
	}
	if cfg.WritebackCache {
		// Tell the kernel we support write-back caching; it'll then buffer
		// writes from userspace and ship them to us in MaxWrite-sized
		// async batches.
		mopts.ExtraCapabilities |= fuse.CAP_WRITEBACK_CACHE
	}

	opts := &fs.Options{
		AttrTimeout:     durPtr(time.Second),
		EntryTimeout:    durPtr(time.Second),
		MountOptions:    mopts,
		NullPermissions: true,
	}

	server, err := fs.Mount(cfg.MountDir, root, opts)
	if err != nil {
		meta.Close()
		return nil, err
	}

	evict.Start()
	compact.Start()

	mctx, mcancel := context.WithCancel(context.Background())
	srv := &Server{
		fuse:          server,
		meta:          meta,
		writer:        writer,
		compact:       compact,
		evict:         evict,
		hotStor:       hot,
		hotSegs:       hotSegs,
		coldSegs:      coldSegs,
		dbg:           d,
		metrics:       metrics,
		metricsCtx:    mctx,
		metricsCancel: mcancel,
	}
	srv.startMetricsLoop()
	return srv, nil
}

// startMetricsLoop samples gauges periodically. Counters are bumped at their
// call sites and don't need a sampler.
func (s *Server) startMetricsLoop() {
	s.metricsWG.Add(1)
	go func() {
		defer s.metricsWG.Done()
		// 10s cadence: gauges are read by scrapers at 15s typical, and the
		// underlying bbolt View + statfs are cheap.
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		s.sampleGauges() // initial sample so /metrics has data immediately
		for {
			select {
			case <-s.metricsCtx.Done():
				return
			case <-t.C:
				s.sampleGauges()
			}
		}
	}()
}

// sampleGauges refreshes the gauge collectors from the current Meta and
// Storage state. Logs and swallows errors — a metrics scrape returning stale
// values is preferable to crashing the loop.
func (s *Server) sampleGauges() {
	m := s.metrics
	m.HotUsedFraction.Set(s.hotStor.UsedFraction())

	for _, tier := range []Tier{TierHot, TierCold} {
		lbl := tierLabel(tier)
		segs, err := s.meta.ListSegments(tier)
		if err != nil {
			log.Printf("place: sampleGauges list %s: %v", lbl, err)
			continue
		}
		var total, live int64
		for _, sm := range segs {
			total += sm.Total
			live += sm.Live
		}
		m.Segments.WithLabelValues(lbl).Set(float64(len(segs)))
		m.SegmentTotalBytes.WithLabelValues(lbl).Set(float64(total))
		m.SegmentLiveBytes.WithLabelValues(lbl).Set(float64(live))
	}

	var inodeN int64
	if err := s.meta.ViewLocked(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucketInodes); b != nil {
			inodeN = int64(b.Stats().KeyN)
		}
		return nil
	}); err != nil {
		log.Printf("place: sampleGauges inodes: %v", err)
	} else {
		m.Inodes.Set(float64(inodeN))
	}
}


func durPtr(d time.Duration) *time.Duration { return &d }

// Wait blocks until the FUSE server exits, then runs cleanup.
func (s *Server) Wait() {
	s.fuse.Wait()
	s.shutdown()
}

func (s *Server) Unmount() {
	if err := s.fuse.Unmount(); err != nil {
		s.dbg.log("unmount: %v (may already be unmounted)", err)
	}
	s.shutdown()
}

func (s *Server) shutdown() {
	s.closeOnce.Do(func() {
		log.Println("place: shutting down...")
		if s.metricsCancel != nil {
			s.metricsCancel()
			s.metricsWG.Wait()
		}
		s.compact.Stop()
		s.evict.Stop()
		if err := s.writer.Close(); err != nil {
			log.Printf("place: writer close: %v", err)
		}
		_ = s.hotSegs.CloseAll()
		_ = s.coldSegs.CloseAll()
		if err := s.meta.Close(); err != nil {
			log.Printf("place: meta close: %v", err)
		}
		log.Println("place: shutdown complete")
	})
}
