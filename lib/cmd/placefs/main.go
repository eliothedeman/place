// Command placefs wires the L1–L5 layered stack and mounts it as a FUSE
// filesystem. It uses the same -hot / -cold directory flags as the legacy
// binary so upgrading is a drop-in: rerun the new binary with the same
// paths and, if it finds an old-format dataset (.place.db + .place/segments
// under -hot), it migrates everything through Store before mounting.
//
// New on-disk state lives under {hot}/.placefs and {cold}/.placefs, which
// never collides with the legacy ".place/" subtree — both layouts coexist
// on disk until you delete the old files yourself.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eliothedeman/place/lib/fuselayer"
	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/migrate"
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
	skipMigrate := flag.Bool("skip-migrate", false, "do not run the old→new migrator even if old data is detected")
	migOldDB := flag.String("migrate-old-db", "", "explicit path to the legacy DB (overrides auto-detection)")
	migTarget := flag.String("migrate-target", "cold", "tier migrated bytes land in: hot or cold (default cold so a bulk migration doesn't fill the hot drive)")
	migCleanup := flag.Bool("migrate-cleanup", false, "delete legacy .place.db and .place/ tree after a successful migration (default keeps them so you can verify)")
	verbose := flag.Bool("verbose", false, "log every migrated file/dir/symlink and every mover decision")
	flag.Parse()

	if *hot == "" || *cold == "" || *mountPoint == "" {
		fmt.Fprintln(os.Stderr, "all of -hot, -cold, -mount are required")
		os.Exit(2)
	}
	log.Printf("placefs: starting (hot=%s cold=%s mount=%s)", *hot, *cold, *mountPoint)
	for _, p := range []string{*hot, *cold} {
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

	// Old-format detection. Log what we see so a quiet startup is
	// self-explanatory: either the operator can tell migration ran, or they
	// can tell why it didn't.
	if *skipMigrate {
		log.Printf("placefs: skip-migrate set; not scanning for legacy data")
	} else {
		tier, err := parseTier(*migTarget)
		if err != nil {
			st.Close()
			log.Fatalf("placefs: -migrate-target: %v", err)
		}
		runMigration(st, *hot, *cold, *migOldDB, tier, *migCleanup, *verbose)
	}

	mv := mover.Start(mover.Config{
		Index:          idx,
		HotMaxBytes:    *hotMax,
		HotTargetBytes: *hotTarget,
		Tick:           *tick,
	})

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "placefs",
			FsName: "placefs",
		},
	}
	server, err := fs.Mount(*mountPoint, fuselayer.NewRoot(st), opts)
	if err != nil {
		mv.Stop()
		st.Close()
		log.Fatalf("mount: %v", err)
	}
	log.Printf("placefs mounted at %s (hot=%s cold=%s)", *mountPoint, *hot, *cold)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("placefs: signal received, unmounting")
		server.Unmount()
	}()
	server.Wait()
	mv.Stop()
	if err := st.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}

// storeIsEmpty returns true when the new store has no entries other than root.
func storeIsEmpty(s *store.Store) (bool, error) {
	entries, err := s.Readdir(store.RootInode)
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}

// runMigration logs each branch so a quiet startup is self-explanatory.
func runMigration(st *store.Store, hot, cold, explicitDB string, target index.Tier, cleanup, verbose bool) {
	if explicitDB != "" {
		// Operator pinned the legacy DB path explicitly. Run regardless of
		// what auto-detect would have said.
		log.Printf("placefs: -migrate-old-db=%s; running migrator", explicitDB)
		doMigrate(st, hot, cold, explicitDB, target, cleanup, verbose)
		return
	}
	dbPath, dbFound := migrate.FindLegacyDB(hot)
	segDir := hot + "/.place/segments"
	segExists := dirExists(segDir)
	switch {
	case !dbFound && !segExists:
		log.Printf("placefs: no legacy data at %s (looked for %v and %s); nothing to migrate",
			hot, migrate.LegacyDBCandidates(hot), segDir)
		return
	case !dbFound:
		log.Printf("placefs: found %s but no legacy DB (looked for %v); skipping migration",
			segDir, migrate.LegacyDBCandidates(hot))
		return
	case !segExists:
		log.Printf("placefs: found legacy DB at %s but no %s; skipping migration",
			dbPath, segDir)
		return
	}
	empty, err := storeIsEmpty(st)
	if err != nil {
		st.Close()
		log.Fatalf("store empty check: %v", err)
	}
	if !empty {
		log.Printf("placefs: legacy data at %s but new store is non-empty; skipping migration (pass -skip-migrate to silence this)", hot)
		return
	}
	log.Printf("placefs: legacy data found (db=%s, segments=%s) — running migrator (target=%s, cleanup=%v)",
		dbPath, segDir, target, cleanup)
	doMigrate(st, hot, cold, dbPath, target, cleanup, verbose)
}

func doMigrate(st *store.Store, hot, cold, dbPath string, target index.Tier, cleanup, verbose bool) {
	// Always-on logger so the operator sees periodic progress lines even
	// without -verbose. -verbose adds per-file detail on top.
	logf := func(format string, args ...any) {
		log.Printf("placefs: migrate: "+format, args...)
	}
	stats, err := migrate.Run(st, migrate.Options{
		OldHot:     hot,
		OldCold:    cold,
		OldDB:      dbPath,
		TargetTier: target,
		Cleanup:    cleanup,
		Logger:     logf,
		Verbose:    verbose,
	})
	if err != nil {
		st.Close()
		log.Fatalf("migrate: %v", err)
	}
	suffix := "(legacy data left in place — pass -migrate-cleanup to delete)"
	if cleanup {
		suffix = "(legacy files deleted)"
	}
	log.Printf("placefs: migration complete — %d files, %d dirs, %d symlinks, %d hardlinks, %d bytes %s",
		stats.Files, stats.Dirs, stats.Symlinks, stats.Hardlinks, stats.Bytes, suffix)
}

func parseTier(s string) (index.Tier, error) {
	switch s {
	case "hot":
		return index.TierHot, nil
	case "cold":
		return index.TierCold, nil
	default:
		return 0, fmt.Errorf("unknown tier %q (want hot or cold)", s)
	}
}

func dirExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}
