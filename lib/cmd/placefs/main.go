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
	flag.Parse()

	if *hot == "" || *cold == "" || *mountPoint == "" {
		fmt.Fprintln(os.Stderr, "all of -hot, -cold, -mount are required")
		os.Exit(2)
	}
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
	st, err := store.Open(idx)
	if err != nil {
		idx.Close()
		log.Fatalf("store open: %v", err)
	}

	// Old-format detection. We migrate when:
	//   1. The -hot directory still has the legacy .place.db + .place/segments
	//      layout (LooksLikeOldFormat covers both checks), AND
	//   2. The new store is empty (only root inode), so we're not merging
	//      old data into an already-populated new store.
	// The user can pass -skip-migrate to bypass this for any reason
	// (e.g. they've already migrated and just haven't deleted the old files).
	if !*skipMigrate && migrate.LooksLikeOldFormat(*hot) {
		empty, err := storeIsEmpty(st)
		if err != nil {
			st.Close()
			log.Fatalf("store empty check: %v", err)
		}
		if !empty {
			log.Printf("placefs: old-format data detected at %s but new store is non-empty; skipping migration", *hot)
		} else {
			log.Printf("placefs: old-format data detected at %s — running migrator", *hot)
			stats, err := migrate.Run(st, migrate.Options{
				OldHot:  *hot,
				OldCold: *cold,
			})
			if err != nil {
				st.Close()
				log.Fatalf("migrate: %v", err)
			}
			log.Printf("placefs: migration complete — %d files, %d dirs, %d symlinks, %d hardlinks, %d bytes (old data still on disk under .place/; delete when you're satisfied)",
				stats.Files, stats.Dirs, stats.Symlinks, stats.Hardlinks, stats.Bytes)
		}
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
