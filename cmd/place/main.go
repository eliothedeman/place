package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eliothedeman/place"
	"github.com/eliothedeman/quack"
)

type mount struct {
	Hot     string  `help:"path to hot (fast) storage directory"`
	Cold    string  `help:"path to cold (slow) storage directory"`
	Mount   string  `help:"path to FUSE mount point"`
	DB      string  `help:"path to state database (default: {hot}/.place.db)"`
	Debug         bool   `help:"enable place's per-op trace (Lookup/Read/Write/replicate/evict/gc)"`
	FuseDebug     bool   `help:"enable go-fuse's kernel-protocol trace (very loud — one rx/tx line per FUSE op)"`
	NoPassthrough bool   `help:"disable FUSE passthrough I/O"`
	PprofAddr     string `help:"address for pprof HTTP server" default:":6060"`
	EvictAt float64 `help:"hot disk usage fraction to start eviction (0=disabled)" default:"0.9"`
	EvictTo float64 `help:"hot disk usage fraction to evict down to" default:"0.8"`

	ReplicateAfter    string `help:"max time to wait before flushing hot data to cold (e.g. 10m, 1h)" default:"10m"`
	ReplicateMaxBytes string `help:"max hot bytes written before flushing to cold (e.g. 10GB, 512MB)" default:"10GB"`
}

func (m *mount) Help() string {
	return "Mount a hot/cold FUSE filesystem. Writes land in hot storage and are asynchronously replicated to cold."
}

func (m *mount) Validate() error {
	for _, d := range []struct{ name, path string }{
		{"hot", m.Hot},
		{"cold", m.Cold},
		{"mount", m.Mount},
	} {
		if d.path == "" {
			return fmt.Errorf("%s directory is required", d.name)
		}
		info, err := os.Stat(d.path)
		if err != nil {
			return fmt.Errorf("%s dir %q: %w", d.name, d.path, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("%s path %q is not a directory", d.name, d.path)
		}
	}
	return nil
}

func (m *mount) Run(args []string) {
	// Recover any stale mount before validating paths.
	if m.Mount != "" {
		if err := place.RecoverStaleMount(m.Mount); err != nil {
			log.Fatal(err)
		}
	}

	if err := m.Validate(); err != nil {
		log.Fatal(err)
	}

	replicateAfter, err := time.ParseDuration(m.ReplicateAfter)
	if err != nil {
		log.Fatalf("--replicate-after: %v", err)
	}
	replicateMaxBytes, err := parseSize(m.ReplicateMaxBytes)
	if err != nil {
		log.Fatalf("--replicate-max-bytes: %v", err)
	}

	cfg := place.Config{
		HotDir:   m.Hot,
		ColdDir:  m.Cold,
		MountDir: m.Mount,
		DBPath:   m.DB,
		Debug:         m.Debug,
		FuseDebug:     m.FuseDebug,
		NoPassthrough: m.NoPassthrough,
		EvictAt:  m.EvictAt,
		EvictTo:  m.EvictTo,
		ReplicateAfter:    replicateAfter,
		ReplicateMaxBytes: replicateMaxBytes,
	}

	server, err := place.Mount(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if m.PprofAddr != "" {
		go func() {
			log.Printf("place: pprof listening on %s", m.PprofAddr)
			if err := http.ListenAndServe(m.PprofAddr, nil); err != nil {
				log.Printf("place: pprof server: %v", err)
			}
		}()
	}

	log.Printf("place: mounted at %s (hot=%s cold=%s)", m.Mount, m.Hot, m.Cold)

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in background.
	go func() {
		s := <-sig
		log.Printf("place: received %s, unmounting...", s)
		// Run Unmount in its own goroutine. server.Unmount() calls
		// shutdown() synchronously (compact.Stop, writer.Close, meta.Close)
		// which can take a while; if we did it inline here we'd never
		// reach the second <-sig read, defeating the force-exit path.
		go server.Unmount()

		s = <-sig
		log.Printf("place: received %s again, forcing exit", s)
		os.Exit(1)
	}()

	// Blocks until FUSE exits (signal-driven unmount OR external fusermount -u).
	// Cleanup (replicator drain, db close) runs automatically via Wait().
	server.Wait()
}

// parseSize parses a byte size with an optional unit suffix (case-insensitive):
//
//	"10"   → 10
//	"512K" / "512KB" / "512KiB" → 524288
//	"10M"  / "10MB"  / "10MiB"  → 10485760
//	"10G"  / "10GB"  / "10GiB"  → 10737418240
//	"1T"   / "1TB"   / "1TiB"   → 1099511627776
//
// Both decimal and binary units are accepted but treated as power-of-two
// (the difference isn't meaningful at the granularity this setting controls).
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	// Split digits from unit.
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numPart, unitPart := s[:i], strings.ToUpper(strings.TrimSpace(s[i:]))
	n, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("bad number %q: %w", numPart, err)
	}
	var mult float64
	switch unitPart {
	case "", "B":
		mult = 1
	case "K", "KB", "KIB":
		mult = 1 << 10
	case "M", "MB", "MIB":
		mult = 1 << 20
	case "G", "GB", "GIB":
		mult = 1 << 30
	case "T", "TB", "TIB":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("unknown unit %q", unitPart)
	}
	v := n * mult
	if v < 0 {
		return 0, fmt.Errorf("negative size")
	}
	return int64(v), nil
}

type migrateCmd struct {
	DB     string `help:"path to meta.db (default: {hot}/.place/meta.db)"`
	Hot    string `help:"path to hot directory (used to derive default DB path)"`
	DryRun bool   `help:"report what would be migrated without writing"`
}

func (c *migrateCmd) Help() string {
	return "Run pending bbolt schema migrations. Creates a backup in .migrations/ before applying. With --dry-run, prints the plan and exits."
}

func (c *migrateCmd) Run(args []string) {
	dbPath := c.DB
	if dbPath == "" {
		if c.Hot == "" {
			log.Fatal("--db or --hot is required")
		}
		dbPath = c.Hot + "/.place/meta.db"
	}
	if c.DryRun {
		if err := place.DryRunMigrations(dbPath); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := place.RunMigrations(dbPath); err != nil {
		log.Fatal(err)
	}
}

func main() {
	quack.MustBindCobra("place", quack.Map{
		"mount":   new(mount),
		"migrate": new(migrateCmd),
	}).Execute()
}
