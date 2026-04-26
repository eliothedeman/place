package place

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type Config struct {
	HotDir   string
	ColdDir  string
	MountDir string
	DBPath   string

	// Debug enables place's own per-op trace (Lookup/Read/Write/Readdir
	// timings, replicate/evict/gc decisions, etc.) via the dbg.log and
	// dbg.op helpers. Loud but not crushing.
	Debug bool
	// FuseDebug enables go-fuse's kernel-protocol trace — the
	// "rx N: READDIRPLUS …" / "tx N: OK" line per FUSE op. Several
	// orders of magnitude noisier than Debug; only useful when
	// reproducing a specific kernel-FUSE issue.
	FuseDebug bool
	// WritebackCache opts into the kernel's FUSE write-back caching
	// (CAP_WRITEBACK_CACHE). Userspace writes of any size land in the
	// page cache; the kernel flushes them to place asynchronously in
	// MaxWrite-sized chunks. This is what lets a tool that issues
	// 4 KB writes (or 16 MB writes) end up sending well-sized 1 MB
	// FUSE WRITE ops to us, with fewer FUSE round-trips and bigger
	// per-record payloads in segments. Trade-offs: stat()'s reported
	// size/mtime can lag committed state by the page cache flush
	// interval. Fine for media/archive workloads, surprising for
	// databases.
	WritebackCache bool
	NoPassthrough  bool // accepted but ignored; userspace is the only path

	// EvictAt is the fraction of hot disk capacity used (0–1) at which
	// eviction begins. Default: 0.9.
	EvictAt float64
	// EvictTo is the target fraction to evict down to. Default: 0.8.
	EvictTo float64

	// ReplicateAfter is the maximum time between replicate passes in steady
	// state. Hot-pressure triggers still fire immediately regardless.
	// Default: 10 minutes.
	ReplicateAfter time.Duration
	// ReplicateMaxBytes is the amount of user data written to hot since the
	// last replicate pass that forces a pass to run. Default: 10 GiB.
	ReplicateMaxBytes int64

	// RebuildMeta scans cold segments on startup and reconstructs bbolt
	// from their inline headers. Use when metadata is lost or corrupt.
	RebuildMeta bool
}

const (
	defaultReplicateAfter    = 10 * time.Minute
	defaultReplicateMaxBytes = int64(10) << 30 // 10 GiB
)

type Storage struct {
	path string
}

func NewStorage(path string) (*Storage, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, syscall.ENOTDIR
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return &Storage{path: abs}, nil
}

func (s *Storage) Path() string { return s.path }

func (s *Storage) FullPath(rel string) string {
	return filepath.Join(s.path, rel)
}

func (s *Storage) Statfs() (syscall.Statfs_t, error) {
	var st syscall.Statfs_t
	err := syscall.Statfs(s.path, &st)
	return st, err
}

// UsedFraction returns hot usage in [0,1].
func (s *Storage) UsedFraction() float64 {
	st, err := s.Statfs()
	if err != nil {
		return 0
	}
	total := st.Blocks * uint64(st.Bsize)
	avail := st.Bavail * uint64(st.Bsize)
	if total == 0 {
		return 0
	}
	return float64(total-avail) / float64(total)
}

// nopDone is a pre-allocated no-op for dbg.op when debug is off.
var nopDone = func(syscall.Errno, ...any) {}

type dbg struct {
	on bool
}

func (d *dbg) log(format string, args ...any) {
	if d.on {
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func (d *dbg) op(name, rel string, startArgs ...any) func(syscall.Errno, ...any) {
	if !d.on {
		return nopDone
	}
	start := time.Now()
	extra := ""
	if len(startArgs) > 0 {
		if f, ok := startArgs[0].(string); ok {
			extra = " " + fmt.Sprintf(f, startArgs[1:]...)
		}
	}
	log.Output(2, fmt.Sprintf("-> %s %q%s", name, rel, extra))
	return func(errno syscall.Errno, endArgs ...any) {
		dt := time.Since(start)
		result := "ok"
		if errno != 0 {
			result = errno.Error()
		}
		suffix := ""
		if len(endArgs) > 0 {
			if f, ok := endArgs[0].(string); ok {
				suffix = " " + fmt.Sprintf(f, endArgs[1:]...)
			}
		}
		log.Output(2, fmt.Sprintf("<- %s %q %s %v%s", name, rel, result, dt, suffix))
	}
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func fs_errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno
	}
	if pe, ok := err.(*os.PathError); ok {
		if errno, ok := pe.Err.(syscall.Errno); ok {
			return errno
		}
	}
	return syscall.EIO
}
