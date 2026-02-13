package place

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/eliothedeman/check"
	"golang.org/x/sys/unix"
)

type (
	Storage interface {
		Path() string
		Open(path string, flag int, mode os.FileMode) (File, error)
		Delete(path string) error
		Size() int64
		Free() int64
	}
	storage struct {
		path string
		size int64
		free int64
	}
	File interface {
		io.Writer
		io.Closer
		io.Seeker
		fs.File
	}
	file struct {
		*os.File
	}
	Dir interface {
		File
		fs.ReadDirFile
	}
	dir struct {
		file
	}
	FS struct {
		layers map[string]Storage
		db     *badger.DB
	}
)

func newStorage(path string) (*storage, error) {
	return check.Catch(func() *storage {
		info := check.Must(os.Stat(path))
		check.Eq(info.IsDir(), true)
		return &storage{
			path: path,
		}
	})
}

func NewFS(dbPath string, paths ...string) (*FS, error) {
	return check.Catch(func() *FS {
		db := check.Must(badger.Open(badger.DefaultOptions(dbPath)))
		f := &FS{
			layers: map[string]Storage{},
			db:     db,
		}
		// TODO: validate the paths are not overlapping
		for _, p := range paths {
			f.layers[p] = check.Must(newStorage(p))
		}
		return f
	})
}

// Delete implements [Storage].
func (s *storage) Delete(path string) error {
	return os.RemoveAll(filepath.Join(s.path, path))
}

// Free implements [Storage].
func (s *storage) Free() int64 {
	var st unix.Statfs_t
	err := unix.Statfs(s.path, &st)
	if err != nil {
		slog.Error("unable to statfs on storage path", "path", s.path, "err", err)
	}
	return int64(st.Bavail * uint64(st.Bsize))
}

// Open implements [Storage].
func (s *storage) Open(path string, flag int, mode os.FileMode) (File, error) {
	f, err := os.OpenFile(filepath.Clean(filepath.Join(s.path, path)), flag, mode)
	if err != nil {
		return nil, err
	}
	return &file{File: f}, nil
}

// Path implements [Storage].
func (s *storage) Path() string {
	return s.path
}

// Size implements [Storage].
func (s *storage) Size() int64 {
	return s.size
}

var (
	filesByPath         = []byte("files_by_path")
	_           Storage = new(storage)
	_           File    = new(file)
	_           Dir     = new(dir)
)

func splitPath(path string) []string {
	return strings.Split(filepath.Clean(path), string(filepath.Separator))
}

type pathMapping struct {
	// StoragePath are all the places this file has been replicated to.
	// This does not contain the relative part of the path
	StoragePaths []string `json:"storage_paths"`
	// RelPath is the actual path both within each storage path and on the mounted path
	RelPath string `json:"rel_path"`
}

func wrap(err error, s string) error {
	return fmt.Errorf("%w: %s", err, s)
}

// func (f *FS) mkdirAll(path string, tx *bbolt.Tx) error {
// }

func (f *FS) Open(path string, flag int, mode os.FileMode) (File, error) {
	if path == "" {
		path = "/"
	}
	// lgr := slog.With(
	// 	"path", path,
	// )

	tx := f.db.NewTransaction(true)
	pm := get[pathMapping](tx, path)
	if pm == nil {
		if os.O_CREATE&flag == os.O_CREATE {
			// randomly pick one layer
			for _, l := range f.layers {
				out, err := l.Open(path, flag, mode)
				if err != nil {
					return nil, err
				}
				pm = &pathMapping{
					StoragePaths: []string{
						l.Path(),
					},
					RelPath: path,
				}
				err = set(tx, path, pm)
				if err != nil {
					out.Close()
					return nil, err
				}
				return out, tx.Commit()
			}
		}
	}
	for _, p := range pm.StoragePaths {
		if s, ok := f.layers[p]; ok {
			return s.Open(pm.RelPath, flag, mode)
		}
		slog.Error("index has storage path which is not configured", "storage_path", p, "rel_path", path)
	}
	return nil, os.ErrNotExist

}

func (f *FS) Delete(path string) error {
	var eout error
	for _, l := range f.layers {
		err := l.Delete(path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			eout = fmt.Errorf("%w %w", eout, err)
		}
	}
	return eout
}

func (f *FS) Size() int64 {
	s := int64(0)
	for _, l := range f.layers {
		s += l.Size()
	}
	return s
}

func (f *FS) Free() int64 {
	s := int64(0)
	for _, l := range f.layers {
		s += l.Free()
	}
	return s
}

/*
func copyKey(ctx context.Context, key Key, src Storage, dst Storage) error {
	r, err := src.Reader(ctx, key)
	if r != nil {
		defer r.Close()
	}
	if err != nil {
		slog.ErrorContext(ctx, "unable to copy", "err", err)
		return err
	}
	w, err := dst.Writer(ctx, key)
	if w != nil {
		defer w.Close()
	}
	if err != nil {
		slog.ErrorContext(ctx, "unable to copy", "err", err)
		return err
	}
	n, err := io.Copy(w, r)
	slog.DebugContext(ctx, "copy complete", "bytes", n, "err", err)
	if err != nil {
		slog.ErrorContext(ctx, "unable to copy", "err", err)
	}
	return err
}

func moveKey(ctx context.Context, key Key, src Storage, dst Storage) error {
	ctx = context.WithValue(ctx, "key", key)
	ctx = context.WithValue(ctx, "action", "move")
	err := copyKey(ctx, key, src, dst)
	if err != nil {
		return err
	}
	return src.Delete(ctx, key)
}
*/
