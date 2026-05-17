package store

import "os"

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out, nil
}

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
}
