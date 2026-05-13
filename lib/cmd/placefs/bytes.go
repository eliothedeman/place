package main

import (
	"fmt"
	"strconv"
	"strings"
)

// bytesValue is a flag.Value that parses human-readable byte sizes like
// "10G", "500GB", "1.5TiB", or a raw integer count.
//
// Suffixes are case-insensitive. The optional trailing "B" and the
// "iB" variant are accepted but have no semantic effect — all multi-
// character suffixes are 1024-based, matching `du -h`, `df -h`, and
// every other Linux filesystem tool. If you want precision, write the
// exact byte count.
//
// Accepted: 0 | 1024 | 1K | 1KB | 1KiB | 1.5MiB | 2g | 500GB | 1tb
type bytesValue int64

func (b *bytesValue) String() string {
	if b == nil {
		return "0"
	}
	return formatBytes(int64(*b))
}

func (b *bytesValue) Set(s string) error {
	v, err := parseBytes(s)
	if err != nil {
		return err
	}
	*b = bytesValue(v)
	return nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}
	// Split off the suffix: trailing run of letters.
	end := len(s)
	for end > 0 {
		c := s[end-1]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			end--
			continue
		}
		break
	}
	numPart := strings.TrimSpace(s[:end])
	suf := strings.ToLower(strings.TrimSpace(s[end:]))

	if numPart == "" {
		return 0, fmt.Errorf("no numeric prefix in %q", s)
	}
	var unit int64
	switch suf {
	case "", "b":
		unit = 1
	case "k", "kb", "kib":
		unit = 1 << 10
	case "m", "mb", "mib":
		unit = 1 << 20
	case "g", "gb", "gib":
		unit = 1 << 30
	case "t", "tb", "tib":
		unit = 1 << 40
	case "p", "pb", "pib":
		unit = 1 << 50
	default:
		return 0, fmt.Errorf("unknown size suffix %q in %q (want one of B, K[B|iB], M[B|iB], G[B|iB], T[B|iB], P[B|iB])", suf, s)
	}

	// Try int first to preserve full precision for "1234567".
	if n, err := strconv.ParseInt(numPart, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("negative size %q", s)
		}
		return n * unit, nil
	}
	// Fall back to float for "1.5GiB" etc.
	f, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q in %q", numPart, s)
	}
	if f < 0 {
		return 0, fmt.Errorf("negative size %q", s)
	}
	return int64(f * float64(unit)), nil
}

// formatBytes renders n with the largest unit that yields a value >= 1
// and ≤ 3 decimal places. Used for flag default printout in -help.
func formatBytes(n int64) string {
	if n == 0 {
		return "0"
	}
	units := []struct {
		div  int64
		name string
	}{
		{1 << 50, "PiB"},
		{1 << 40, "TiB"},
		{1 << 30, "GiB"},
		{1 << 20, "MiB"},
		{1 << 10, "KiB"},
	}
	for _, u := range units {
		if n%u.div == 0 {
			return fmt.Sprintf("%d%s", n/u.div, u.name)
		}
	}
	return strconv.FormatInt(n, 10)
}
