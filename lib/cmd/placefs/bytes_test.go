package main

import "testing"

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"1024", 1024, false},
		{"1B", 1, false},
		{"1b", 1, false},
		{"1K", 1 << 10, false},
		{"1KB", 1 << 10, false},
		{"1KiB", 1 << 10, false},
		{"1M", 1 << 20, false},
		{"500MB", 500 << 20, false},
		{"2G", 2 << 30, false},
		{"10gb", 10 << 30, false},
		{"1.5GiB", int64(1.5 * (1 << 30)), false},
		{"1T", 1 << 40, false},
		{"500GB", 500 << 30, false},
		{"  10MiB  ", 10 << 20, false},
		{"", 0, true},
		{"K", 0, true},
		{"500XB", 0, true},
		{"-1G", 0, true},
		{"1.2.3GB", 0, true},
	}
	for _, c := range cases {
		got, err := parseBytes(c.in)
		if c.err {
			if err == nil {
				t.Errorf("parseBytes(%q) = %d, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseBytes(%q) errored: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{1024, "1KiB"},
		{1 << 30, "1GiB"},
		{500 << 30, "500GiB"},
		{1234, "1234"}, // not a clean unit, fall back to bytes
	}
	for _, c := range cases {
		if got := formatBytes(c.in); got != c.want {
			t.Errorf("formatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBytesValueSetAndString(t *testing.T) {
	var v bytesValue
	if err := v.Set("2GiB"); err != nil {
		t.Fatal(err)
	}
	if int64(v) != 2<<30 {
		t.Errorf("Set 2GiB → %d, want %d", int64(v), 2<<30)
	}
	if v.String() != "2GiB" {
		t.Errorf("String() = %q, want \"2GiB\"", v.String())
	}
}
