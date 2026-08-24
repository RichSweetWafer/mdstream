package main

import "testing"

func TestParseSize(t *testing.T) {
	ok := map[string]int64{
		"4096":   4096,
		"64KB":   64 << 10,
		"256MB":  256 << 20,
		"1gb":    1 << 30,
		" 2 MB ": 2 << 20,
		"100B":   100,
	}
	for in, want := range ok {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "MB", "-1MB", "0", "1.5MB", "ten"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) accepted", bad)
		}
	}
}
