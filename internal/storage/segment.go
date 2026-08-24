package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// Segment files are named after the sequence of their first record, zero-padded
// to 20 digits so that lexical order equals numeric order:
//
//	segment-00000000000000000001.log
//	segment-00000000000001048577.log
//
// Naming by base sequence (rather than 000001, 000002, …) means the segment
// that holds any given sequence can be found from the directory listing alone,
// which replay and catch-up rely on.
const (
	segmentPrefix = "segment-"
	segmentSuffix = ".log"
	segmentDigits = 20
)

type segmentInfo struct {
	base uint64 // sequence of the first record
	path string
	size int64 // bytes; for the active segment, a snapshot
}

func segmentName(base uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, base, segmentSuffix)
}

// parseSegmentName returns the base sequence encoded in a segment file name.
func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, segmentPrefix)
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, segmentSuffix)
	if !ok || len(digits) != segmentDigits {
		return 0, false
	}
	base, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || base == 0 {
		return 0, false
	}
	return base, true
}

// listSegments returns the segment files in dir ordered by base sequence.
// Other files are ignored.
func listSegments(dir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []segmentInfo
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		base, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, segmentInfo{base: base, path: filepath.Join(dir, e.Name()), size: info.Size()})
	}
	slices.SortFunc(segs, func(a, b segmentInfo) int {
		switch {
		case a.base < b.base:
			return -1
		case a.base > b.base:
			return 1
		default:
			return 0
		}
	})
	return segs, nil
}

// syncDir makes a newly created file's directory entry durable.
//
// Windows cannot fsync a directory; NTFS journals metadata changes, so it is
// skipped there.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
