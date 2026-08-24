// Package offset persists a consumer's position in the stream: the sequence of
// the last event it has fully processed.
//
// Offsets are tracked on the client side. The server only guarantees that every
// published event is in the event log and can be found by sequence; a consumer
// that knows its last processed sequence can therefore resume after it
// (sequence-based recovery, a later stage).
package offset

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FileStore keeps one offset in a small text file.
//
// Save writes a temporary file, fsyncs it and renames it over the old one, so
// a crash leaves either the previous or the new offset, never a torn value.
type FileStore struct {
	path string
}

// NewFileStore returns a store backed by path. The file is created on first Save.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Path returns the file path.
func (s *FileStore) Path() string { return s.path }

// Load returns the stored sequence, or 0 if nothing has been saved yet.
func (s *FileStore) Load() (uint64, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("offset: read %s: %w", s.path, err)
	}
	seq, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("offset: %s: invalid content %q", s.path, b)
	}
	return seq, nil
}

// Save durably records seq as the last processed sequence.
func (s *FileStore) Save(seq uint64) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("offset: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.WriteString(strconv.FormatUint(seq, 10) + "\n"); err != nil {
		tmp.Close()
		return fmt.Errorf("offset: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("offset: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("offset: close: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("offset: rename: %w", err)
	}
	return nil
}
