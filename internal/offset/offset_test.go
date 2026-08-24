package offset_test

import (
	"os"
	"path/filepath"
	"testing"

	"mdstream/internal/offset"
)

func TestLoadMissingIsZero(t *testing.T) {
	s := offset.NewFileStore(filepath.Join(t.TempDir(), "client.offset"))
	seq, err := s.Load()
	if err != nil || seq != 0 {
		t.Fatalf("Load = %d, %v; want 0, nil", seq, err)
	}
}

func TestSaveLoad(t *testing.T) {
	dir := t.TempDir()
	s := offset.NewFileStore(filepath.Join(dir, "client.offset"))
	for _, seq := range []uint64{1, 123456, 18446744073709551615} {
		if err := s.Save(seq); err != nil {
			t.Fatalf("Save(%d): %v", seq, err)
		}
		got, err := s.Load()
		if err != nil || got != seq {
			t.Fatalf("Load = %d, %v; want %d", got, err, seq)
		}
	}
	// No temporary files are left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("directory has %d entries, want only the offset file", len(entries))
	}
}

func TestLoadRejectsGarbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.offset")
	if err := os.WriteFile(path, []byte("not a number"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := offset.NewFileStore(path).Load(); err == nil {
		t.Fatal("Load accepted garbage")
	}
}
