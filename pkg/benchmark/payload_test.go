package benchmark

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsurePayloadFileReusesMatchingSize(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "payload.bin")
	if err := EnsurePayloadFile(p, 4096); err != nil {
		t.Fatal(err)
	}
	st1, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsurePayloadFile(p, 4096); err != nil {
		t.Fatal(err)
	}
	st2, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if !st1.ModTime().Equal(st2.ModTime()) {
		t.Fatal("expected matching-size payload to be reused")
	}
	if err := EnsurePayloadFile(p, 8192); err != nil {
		t.Fatal(err)
	}
	st3, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st3.Size() != 8192 {
		t.Fatalf("size %d", st3.Size())
	}
}

func TestPrepareOriginManagedAndExternal(t *testing.T) {
	dir := t.TempDir()
	managed := filepath.Join(dir, "origin", "payload.bin")
	got, err := PrepareOrigin(managed, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	if got != managed {
		t.Fatalf("got %s", got)
	}
	external := filepath.Join(dir, "missing.bin")
	if _, err := PrepareOrigin(external, 1024, false); err == nil {
		t.Fatal("expected error for missing external file")
	}
}

func TestIsDefaultPayloadAbs(t *testing.T) {
	if !IsDefaultPayload("") || !IsDefaultPayload(DefaultPayloadPath()) {
		t.Fatal("relative default")
	}
	abs, err := filepath.Abs(DefaultPayloadPath())
	if err != nil {
		t.Fatal(err)
	}
	if !IsDefaultPayload(abs) {
		t.Fatalf("abs %s", abs)
	}
}
