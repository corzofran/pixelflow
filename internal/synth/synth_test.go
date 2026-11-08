package synth

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDeterministic(t *testing.T) {
	a, b, c := Image(50, 40, 3), Image(50, 40, 3), Image(50, 40, 4)
	if !bytes.Equal(a.Pix, b.Pix) {
		t.Fatal("la misma semilla debe dar la misma imagen")
	}
	if bytes.Equal(a.Pix, c.Pix) {
		t.Fatal("semillas distintas deben dar imágenes distintas")
	}
}

func TestWriteSet(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "set")
	paths, err := WriteSet(dir, 4, 16, 12, 1)
	if err != nil || len(paths) != 4 {
		t.Fatal(err, paths)
	}
	if filepath.Ext(paths[0]) != ".png" || filepath.Ext(paths[1]) != ".jpg" {
		t.Fatal("debe alternar PNG y JPEG")
	}
	f := filepath.Join(t.TempDir(), "archivo")
	os.WriteFile(f, nil, 0o644)
	if _, err := WriteSet(filepath.Join(f, "sub"), 1, 2, 2, 1); err == nil {
		t.Fatal("debía fallar al crear la carpeta")
	}
}
