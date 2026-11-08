package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fcorzo/pixelflow/internal/synth"
)

// Prueba de integración del comando completo (flags → pipeline → disco).
func TestCLI(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	if _, err := synth.WriteSet(in, 6, 40, 30, 1); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out")
	prof := filepath.Join(t.TempDir(), "cpu.prof")

	var so, se bytes.Buffer
	code := run([]string{"-in", in, "-out", out, "-filtros", "gray,blur:1,resize:20x15", "-workers", "3", "-cpuprofile", prof}, &so, &se)
	if code != 0 {
		t.Fatalf("código %d, stderr=%s", code, se.String())
	}
	if !strings.Contains(so.String(), "Resumen: 6/6") {
		t.Fatalf("salida inesperada:\n%s", so.String())
	}
	entries, _ := os.ReadDir(out)
	if len(entries) != 6 {
		t.Fatalf("%d archivos de salida", len(entries))
	}
	if fi, err := os.Stat(prof); err != nil || fi.Size() == 0 {
		t.Fatal("no se escribió el perfil de CPU")
	}

	so.Reset()
	if code := run([]string{"-in", in, "-out", out, "-secuencial", "-q"}, &so, &se); code != 0 {
		t.Fatalf("secuencial: código %d", code)
	}
	if !strings.Contains(so.String(), "modo secuencial") {
		t.Fatal(so.String())
	}
}

func TestCLIErrors(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	paths, err := synth.WriteSet(in, 4, 20, 20, 1)
	if err != nil {
		t.Fatal(err)
	}
	var so, se bytes.Buffer
	if code := run([]string{"-filtros", "nada"}, &so, &se); code != 2 {
		t.Fatalf("filtro inválido: código %d", code)
	}
	if code := run([]string{"-bandera-inexistente"}, &so, &se); code != 2 {
		t.Fatalf("flag inválida: código %d", code)
	}
	if code := run([]string{"-in", in, "-out", t.TempDir(), "-timeout", "1ns", "-q"}, &so, &se); code != 124 {
		t.Fatalf("plazo agotado: código %d", code)
	}
	if err := os.WriteFile(paths[1], []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	se.Reset()
	if code := run([]string{"-in", in, "-out", t.TempDir(), "-q"}, &so, &se); code != 1 {
		t.Fatalf("imagen corrupta: código %d", code)
	}
	if !strings.Contains(se.String(), "img_0001.jpg") {
		t.Fatalf("el error debe nombrar el archivo: %s", se.String())
	}
}
