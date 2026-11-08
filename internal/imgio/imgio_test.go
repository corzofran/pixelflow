package imgio

import (
	"image"
	"image/color"
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTripAndList(t *testing.T) {
	dir := t.TempDir()
	img := image.NewRGBA(image.Rect(0, 0, 3, 2))
	img.SetRGBA(1, 1, color.RGBA{10, 20, 30, 255})
	png := filepath.Join(dir, "b.png")
	if err := Encode(png, img, FormatPNG, 90); err != nil {
		t.Fatal(err)
	}
	if err := Encode(filepath.Join(dir, "a.jpg"), img, FormatJPEG, 90); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "notas.txt"), []byte("x"), 0o644)
	os.Mkdir(filepath.Join(dir, "sub.png"), 0o755)

	list, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || filepath.Base(list[0]) != "a.jpg" || filepath.Base(list[1]) != "b.png" {
		t.Fatalf("listado inesperado: %v", list)
	}
	got, format, err := Decode(png)
	if err != nil || format != FormatPNG {
		t.Fatalf("decode: %v %s", err, format)
	}
	for y := 0; y < 2; y++ {
		for x := 0; x < 3; x++ {
			if color.RGBAModel.Convert(got.At(x, y)) != img.RGBAAt(x, y) {
				t.Fatalf("PNG no es sin pérdida en (%d,%d)", x, y)
			}
		}
	}
	if _, f, err := Decode(list[0]); err != nil || f != FormatJPEG {
		t.Fatalf("jpeg: %v %s", err, f)
	}
	tmps, _ := filepath.Glob(filepath.Join(dir, ".pixelflow-*"))
	if len(tmps) != 0 {
		t.Fatal("quedaron temporales")
	}
}

func TestErrorsAndNames(t *testing.T) {
	dir := t.TempDir()
	if _, err := List(filepath.Join(dir, "nada")); err == nil {
		t.Error("List de carpeta inexistente debía fallar")
	}
	if _, _, err := Decode(filepath.Join(dir, "nada.png")); err == nil {
		t.Error("Decode de archivo inexistente debía fallar")
	}
	bad := filepath.Join(dir, "x.png")
	os.WriteFile(bad, []byte("basura"), 0o644)
	if _, _, err := Decode(bad); err == nil {
		t.Error("Decode de basura debía fallar")
	}
	if err := Encode(filepath.Join(dir, "no", "existe.png"), image.NewRGBA(image.Rect(0, 0, 1, 1)), FormatPNG, 90); err == nil {
		t.Error("Encode a carpeta inexistente debía fallar")
	}
	if err := Encode(filepath.Join(dir, "vacia.png"), image.NewRGBA(image.Rect(0, 0, 0, 0)), FormatPNG, 90); err == nil {
		t.Error("Encode de imagen vacía debía fallar")
	}
	if OutputName("o", "in/foto.JPEG", FormatJPEG) != filepath.Join("o", "foto.jpg") ||
		OutputName("o", "in/x.png", FormatPNG) != filepath.Join("o", "x.png") {
		t.Error("OutputName")
	}
	if !IsImage("A.PNG") || IsImage("a.gif") {
		t.Error("IsImage")
	}
}
