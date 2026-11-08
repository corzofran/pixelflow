package filters

import (
	"bytes"
	"image"
	"image/color"
	"testing"

	"github.com/fcorzo/pixelflow/internal/synth"
)

func fill(w, h int, c color.RGBA) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.SetRGBA(x, y, c)
		}
	}
	return img
}

func expectPixel(t *testing.T, img *image.RGBA, x, y int, want color.RGBA) {
	t.Helper()
	if got := img.RGBAAt(x, y); got != want {
		t.Errorf("pixel (%d,%d) = %v, se esperaba %v", x, y, got, want)
	}
}

func TestGrayscaleKnownValues(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 4, 1))
	src.SetRGBA(0, 0, color.RGBA{255, 0, 0, 255})     // 0.299·255 = 76.2  → 76
	src.SetRGBA(1, 0, color.RGBA{0, 255, 0, 255})     // 0.587·255 = 149.7 → 150
	src.SetRGBA(2, 0, color.RGBA{0, 0, 255, 255})     // 0.114·255 = 29.1  → 29
	src.SetRGBA(3, 0, color.RGBA{100, 150, 200, 128}) // 29.9+88.05+22.8 = 140.75 → 141, alfa intacto
	before := append([]byte(nil), src.Pix...)

	got := Grayscale(src, 1)
	expectPixel(t, got, 0, 0, color.RGBA{76, 76, 76, 255})
	expectPixel(t, got, 1, 0, color.RGBA{150, 150, 150, 255})
	expectPixel(t, got, 2, 0, color.RGBA{29, 29, 29, 255})
	expectPixel(t, got, 3, 0, color.RGBA{141, 141, 141, 128})
	if !bytes.Equal(before, src.Pix) {
		t.Fatal("Grayscale modificó la imagen de entrada")
	}
}

func TestBoxBlurImpulse(t *testing.T) {
	// Impulso blanco al centro de una imagen negra 3×3, r=1.
	// Pasada horizontal: fila central = 255/3 = 85 en las tres columnas
	// (bordes replicados). Pasada vertical: (85+1)/3 = 28 en todo píxel.
	src := fill(3, 3, color.RGBA{0, 0, 0, 255})
	src.SetRGBA(1, 1, color.RGBA{255, 255, 255, 255})
	got := BoxBlur(src, 1, 1)
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			expectPixel(t, got, x, y, color.RGBA{28, 28, 28, 255})
		}
	}
}

func TestBoxBlurUniformAndZeroRadius(t *testing.T) {
	c := color.RGBA{10, 120, 230, 255}
	src := fill(7, 5, c)
	got := BoxBlur(src, 3, 1)
	if !bytes.Equal(got.Pix, src.Pix) {
		t.Fatal("una imagen uniforme debe quedar igual tras el desenfoque")
	}
	cp := BoxBlur(src, 0, 1)
	if !bytes.Equal(cp.Pix, src.Pix) || &cp.Pix[0] == &src.Pix[0] {
		t.Fatal("r=0 debe devolver una copia idéntica e independiente")
	}
}

func TestBoxBlurRowStep(t *testing.T) {
	// Fila 1×5 [0,0,255,0,0] con r=1 → [0,85,85,85,0].
	src := fill(5, 1, color.RGBA{0, 0, 0, 255})
	src.SetRGBA(2, 0, color.RGBA{255, 255, 255, 255})
	got := BoxBlur(src, 1, 1)
	want := []uint8{0, 85, 85, 85, 0}
	for x, v := range want {
		expectPixel(t, got, x, 0, color.RGBA{v, v, v, 255})
	}
}

func TestResizeDownscaleNearest(t *testing.T) {
	// 4×4 → 2×2 toma los píxeles (1,1), (3,1), (1,3), (3,3).
	src := image.NewRGBA(image.Rect(0, 0, 4, 4))
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			src.SetRGBA(x, y, color.RGBA{uint8(x * 10), uint8(y * 10), 0, 255})
		}
	}
	got := Resize(src, 2, 2, 1)
	if got.Rect.Dx() != 2 || got.Rect.Dy() != 2 {
		t.Fatalf("tamaño %v", got.Rect)
	}
	expectPixel(t, got, 0, 0, color.RGBA{10, 10, 0, 255})
	expectPixel(t, got, 1, 0, color.RGBA{30, 10, 0, 255})
	expectPixel(t, got, 0, 1, color.RGBA{10, 30, 0, 255})
	expectPixel(t, got, 1, 1, color.RGBA{30, 30, 0, 255})
}

func TestResizeUpscaleAndIdentity(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 2, 1))
	src.SetRGBA(0, 0, color.RGBA{1, 2, 3, 255})
	src.SetRGBA(1, 0, color.RGBA{4, 5, 6, 255})
	got := Resize(src, 4, 2, 1)
	for y := 0; y < 2; y++ {
		expectPixel(t, got, 0, y, color.RGBA{1, 2, 3, 255})
		expectPixel(t, got, 1, y, color.RGBA{1, 2, 3, 255})
		expectPixel(t, got, 2, y, color.RGBA{4, 5, 6, 255})
		expectPixel(t, got, 3, y, color.RGBA{4, 5, 6, 255})
	}
	id := Resize(src, 0, 0, 1)
	if !bytes.Equal(id.Pix, src.Pix) {
		t.Fatal("tamaño 0 debe conservar la imagen")
	}
}

func TestSobel(t *testing.T) {
	flat := Sobel(fill(4, 4, color.RGBA{90, 90, 90, 255}), 1)
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			expectPixel(t, flat, x, y, color.RGBA{0, 0, 0, 255}) // sin bordes
		}
	}

	// Borde vertical: columnas 0-1 negras y 2-3 blancas → magnitud saturada
	// en las columnas 1 y 2, cero en los extremos.
	edge := fill(4, 3, color.RGBA{0, 0, 0, 255})
	for y := 0; y < 3; y++ {
		edge.SetRGBA(2, y, color.RGBA{255, 255, 255, 255})
		edge.SetRGBA(3, y, color.RGBA{255, 255, 255, 255})
	}
	got := Sobel(edge, 1)
	expectPixel(t, got, 0, 1, color.RGBA{0, 0, 0, 255})
	expectPixel(t, got, 1, 1, color.RGBA{255, 255, 255, 255})
	expectPixel(t, got, 2, 1, color.RGBA{255, 255, 255, 255})
	expectPixel(t, got, 3, 1, color.RGBA{0, 0, 0, 255})
}

// TestStripsIdenticalToSequential verifica que el paralelismo por franjas no
// cambia ni un byte del resultado, para cualquier número de franjas.
func TestStripsIdenticalToSequential(t *testing.T) {
	src := synth.Image(97, 61, 7) // tamaños primos: franjas desiguales
	cases := map[string]func(*image.RGBA, int) *image.RGBA{
		"gray":   Grayscale,
		"blur1":  func(s *image.RGBA, k int) *image.RGBA { return BoxBlur(s, 1, k) },
		"blur4":  func(s *image.RGBA, k int) *image.RGBA { return BoxBlur(s, 4, k) },
		"resize": func(s *image.RGBA, k int) *image.RGBA { return Resize(s, 40, 33, k) },
		"sobel":  Sobel,
	}
	for name, f := range cases {
		ref := f(src, 1)
		for _, k := range []int{2, 3, 4, 8, 61, 200} {
			if got := f(src, k); !bytes.Equal(got.Pix, ref.Pix) {
				t.Errorf("%s con %d franjas difiere de la versión secuencial", name, k)
			}
		}
	}
}

func TestToRGBA(t *testing.T) {
	rgba := image.NewRGBA(image.Rect(0, 0, 2, 2))
	if ToRGBA(rgba) != rgba {
		t.Fatal("no debe copiar una *image.RGBA con origen 0")
	}
	off := image.NewRGBA(image.Rect(5, 5, 7, 8))
	off.SetRGBA(5, 5, color.RGBA{9, 8, 7, 255})
	got := ToRGBA(off)
	if got.Rect != image.Rect(0, 0, 2, 3) {
		t.Fatalf("bounds %v", got.Rect)
	}
	expectPixel(t, got, 0, 0, color.RGBA{9, 8, 7, 255})
	g := image.NewGray(image.Rect(0, 0, 1, 1))
	g.SetGray(0, 0, color.Gray{77})
	expectPixel(t, ToRGBA(g), 0, 0, color.RGBA{77, 77, 77, 255})
}

func TestParse(t *testing.T) {
	good := map[string]string{
		"gray":                     "gray",
		"gray, blur:3 ,resize:50%": "gray,blur:3,resize:50%",
		"blur,resize:64x32,sobel":  "blur:2,resize:64x32,sobel",
		"grises,desenfoque:1,redimensionar:10x10": "gray,blur:1,resize:10x10",
	}
	for in, want := range good {
		c, err := Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if c.String() != want {
			t.Errorf("Parse(%q) = %q, se esperaba %q", in, c, want)
		}
	}
	for _, in := range []string{"", " , ", "magic", "blur:x", "blur:-1", "resize", "resize:0x5", "resize:abc%", "resize:500%"} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) debía fallar", in)
		}
	}
	c, _ := Parse("resize:50%,gray")
	out := c.Apply(synth.Image(10, 6, 1), 1)
	if out.Rect.Dx() != 5 || out.Rect.Dy() != 3 {
		t.Fatalf("resize 50%% dio %v", out.Rect)
	}
}

func BenchmarkFilters(b *testing.B) {
	src := synth.Image(800, 600, 3)
	for _, bc := range []struct {
		name string
		f    func(*image.RGBA, int) *image.RGBA
	}{
		{"gray", Grayscale},
		{"blur2", func(s *image.RGBA, k int) *image.RGBA { return BoxBlur(s, 2, k) }},
		{"resize50", func(s *image.RGBA, k int) *image.RGBA { return Resize(s, 400, 300, k) }},
		{"sobel", Sobel},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bc.f(src, 1)
			}
		})
	}
}
