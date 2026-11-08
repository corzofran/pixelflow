// Package synth genera imágenes sintéticas deterministas para pruebas y
// benchmarks, de modo que no se necesita descargar ningún conjunto de datos.
package synth

import (
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"math/rand"
	"os"
	"path/filepath"
)

// Image crea una imagen w×h con degradados, círculos y ruido, determinada
// por seed (la misma semilla produce exactamente la misma imagen).
func Image(w, h int, seed int64) *image.RGBA {
	rng := rand.New(rand.NewSource(seed))
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	r0, g0, b0 := rng.Intn(256), rng.Intn(256), rng.Intn(256)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			i := y*img.Stride + x*4
			img.Pix[i] = uint8((r0 + x*255/max(1, w)) % 256)
			img.Pix[i+1] = uint8((g0 + y*255/max(1, h)) % 256)
			img.Pix[i+2] = uint8((b0 + (x+y)*128/max(1, w+h)) % 256)
			img.Pix[i+3] = 255
		}
	}
	for c := 0; c < 6; c++ {
		cx, cy := rng.Intn(max(1, w)), rng.Intn(max(1, h))
		rad := 4 + rng.Intn(max(1, min(w, h)/4))
		col := color.RGBA{uint8(rng.Intn(256)), uint8(rng.Intn(256)), uint8(rng.Intn(256)), 255}
		for y := max(0, cy-rad); y < min(h, cy+rad); y++ {
			for x := max(0, cx-rad); x < min(w, cx+rad); x++ {
				if (x-cx)*(x-cx)+(y-cy)*(y-cy) <= rad*rad {
					img.SetRGBA(x, y, col)
				}
			}
		}
	}
	for i := 0; i < w*h/20; i++ {
		x, y := rng.Intn(max(1, w)), rng.Intn(max(1, h))
		v := uint8(rng.Intn(256))
		img.SetRGBA(x, y, color.RGBA{v, v, v, 255})
	}
	return img
}

// WriteSet escribe n imágenes en dir alternando PNG y JPEG. Devuelve las rutas.
func WriteSet(dir string, n, w, h int, seed int64) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	paths := make([]string, 0, n)
	for i := 0; i < n; i++ {
		img := Image(w, h, seed+int64(i))
		ext := ".png"
		if i%2 == 1 {
			ext = ".jpg"
		}
		p := filepath.Join(dir, fmt.Sprintf("img_%04d%s", i, ext))
		if err := writeFile(p, img); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, nil
}

func writeFile(path string, img image.Image) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if filepath.Ext(path) == ".jpg" {
		err = jpeg.Encode(f, img, &jpeg.Options{Quality: 90})
	} else {
		err = png.Encode(f, img)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
