// Package filters implementa los filtros de PixelFlow sobre *image.RGBA:
// escala de grises, desenfoque de caja, redimensionado y Sobel (extensión).
//
// Todos los filtros son funciones puras: no modifican la imagen de entrada y
// devuelven una imagen nueva. Por eso pueden ejecutarse desde muchas
// goroutines a la vez sin sincronización: no hay estado compartido mutable.
//
// Cada filtro acepta un parámetro strips (franjas). Con strips > 1 la imagen
// se divide en franjas horizontales que se procesan en paralelo
// (paralelismo de datos dentro de una imagen). El resultado es idéntico,
// byte a byte, para cualquier valor de strips porque cada franja escribe
// únicamente en sus propias filas de la imagen destino.
package filters

import (
	"image"
	"image/draw"
	"sync"
)

// ToRGBA convierte cualquier image.Image a *image.RGBA con origen en (0,0).
// Si la imagen ya cumple esa condición se devuelve sin copiar.
func ToRGBA(src image.Image) *image.RGBA {
	if rgba, ok := src.(*image.RGBA); ok && rgba.Rect.Min == (image.Point{}) {
		return rgba
	}
	b := src.Bounds()
	dst := image.NewRGBA(image.Rect(0, 0, b.Dx(), b.Dy()))
	draw.Draw(dst, dst.Rect, src, b.Min, draw.Src)
	return dst
}

// forRows divide el rango de filas [0,h) en strips franjas y ejecuta fn sobre
// cada una. Con strips <= 1 se ejecuta en la goroutine que llama.
//
// Goroutines: una por franja. Dueño: la goroutine que llama a forRows.
// Terminación: cada una termina al procesar su franja; forRows no regresa
// hasta que todas terminaron (wg.Wait), así que nunca quedan colgadas.
func forRows(h, strips int, fn func(y0, y1 int)) {
	if strips <= 1 || h < 2 {
		fn(0, h)
		return
	}
	if strips > h {
		strips = h
	}
	var wg sync.WaitGroup
	step := (h + strips - 1) / strips
	for y0 := 0; y0 < h; y0 += step {
		y1 := min(y0+step, h)
		wg.Add(1)
		go func(y0, y1 int) {
			defer wg.Done()
			fn(y0, y1)
		}(y0, y1)
	}
	wg.Wait()
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// luma calcula la luminancia ITU-R BT.601 con aritmética entera y redondeo.
func luma(r, g, b uint8) uint8 {
	return uint8((299*int(r) + 587*int(g) + 114*int(b) + 500) / 1000)
}

// Grayscale convierte la imagen a escala de grises (Y = 0.299R+0.587G+0.114B).
// El canal alfa se conserva.
func Grayscale(src *image.RGBA, strips int) *image.RGBA {
	w, h := src.Rect.Dx(), src.Rect.Dy()
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	forRows(h, strips, func(y0, y1 int) {
		for y := y0; y < y1; y++ {
			s := src.Pix[y*src.Stride : y*src.Stride+w*4]
			d := dst.Pix[y*dst.Stride : y*dst.Stride+w*4]
			for i := 0; i < len(s); i += 4 {
				l := luma(s[i], s[i+1], s[i+2])
				d[i], d[i+1], d[i+2], d[i+3] = l, l, l, s[i+3]
			}
		}
	})
	return dst
}

// BoxBlur aplica un desenfoque de caja separable de radio r (ventana 2r+1),
// primero horizontal y después vertical, con bordes replicados y redondeo
// al entero más cercano en cada pasada. Con r <= 0 devuelve una copia.
func BoxBlur(src *image.RGBA, r, strips int) *image.RGBA {
	w, h := src.Rect.Dx(), src.Rect.Dy()
	if r <= 0 {
		dst := image.NewRGBA(image.Rect(0, 0, w, h))
		copy(dst.Pix, src.Pix)
		return dst
	}
	d := 2*r + 1
	half := d / 2
	tmp := image.NewRGBA(image.Rect(0, 0, w, h))
	dst := image.NewRGBA(image.Rect(0, 0, w, h))

	// Pasada horizontal: cada fila es independiente.
	forRows(h, strips, func(y0, y1 int) {
		for y := y0; y < y1; y++ {
			row := src.Pix[y*src.Stride:]
			out := tmp.Pix[y*tmp.Stride:]
			for c := 0; c < 4; c++ {
				sum := 0
				for k := -r; k <= r; k++ {
					sum += int(row[clamp(k, 0, w-1)*4+c])
				}
				for x := 0; x < w; x++ {
					out[x*4+c] = uint8((sum + half) / d)
					// Deslizar la ventana: sale x-r, entra x+r+1.
					sum += int(row[clamp(x+r+1, 0, w-1)*4+c]) - int(row[clamp(x-r, 0, w-1)*4+c])
				}
			}
		}
	})

	// Pasada vertical: necesita tmp completo (barrera implícita: forRows
	// anterior ya regresó). Cada franja mantiene sumas por columna.
	forRows(h, strips, func(y0, y1 int) {
		sums := make([]int, w*4)
		for i := range sums {
			x, c := i/4, i%4
			for k := -r; k <= r; k++ {
				sums[i] += int(tmp.Pix[clamp(y0+k, 0, h-1)*tmp.Stride+x*4+c])
			}
		}
		for y := y0; y < y1; y++ {
			out := dst.Pix[y*dst.Stride:]
			inRow := tmp.Pix[clamp(y+r+1, 0, h-1)*tmp.Stride:]
			outRow := tmp.Pix[clamp(y-r, 0, h-1)*tmp.Stride:]
			for i := range sums {
				out[i] = uint8((sums[i] + half) / d)
				sums[i] += int(inRow[i]) - int(outRow[i])
			}
		}
	})
	return dst
}

// Resize redimensiona a w×h con vecino más cercano muestreando el centro de
// cada píxel destino: sx = ((2x+1)·W) / (2w). Es determinista y exacto, lo
// que permite pruebas píxel a píxel sin tolerancias.
func Resize(src *image.RGBA, w, h, strips int) *image.RGBA {
	sw, sh := src.Rect.Dx(), src.Rect.Dy()
	if w <= 0 || h <= 0 {
		w, h = sw, sh
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	xs := make([]int, w)
	for x := range xs {
		xs[x] = ((2*x + 1) * sw) / (2 * w)
	}
	forRows(h, strips, func(y0, y1 int) {
		for y := y0; y < y1; y++ {
			sy := ((2*y + 1) * sh) / (2 * h)
			srow := src.Pix[sy*src.Stride:]
			drow := dst.Pix[y*dst.Stride:]
			for x, sx := range xs {
				copy(drow[x*4:x*4+4], srow[sx*4:sx*4+4])
			}
		}
	})
	return dst
}

// Sobel (extensión) calcula la magnitud del gradiente |Gx|+|Gy| sobre la
// luminancia, saturada a 255. Devuelve una imagen en grises opaca.
func Sobel(src *image.RGBA, strips int) *image.RGBA {
	w, h := src.Rect.Dx(), src.Rect.Dy()
	lum := make([]int, w*h)
	forRows(h, strips, func(y0, y1 int) {
		for y := y0; y < y1; y++ {
			for x := 0; x < w; x++ {
				p := src.Pix[y*src.Stride+x*4:]
				lum[y*w+x] = int(luma(p[0], p[1], p[2]))
			}
		}
	})
	at := func(x, y int) int { return lum[clamp(y, 0, h-1)*w+clamp(x, 0, w-1)] }
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	forRows(h, strips, func(y0, y1 int) {
		for y := y0; y < y1; y++ {
			for x := 0; x < w; x++ {
				gx := -at(x-1, y-1) - 2*at(x-1, y) - at(x-1, y+1) +
					at(x+1, y-1) + 2*at(x+1, y) + at(x+1, y+1)
				gy := -at(x-1, y-1) - 2*at(x, y-1) - at(x+1, y-1) +
					at(x-1, y+1) + 2*at(x, y+1) + at(x+1, y+1)
				m := uint8(min(abs(gx)+abs(gy), 255))
				i := y*dst.Stride + x*4
				dst.Pix[i], dst.Pix[i+1], dst.Pix[i+2], dst.Pix[i+3] = m, m, m, 255
			}
		}
	})
	return dst
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
