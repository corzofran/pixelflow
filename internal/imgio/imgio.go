// Package imgio agrupa la entrada/salida de imágenes: listar, decodificar y
// codificar archivos JPEG y PNG.
package imgio

import (
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Formatos admitidos.
const (
	FormatPNG  = "png"
	FormatJPEG = "jpeg"
)

// IsImage indica si la extensión del archivo es una imagen admitida.
func IsImage(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".png", ".jpg", ".jpeg":
		return true
	}
	return false
}

// List devuelve, ordenadas por nombre, las rutas de las imágenes admitidas
// en dir (sin recorrer subcarpetas).
func List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("leer carpeta de entrada: %w", err)
	}
	var paths []string
	for _, e := range entries {
		if e.Type().IsRegular() && IsImage(e.Name()) {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// Decode abre y decodifica una imagen. Devuelve también el formato detectado.
func Decode(path string) (image.Image, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	img, format, err := image.Decode(f)
	if err != nil {
		return nil, "", fmt.Errorf("decodificar %s: %w", filepath.Base(path), err)
	}
	return img, format, nil
}

// Encode escribe img en path con el formato indicado. Escribe primero a un
// archivo temporal y lo renombra al final, de modo que una cancelación a
// mitad de la escritura nunca deja un archivo de salida truncado.
func Encode(path string, img image.Image, format string, quality int) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".pixelflow-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	switch format {
	case FormatJPEG:
		err = jpeg.Encode(tmp, img, &jpeg.Options{Quality: quality})
	default:
		enc := png.Encoder{CompressionLevel: png.DefaultCompression}
		err = enc.Encode(tmp, img)
	}
	if err != nil {
		return fmt.Errorf("codificar %s: %w", filepath.Base(path), err)
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// OutputName devuelve el nombre de salida conservando el nombre base y el
// formato de entrada.
func OutputName(outDir, inPath, format string) string {
	base := strings.TrimSuffix(filepath.Base(inPath), filepath.Ext(inPath))
	ext := ".png"
	if format == FormatJPEG {
		ext = ".jpg"
	}
	return filepath.Join(outDir, base+ext)
}
