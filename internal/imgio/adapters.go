package imgio

import (
	"context"
	"image"
	"os"
	"sync"
)

// DirSource es el adaptador de entrada de disco: implementa pipeline.Source
// sobre una carpeta. Los identificadores son las rutas de los archivos.
// No tiene estado mutable, así que es seguro para uso concurrente.
type DirSource struct{ Dir string }

// List devuelve las rutas de las imágenes de la carpeta, ordenadas.
func (d DirSource) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return List(d.Dir)
}

// Decode abre y decodifica la imagen id (una ruta devuelta por List).
func (d DirSource) Decode(id string) (image.Image, string, error) { return Decode(id) }

// DirSink es el adaptador de salida de disco: implementa pipeline.Sink.
// Crea la carpeta una sola vez (sync.Once, seguro con varios codificadores)
// y escribe cada imagen de forma atómica con Encode.
type DirSink struct {
	Dir     string
	Quality int // calidad JPEG (1-100); 0 significa 90

	once sync.Once
	err  error
}

// NewDirSink devuelve un DirSink para dir con la calidad JPEG indicada.
func NewDirSink(dir string, quality int) *DirSink {
	return &DirSink{Dir: dir, Quality: quality}
}

// Encode escribe img conservando el nombre base y el formato de id.
func (s *DirSink) Encode(id string, img image.Image, format string) (string, error) {
	s.once.Do(func() { s.err = os.MkdirAll(s.Dir, 0o755) })
	if s.err != nil {
		return "", s.err
	}
	q := s.Quality
	if q <= 0 || q > 100 {
		q = 90
	}
	out := OutputName(s.Dir, id, format)
	return out, Encode(out, img, format, q)
}
