package app

import (
	"archive/zip"
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png" // registro del decodificador PNG para DecodeConfig
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/synth"
)

// Límites de la carpeta de entrada.
const (
	MaxImages  = 500
	ThumbWidth = 360
)

// Carpetas que se pueden consultar.
const (
	DirInput  = "entrada"
	DirOutput = "salida"
)

// ImageInfo describe una imagen de entrada.
type ImageInfo struct {
	Name   string `json:"nombre"`
	Bytes  int64  `json:"bytes"`
	Width  int    `json:"ancho"`
	Height int    `json:"alto"`
	Valid  bool   `json:"valida"`
}

// Rejected es un archivo que no se aceptó al subirlo.
type Rejected struct {
	Name   string `json:"nombre"`
	Reason string `json:"motivo"`
}

// Images lista las imágenes de entrada y su tamaño total.
func (s *Service) Images() ([]ImageInfo, int64) {
	list, _ := imgio.List(s.inDir)
	infos := make([]ImageInfo, 0, len(list))
	var total int64
	for _, p := range list {
		info := ImageInfo{Name: filepath.Base(p)}
		if fi, err := os.Stat(p); err == nil {
			info.Bytes = fi.Size()
			total += fi.Size()
		}
		if f, err := os.Open(p); err == nil {
			if cfg, _, err := image.DecodeConfig(f); err == nil {
				info.Width, info.Height, info.Valid = cfg.Width, cfg.Height, true
			}
			f.Close()
		}
		infos = append(infos, info)
	}
	return infos, total
}

// SafeName conserva solo letras ASCII, dígitos, '-', '_' y '.', de modo que
// el nombre no pueda salir de la carpeta ni causar problemas en Windows.
func SafeName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		}
	}
	if out := strings.TrimLeft(b.String(), "."); out != "" {
		return out
	}
	return "imagen"
}

func uniquePath(dir, name string) string {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	p := filepath.Join(dir, name)
	for i := 2; ; i++ {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			return p
		}
		p = filepath.Join(dir, fmt.Sprintf("%s_%d%s", base, i, ext))
	}
}

// AddImage valida y guarda una imagen subida. Devuelve el nombre final o un
// *Rejected si no se aceptó (no es un error del sistema).
func (s *Service) AddImage(original string, data []byte) (string, *Rejected, error) {
	if err := s.busy(); err != nil {
		return "", nil, err
	}
	name := SafeName(original)
	if !imgio.IsImage(name) {
		return "", &Rejected{original, "solo se aceptan JPEG y PNG"}, nil
	}
	if list, _ := imgio.List(s.inDir); len(list) >= MaxImages {
		return "", &Rejected{original, fmt.Sprintf("se alcanzó el máximo de %d imágenes", MaxImages)}, nil
	}
	if _, _, err := image.DecodeConfig(bytes.NewReader(data)); err != nil {
		return "", &Rejected{original, "el archivo no es una imagen JPEG o PNG válida"}, nil
	}
	dst := uniquePath(s.inDir, name)
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		return "", nil, fmt.Errorf("guardar %s: %w", name, err)
	}
	return filepath.Base(dst), nil, nil
}

// AddSamples genera n imágenes sintéticas y, si damaged, un archivo dañado
// (para demostrar que el primer error cancela todo el pipeline).
func (s *Service) AddSamples(n, w, h int, damaged bool) (int, error) {
	if err := s.busy(); err != nil {
		return 0, err
	}
	if n < 0 || n > MaxImages || (n > 0 && (w < 16 || w > 4000 || h < 16 || h > 4000)) {
		return 0, userErr(ErrInvalid, "Use de 1 a 500 imágenes de 16 a 4000 píxeles por lado.")
	}
	if list, _ := imgio.List(s.inDir); len(list)+n > MaxImages {
		return 0, userErr(ErrInvalid, fmt.Sprintf("Con estas imágenes se superaría el máximo de %d.", MaxImages))
	}
	tmp, err := os.MkdirTemp(filepath.Dir(s.inDir), "ejemplos-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tmp)
	paths, err := synth.WriteSet(tmp, n, w, h, time.Now().UnixNano()%100000)
	if err != nil {
		return 0, err
	}
	stamp := time.Now().Format("150405")
	added := 0
	for _, p := range paths {
		name := "ejemplo_" + stamp + "_" + strings.TrimPrefix(filepath.Base(p), "img_")
		if err := os.Rename(p, uniquePath(s.inDir, name)); err == nil {
			added++
		}
	}
	if damaged {
		if err := os.WriteFile(uniquePath(s.inDir, "zz_danada_"+stamp+".jpg"), []byte("esto no es una imagen"), 0o644); err != nil {
			return added, err
		}
		added++
	}
	return added, nil
}

// RemoveImage elimina una imagen de entrada.
func (s *Service) RemoveImage(name string) error {
	if err := s.busy(); err != nil {
		return err
	}
	if name != filepath.Base(name) || !imgio.IsImage(name) {
		return userErr(ErrInvalid, "Nombre de archivo no válido.")
	}
	if err := os.Remove(filepath.Join(s.inDir, name)); err != nil {
		return userErr(ErrNotFound, "La imagen ya no existe.")
	}
	return nil
}

// Clear elimina todas las imágenes de entrada y de salida.
func (s *Service) Clear() error {
	if err := s.busy(); err != nil {
		return err
	}
	os.RemoveAll(s.inDir)
	os.RemoveAll(s.outDir)
	s.mu.Lock()
	s.names, s.stage, s.outputs = nil, nil, nil
	s.mu.Unlock()
	return os.MkdirAll(s.inDir, 0o755)
}

// Path resuelve el archivo name dentro de la carpeta dir (entrada o salida)
// sin permitir salir de ella.
func (s *Service) Path(dir, name string) (string, error) {
	if name != filepath.Base(name) || !imgio.IsImage(name) {
		return "", ErrNotFound
	}
	switch dir {
	case DirInput:
		return filepath.Join(s.inDir, name), nil
	case DirOutput:
		return filepath.Join(s.outDir, name), nil
	}
	return "", ErrNotFound
}

// Thumbnail devuelve una miniatura JPEG de ThumbWidth píxeles de ancho,
// generada con el filtro Resize del proyecto y guardada en caché. La
// etiqueta (tag) cambia cuando cambia el archivo.
func (s *Service) Thumbnail(dir, name string) (data []byte, tag string, err error) {
	p, err := s.Path(dir, name)
	if err != nil {
		return nil, "", err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return nil, "", ErrNotFound
	}
	tag = fmt.Sprintf(`"%x-%x"`, fi.Size(), fi.ModTime().UnixNano())
	key := p + tag
	s.thumbMu.Lock()
	data, hit := s.thumbs[key]
	s.thumbMu.Unlock()
	if hit {
		return data, tag, nil
	}
	img, _, err := imgio.Decode(p)
	if err != nil {
		return nil, "", userErr(ErrInvalid, "Archivo dañado.")
	}
	rgba := filters.ToRGBA(img)
	tw := min(ThumbWidth, rgba.Rect.Dx())
	th := max(1, rgba.Rect.Dy()*tw/max(1, rgba.Rect.Dx()))
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, filters.Resize(rgba, tw, th, 1), &jpeg.Options{Quality: 82}); err != nil {
		return nil, "", err
	}
	s.thumbMu.Lock()
	if len(s.thumbs) > 3000 {
		s.thumbs = map[string][]byte{}
	}
	s.thumbs[key] = buf.Bytes()
	s.thumbMu.Unlock()
	return buf.Bytes(), tag, nil
}

// HasOutputs indica si hay imágenes procesadas para descargar.
func (s *Service) HasOutputs() bool {
	list, _ := imgio.List(s.outDir)
	return len(list) > 0
}

// WriteZip escribe en w un ZIP con las imágenes procesadas.
func (s *Service) WriteZip(w io.Writer) error {
	list, _ := imgio.List(s.outDir)
	if len(list) == 0 {
		return userErr(ErrNotFound, "Todavía no hay imágenes procesadas para descargar.")
	}
	zw := zip.NewWriter(w)
	for _, p := range list {
		if err := addToZip(zw, p); err != nil {
			return err
		}
	}
	return zw.Close()
}

func addToZip(zw *zip.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dst, err := zw.Create(filepath.Base(path))
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, f)
	return err
}
