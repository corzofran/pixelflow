package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png" // registro del decodificador PNG para DecodeConfig
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/synth"
)

const (
	maxImages     = 500
	maxUploadSize = 512 << 20 // por petición
	thumbWidth    = 360
)

type imageInfo struct {
	Nombre string `json:"nombre"`
	Bytes  int64  `json:"bytes"`
	Ancho  int    `json:"ancho"`
	Alto   int    `json:"alto"`
	Valida bool   `json:"valida"`
}

// busy responde 409 si hay un trabajo en curso. Las operaciones que cambian
// la carpeta de entrada no pueden ocurrir mientras el pipeline la lee.
func (s *server) busy(w http.ResponseWriter) bool {
	s.mu.Lock()
	job := s.job
	s.mu.Unlock()
	if job != "" {
		fail(w, http.StatusConflict, "Espere a que termine el procesamiento en curso para cambiar las imágenes.")
		return true
	}
	return false
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	list, _ := imgio.List(s.inDir)
	infos := make([]imageInfo, 0, len(list))
	var total int64
	for _, p := range list {
		info := imageInfo{Nombre: filepath.Base(p)}
		if fi, err := os.Stat(p); err == nil {
			info.Bytes = fi.Size()
			total += fi.Size()
		}
		if f, err := os.Open(p); err == nil {
			if cfg, _, err := image.DecodeConfig(f); err == nil {
				info.Ancho, info.Alto, info.Valida = cfg.Width, cfg.Height, true
			}
			f.Close()
		}
		infos = append(infos, info)
	}
	writeJSON(w, http.StatusOK, map[string]any{"imagenes": infos, "bytes": total, "maximo": maxImages})
}

// safeName conserva solo letras, dígitos, '-', '_' y '.' para evitar rutas
// fuera de la carpeta de entrada y nombres problemáticos en Windows.
func safeName(name string) string {
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
	out := strings.TrimLeft(b.String(), ".")
	if out == "" {
		out = "imagen"
	}
	return out
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

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if s.busy(w) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	mr, err := r.MultipartReader()
	if err != nil {
		fail(w, http.StatusBadRequest, "Envíe las imágenes como multipart/form-data.")
		return
	}
	existing, _ := imgio.List(s.inDir)
	count := len(existing)
	var added []string
	var rejected []map[string]string
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			fail(w, http.StatusBadRequest, "La carga se interrumpió o supera 512 MB.")
			return
		}
		if part.FileName() == "" {
			continue
		}
		name := safeName(part.FileName())
		reject := func(why string) {
			rejected = append(rejected, map[string]string{"nombre": part.FileName(), "motivo": why})
		}
		if !imgio.IsImage(name) {
			reject("solo se aceptan JPEG y PNG")
			continue
		}
		if count >= maxImages {
			reject(fmt.Sprintf("se alcanzó el máximo de %d imágenes", maxImages))
			continue
		}
		data, err := io.ReadAll(part)
		if err != nil {
			fail(w, http.StatusBadRequest, "No se pudo leer "+part.FileName()+".")
			return
		}
		if _, _, err := image.DecodeConfig(bytes.NewReader(data)); err != nil {
			reject("el archivo no es una imagen JPEG o PNG válida")
			continue
		}
		dst := uniquePath(s.inDir, name)
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			fail(w, http.StatusInternalServerError, "No se pudo guardar "+name+".")
			return
		}
		added = append(added, filepath.Base(dst))
		count++
	}
	writeJSON(w, http.StatusOK, map[string]any{"agregadas": added, "rechazadas": rejected})
}

func (s *server) handleSamples(w http.ResponseWriter, r *http.Request) {
	if s.busy(w) {
		return
	}
	var req struct {
		N      int  `json:"n"`
		Ancho  int  `json:"ancho"`
		Alto   int  `json:"alto"`
		Danada bool `json:"danada"`
	}
	if err := decodeBody(r, &req); err != nil || req.N < 0 || req.N > maxImages ||
		(req.N > 0 && (req.Ancho < 16 || req.Ancho > 4000 || req.Alto < 16 || req.Alto > 4000)) {
		fail(w, http.StatusBadRequest, "Use de 1 a 500 imágenes de 16 a 4000 píxeles por lado.")
		return
	}
	existing, _ := imgio.List(s.inDir)
	if len(existing)+req.N > maxImages {
		fail(w, http.StatusBadRequest, fmt.Sprintf("Con estas imágenes se superaría el máximo de %d.", maxImages))
		return
	}
	tmp, err := os.MkdirTemp(filepath.Dir(s.inDir), "ejemplos-")
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer os.RemoveAll(tmp)
	paths, err := synth.WriteSet(tmp, req.N, req.Ancho, req.Alto, time.Now().UnixNano()%100000)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	stamp := time.Now().Format("150405")
	n := 0
	for _, p := range paths {
		name := "ejemplo_" + stamp + "_" + filepath.Base(p)[4:]
		if err := os.Rename(p, uniquePath(s.inDir, name)); err == nil {
			n++
		}
	}
	if req.Danada {
		os.WriteFile(uniquePath(s.inDir, "zz_danada_"+stamp+".jpg"), []byte("esto no es una imagen"), 0o644)
		n++
	}
	writeJSON(w, http.StatusOK, map[string]int{"agregadas": n})
}

func decodeBody(r *http.Request, v any) error {
	return jsonDecode(io.LimitReader(r.Body, 1<<20), v)
}

func (s *server) handleClear(w http.ResponseWriter, r *http.Request) {
	if s.busy(w) {
		return
	}
	os.RemoveAll(s.inDir)
	os.RemoveAll(s.outDir)
	os.MkdirAll(s.inDir, 0o755)
	s.mu.Lock()
	s.names, s.stage, s.outputs = nil, nil, nil
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]bool{"vaciado": true})
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if s.busy(w) {
		return
	}
	name := r.PathValue("name")
	if name != filepath.Base(name) || !imgio.IsImage(name) {
		fail(w, http.StatusBadRequest, "Nombre de archivo no válido.")
		return
	}
	if err := os.Remove(filepath.Join(s.inDir, name)); err != nil {
		fail(w, http.StatusNotFound, "La imagen ya no existe.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"eliminada": true})
}

func (s *server) resolve(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.PathValue("name")
	if name != filepath.Base(name) || !imgio.IsImage(name) {
		http.NotFound(w, r)
		return "", false
	}
	switch r.PathValue("dir") {
	case "entrada":
		return filepath.Join(s.inDir, name), true
	case "salida":
		return filepath.Join(s.outDir, name), true
	}
	http.NotFound(w, r)
	return "", false
}

func (s *server) handleImage(w http.ResponseWriter, r *http.Request) {
	p, ok := s.resolve(w, r)
	if !ok {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, p)
}

// handleThumb devuelve una miniatura JPEG de thumbWidth píxeles de ancho,
// generada con el mismo filtro Resize del proyecto y guardada en caché por
// ruta, tamaño y fecha de modificación.
func (s *server) handleThumb(w http.ResponseWriter, r *http.Request) {
	p, ok := s.resolve(w, r)
	if !ok {
		return
	}
	fi, err := os.Stat(p)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	key := fmt.Sprintf("%s|%d|%d", p, fi.Size(), fi.ModTime().UnixNano())
	etag := fmt.Sprintf(`"%x-%x"`, fi.Size(), fi.ModTime().UnixNano())
	w.Header().Set("Cache-Control", "no-cache") // el navegador revalida con ETag
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	s.thumbMu.Lock()
	data, hit := s.thumbs[key]
	s.thumbMu.Unlock()
	if !hit {
		img, _, err := imgio.Decode(p)
		if err != nil {
			http.Error(w, "imagen dañada", http.StatusUnprocessableEntity)
			return
		}
		rgba := filters.ToRGBA(img)
		tw := min(thumbWidth, rgba.Rect.Dx())
		th := max(1, rgba.Rect.Dy()*tw/max(1, rgba.Rect.Dx()))
		var buf bytes.Buffer
		jpeg.Encode(&buf, filters.Resize(rgba, tw, th, 1), &jpeg.Options{Quality: 82})
		data = buf.Bytes()
		s.thumbMu.Lock()
		if len(s.thumbs) > 3000 {
			s.thumbs = map[string][]byte{}
		}
		s.thumbs[key] = data
		s.thumbMu.Unlock()
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Write(data)
}

func (s *server) handleDownload(w http.ResponseWriter, r *http.Request) {
	list, _ := imgio.List(s.outDir)
	if len(list) == 0 {
		fail(w, http.StatusNotFound, "Todavía no hay imágenes procesadas para descargar.")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="pixelflow_resultados.zip"`)
	zw := zip.NewWriter(w)
	for _, p := range list {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		dst, err := zw.Create(filepath.Base(p))
		if err == nil {
			io.Copy(dst, f)
		}
		f.Close()
	}
	zw.Close()
}
