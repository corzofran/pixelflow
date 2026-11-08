// Package web es el adaptador HTTP de PixelFlow: traduce peticiones a
// llamadas de app.Service y errores de la capa de servicio a códigos HTTP.
// No contiene reglas de negocio ni toca el sistema de archivos directamente.
package web

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/fcorzo/pixelflow/internal/app"
)

//go:embed ui
var uiFS embed.FS

const (
	maxUploadBytes = 512 << 20 // por petición
	maxJSONBytes   = 1 << 20
)

type handler struct {
	svc *app.Service
	log *slog.Logger
}

// New devuelve el http.Handler completo: rutas y middleware (recuperación de
// pánicos, registro de peticiones y cabeceras de seguridad).
func New(svc *app.Service, log *slog.Logger) http.Handler {
	h := &handler{svc: svc, log: log}
	mux := http.NewServeMux()
	page, _ := fs.Sub(uiFS, "ui")
	mux.Handle("GET /", http.FileServer(http.FS(page)))
	mux.HandleFunc("GET /api/imagenes", h.listImages)
	mux.HandleFunc("POST /api/imagenes", h.upload)
	mux.HandleFunc("DELETE /api/imagenes", h.clear)
	mux.HandleFunc("DELETE /api/imagenes/{name}", h.remove)
	mux.HandleFunc("POST /api/ejemplos", h.samples)
	mux.HandleFunc("POST /api/ejecutar", h.run)
	mux.HandleFunc("POST /api/medir", h.measure)
	mux.HandleFunc("POST /api/cancelar", h.cancel)
	mux.HandleFunc("GET /api/estado", h.status)
	mux.HandleFunc("GET /api/descargar", h.download)
	mux.HandleFunc("GET /miniatura/{dir}/{name}", h.thumbnail)
	mux.HandleFunc("GET /imagen/{dir}/{name}", h.image)
	return chain(mux, recoverer(log), logger(log), secureHeaders)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// fail traduce un error de la capa de servicio a una respuesta JSON.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, app.ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, app.ErrBusy):
		code = http.StatusConflict
	case errors.Is(err, app.ErrNotFound):
		code = http.StatusNotFound
	}
	msg := err.Error()
	var ue *app.UserError
	if code == http.StatusInternalServerError && !errors.As(err, &ue) {
		h.log.Error("error interno", "ruta", r.URL.Path, "error", err)
		msg = "Ocurrió un error interno; revise la terminal donde corre PixelFlow."
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

func decodeJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(io.LimitReader(r.Body, maxJSONBytes)).Decode(v); err != nil {
		return &app.UserError{Kind: app.ErrInvalid, Msg: "La solicitud no es JSON válido."}
	}
	return nil
}

func (h *handler) listImages(w http.ResponseWriter, r *http.Request) {
	imgs, total := h.svc.Images()
	writeJSON(w, http.StatusOK, map[string]any{"imagenes": imgs, "bytes": total, "maximo": app.MaxImages})
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		h.fail(w, r, &app.UserError{Kind: app.ErrInvalid, Msg: "Envíe las imágenes como multipart/form-data."})
		return
	}
	added, rejected := []string{}, []app.Rejected{}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.fail(w, r, &app.UserError{Kind: app.ErrInvalid, Msg: "La carga se interrumpió o supera 512 MB."})
			return
		}
		if part.FileName() == "" {
			continue
		}
		data, err := io.ReadAll(part)
		if err != nil {
			h.fail(w, r, &app.UserError{Kind: app.ErrInvalid, Msg: "No se pudo leer " + part.FileName() + "."})
			return
		}
		name, rej, err := h.svc.AddImage(part.FileName(), data)
		switch {
		case err != nil:
			h.fail(w, r, err)
			return
		case rej != nil:
			rejected = append(rejected, *rej)
		default:
			added = append(added, name)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"agregadas": added, "rechazadas": rejected})
}

func (h *handler) samples(w http.ResponseWriter, r *http.Request) {
	var req struct {
		N       int  `json:"n"`
		Width   int  `json:"ancho"`
		Height  int  `json:"alto"`
		Damaged bool `json:"danada"`
	}
	if err := decodeJSON(r, &req); err != nil {
		h.fail(w, r, err)
		return
	}
	n, err := h.svc.AddSamples(req.N, req.Width, req.Height, req.Damaged)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"agregadas": n})
}

func (h *handler) remove(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.RemoveImage(r.PathValue("name")); err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"eliminada": true})
}

func (h *handler) clear(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Clear(); err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"vaciado": true})
}

func (h *handler) run(w http.ResponseWriter, r *http.Request) {
	var req app.RunRequest
	if err := decodeJSON(r, &req); err != nil {
		h.fail(w, r, err)
		return
	}
	id, err := h.svc.StartRun(req)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.log.Info("ejecución iniciada", "id", id, "filtros", req.Filters, "workers", req.Workers, "buf", req.Buffer, "secuencial", req.Sequential)
	writeJSON(w, http.StatusAccepted, map[string]int{"id": id})
}

func (h *handler) measure(w http.ResponseWriter, r *http.Request) {
	var req app.MeasureRequest
	if err := decodeJSON(r, &req); err != nil {
		h.fail(w, r, err)
		return
	}
	if err := h.svc.StartMeasure(req); err != nil {
		h.fail(w, r, err)
		return
	}
	h.log.Info("medición iniciada", "filtros", req.Filters, "repeticiones", req.Reps)
	writeJSON(w, http.StatusAccepted, map[string]bool{"iniciada": true})
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Cancel(); err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"cancelado": true})
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.Snapshot())
}

func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	if !h.svc.HasOutputs() {
		h.fail(w, r, &app.UserError{Kind: app.ErrNotFound, Msg: "Todavía no hay imágenes procesadas para descargar."})
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="pixelflow_resultados.zip"`)
	if err := h.svc.WriteZip(w); err != nil {
		h.log.Error("zip", "error", err) // la respuesta ya empezó: solo se registra
	}
}

func (h *handler) thumbnail(w http.ResponseWriter, r *http.Request) {
	data, tag, err := h.svc.Thumbnail(r.PathValue("dir"), r.PathValue("name"))
	if err != nil {
		if errors.Is(err, app.ErrInvalid) {
			http.Error(w, "imagen dañada", http.StatusUnprocessableEntity)
			return
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache") // el navegador revalida con ETag
	w.Header().Set("ETag", tag)
	if r.Header.Get("If-None-Match") == tag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Write(data)
}

func (h *handler) image(w http.ResponseWriter, r *http.Request) {
	p, err := h.svc.Path(r.PathValue("dir"), r.PathValue("name"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, p)
}
