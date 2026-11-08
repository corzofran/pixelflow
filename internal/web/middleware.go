package web

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

type middleware func(http.Handler) http.Handler

// chain aplica los middleware en orden: el primero es el más externo.
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// recoverer convierte un pánico en un handler en una respuesta 500 y lo
// registra con su traza, en lugar de cortar la conexión sin explicación.
func recoverer(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					if v == http.ErrAbortHandler {
						panic(v)
					}
					log.Error("pánico en handler", "ruta", r.URL.Path, "valor", v, "traza", string(debug.Stack()))
					http.Error(w, "error interno", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }

// logger registra cada petición de la API con su duración. Las consultas de
// estado, imágenes y miniaturas son muy frecuentes y solo se registran si
// fallan.
func logger(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
			next.ServeHTTP(rec, r)
			quiet := r.URL.Path == "/api/estado" || !strings.HasPrefix(r.URL.Path, "/api/")
			if quiet && rec.code < 400 {
				return
			}
			lvl := slog.LevelInfo
			if rec.code >= 500 {
				lvl = slog.LevelError
			} else if rec.code >= 400 {
				lvl = slog.LevelWarn
			}
			log.Log(r.Context(), lvl, "petición", "método", r.Method, "ruta", r.URL.Path, "código", rec.code, "duración", time.Since(start).Round(time.Microsecond))
		})
	}
}

// secureHeaders agrega cabeceras de seguridad básicas. La página no carga
// recursos externos, así que la política de contenido es estricta.
func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
		next.ServeHTTP(w, r)
	})
}
