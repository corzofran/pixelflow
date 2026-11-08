// Comando pixelflow-web: interfaz web local para PixelFlow.
//
//	go run ./cmd/pixelflow-web            → abrir http://localhost:8080
//	go run ./cmd/pixelflow-web -addr :9090 -data data/web
//
// Solo usa la biblioteca estándar (net/http, embed). La página consulta el
// estado con /api/status cada ~200 ms; no hay WebSockets ni dependencias.
//
// Concurrencia del servidor:
//   - net/http atiende cada petición en su propia goroutine.
//   - Todo el estado compartido (ejecución en curso, resultados, historial)
//     vive en server y lo protege server.mu (sync.Mutex). Nunca se mantiene
//     el candado mientras se procesa una imagen: el callback OnResult solo
//     agrega un elemento a un slice.
//   - Hay a lo más una ejecución a la vez. Su goroutine la crea handleRun,
//     termina cuando pipeline.Run regresa y avisa cerrando s.done (único
//     responsable de ese cierre).
//   - Ctrl+C cancela la ejecución en curso y apaga el servidor con un plazo.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
	"github.com/fcorzo/pixelflow/internal/synth"
)

//go:embed ui/index.html
var uiFS embed.FS

type runRequest struct {
	Filtros    string `json:"filtros"`
	Workers    int    `json:"workers"`
	Buffer     int    `json:"buf"`
	Franjas    int    `json:"franjas"`
	Secuencial bool   `json:"secuencial"`
}

type historyEntry struct {
	ID         int        `json:"id"`
	Modo       string     `json:"modo"`
	Filtros    string     `json:"filtros"`
	Workers    int        `json:"workers"`
	Buffer     int        `json:"buf"`
	Franjas    int        `json:"franjas"`
	Imagenes   int        `json:"imagenes"`
	Segundos   float64    `json:"segundos"`
	ImgPorSeg  float64    `json:"img_s"`
	MaxEnVuelo int        `json:"max_en_vuelo"`
	Etapas     [4]float64 `json:"etapas"`
	Estado     string     `json:"estado"`
}

type server struct {
	inDir, outDir string

	mu      sync.Mutex // protege todos los campos de abajo
	running bool
	cancel  context.CancelFunc
	done    chan struct{} // lo cierra la goroutine de la ejecución en curso
	runID   int
	req     runRequest
	listed  int
	started time.Time
	results []pipeline.Result
	errMsg  string
	history []historyEntry
}

func newServer(dataDir string) *server {
	return &server{inDir: filepath.Join(dataDir, "in"), outDir: filepath.Join(dataDir, "out")}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	page, _ := fs.Sub(uiFS, "ui")
	mux.Handle("/", http.FileServer(http.FS(page)))
	mux.HandleFunc("/api/generate", s.handleGenerate)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/cancel", s.handleCancel)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/inputs", s.handleInputs)
	mux.Handle("/img/in/", noCache(http.StripPrefix("/img/in/", http.FileServer(http.Dir(s.inDir)))))
	mux.Handle("/img/out/", noCache(http.StripPrefix("/img/out/", http.FileServer(http.Dir(s.outDir)))))
	return mux
}

func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	var req struct {
		N, W, H  int
		Corrupta int `json:"corrupta"`
	}
	req.Corrupta = -1
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	if req.N < 1 || req.N > 500 || req.W < 16 || req.W > 4000 || req.H < 16 || req.H > 4000 {
		fail(w, http.StatusBadRequest, "valores fuera de rango (n 1-500, tamaño 16-4000)")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock() // mientras se generan no puede empezar una ejecución
	if s.running {
		fail(w, http.StatusConflict, "hay una ejecución en curso")
		return
	}
	os.RemoveAll(s.inDir)
	os.RemoveAll(s.outDir)
	s.results, s.listed, s.errMsg = nil, 0, ""
	paths, err := synth.WriteSet(s.inDir, req.N, req.W, req.H, time.Now().UnixNano()%1000)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	if req.Corrupta >= 0 && req.Corrupta < len(paths) {
		os.WriteFile(paths[req.Corrupta], []byte("no soy una imagen"), 0o644)
	}
	writeJSON(w, http.StatusOK, map[string]int{"generadas": len(paths)})
}

func (s *server) handleInputs(w http.ResponseWriter, r *http.Request) {
	list, err := imgio.List(s.inDir)
	if err != nil {
		list = nil
	}
	names := make([]string, len(list))
	for i, p := range list {
		names[i] = filepath.Base(p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"imagenes": names})
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	chain, err := filters.Parse(req.Filtros)
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Workers < 1 || req.Workers > 64 || req.Buffer < 0 || req.Buffer > 1024 || req.Franjas < 1 || req.Franjas > 64 {
		fail(w, http.StatusBadRequest, "workers 1-64, buf 0-1024, franjas 1-64")
		return
	}
	list, err := imgio.List(s.inDir)
	if err != nil || len(list) == 0 {
		fail(w, http.StatusBadRequest, "no hay imágenes de entrada: genere un conjunto primero")
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		fail(w, http.StatusConflict, "ya hay una ejecución en curso")
		return
	}
	os.RemoveAll(s.outDir)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.running, s.cancel, s.done = true, cancel, done
	s.runID++
	s.req, s.listed, s.results, s.errMsg, s.started = req, len(list), nil, "", time.Now()
	id := s.runID
	s.mu.Unlock()

	cfg := pipeline.Config{
		InputDir: s.inDir, OutputDir: s.outDir, Chain: chain,
		Decoders: req.Workers, Workers: req.Workers, Encoders: req.Workers,
		Buffer: req.Buffer, Strips: req.Franjas,
		OnResult: func(res pipeline.Result) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.results = append(s.results, res)
		},
	}
	runFn := pipeline.Run
	modo := "concurrente"
	if req.Secuencial {
		runFn, modo = pipeline.RunSequential, "secuencial"
	}

	// Goroutine de la ejecución. Dueño: el servidor. Termina cuando
	// pipeline.Run regresa (fin, error o cancelación); cierra done.
	go func() {
		defer close(done)
		defer cancel()
		st, err := runFn(ctx, cfg)
		e := historyEntry{
			ID: id, Modo: modo, Filtros: chain.String(), Workers: req.Workers, Buffer: req.Buffer,
			Franjas: req.Franjas, Imagenes: st.Processed, Segundos: st.Elapsed.Seconds(),
			MaxEnVuelo: st.MaxInFlight, Estado: "completa",
		}
		if req.Secuencial {
			e.Workers, e.Buffer = 1, 0
		}
		if st.Elapsed > 0 {
			e.ImgPorSeg = float64(st.Processed) / st.Elapsed.Seconds()
		}
		for i, b := range st.Busy {
			e.Etapas[i] = b.Seconds()
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case errors.Is(err, context.Canceled):
			e.Estado = "cancelada"
		case err != nil:
			e.Estado = "error"
			s.errMsg = err.Error()
		}
		s.history = append(s.history, e)
		s.running, s.cancel = false, nil
	}()
	writeJSON(w, http.StatusAccepted, map[string]int{"id": id})
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		fail(w, http.StatusConflict, "no hay ejecución en curso")
		return
	}
	cancel()
	writeJSON(w, http.StatusOK, map[string]bool{"cancelada": true})
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := 0.0
	if !s.started.IsZero() {
		if s.running {
			elapsed = time.Since(s.started).Seconds()
		} else if n := len(s.history); n > 0 {
			elapsed = s.history[n-1].Segundos
		}
	}
	outs := make([]string, len(s.results))
	for i, r := range s.results {
		outs[i] = filepath.Base(r.Output)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"running": s.running, "id": s.runID, "listadas": s.listed, "procesadas": len(s.results),
		"salidas": outs, "segundos": elapsed, "error": s.errMsg, "historial": s.history,
		"gomaxprocs": runtime.GOMAXPROCS(0), "cpus": runtime.NumCPU(),
	})
}

// wait bloquea hasta que termine la ejecución en curso (si la hay).
func (s *server) wait() {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

func main() {
	addr := flag.String("addr", "localhost:8080", "dirección de escucha")
	data := flag.String("data", filepath.Join("data", "web"), "carpeta de trabajo (in/ y out/)")
	flag.Parse()

	s := newServer(*data)
	if list, _ := imgio.List(s.inDir); len(list) == 0 {
		if _, err := synth.WriteSet(s.inDir, 60, 800, 600, 1); err != nil {
			log.Fatal(err)
		}
	}
	srv := &http.Server{Addr: *addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	// Goroutine del servidor HTTP: termina cuando Shutdown cierra el listener.
	go func() { errc <- srv.ListenAndServe() }()
	url := *addr
	if strings.HasPrefix(url, ":") {
		url = "localhost" + url
	}
	fmt.Printf("PixelFlow web en http://%s  (Ctrl+C para salir)\n", url)

	select {
	case err := <-errc:
		log.Fatal(err)
	case <-ctx.Done():
	}
	fmt.Println("\napagando: cancelando la ejecución en curso…")
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.wait()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shCtx)
	<-errc
	fmt.Println("listo")
}
