package main

// Concurrencia del servidor web
//
//   - net/http atiende cada petición en su propia goroutine.
//   - Todo el estado compartido vive en server y lo protege server.mu
//     (sync.Mutex). Las secciones críticas solo copian o actualizan
//     contadores y slices; nunca se procesa una imagen con el candado tomado.
//     Los callbacks OnStage/OnResult del pipeline también toman mu, por lo que
//     su costo es O(1).
//   - Hay a lo más un trabajo a la vez (una ejecución o una medición). Su
//     goroutine la crea startJob; termina cuando el pipeline regresa y avisa
//     cerrando done, del que es la única responsable.
//   - La caché de miniaturas tiene su propio candado (thumbMu) para que
//     servir imágenes no compita con las actualizaciones del pipeline.

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

//go:embed ui
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
	Total      int        `json:"total"`
	Segundos   float64    `json:"segundos"`
	MaxEnVuelo int        `json:"max_en_vuelo"`
	Etapas     [4]float64 `json:"etapas"`
	Estado     string     `json:"estado"`
	Error      string     `json:"error,omitempty"`
}

type benchRow struct {
	Etiqueta string    `json:"etiqueta"`
	P        int       `json:"p"`
	Tiempos  []float64 `json:"tiempos"`
}

type benchState struct {
	Filtros  string     `json:"filtros"`
	Reps     int        `json:"reps"`
	Paso     int        `json:"paso"`
	Total    int        `json:"total"`
	Actual   string     `json:"actual"`
	Filas    []benchRow `json:"filas"`
	Estado   string     `json:"estado"` // "", "midiendo", "completa", "cancelada", "error"
	Error    string     `json:"error,omitempty"`
	Imagenes int        `json:"imagenes"`
}

type server struct {
	inDir, outDir, benchDir string

	mu      sync.Mutex // protege todo lo de abajo hasta thumbMu
	job     string     // "", "run" o "bench"
	cancel  context.CancelFunc
	done    chan struct{}
	runID   int
	req     runRequest
	names   []string // nombres de entrada por índice
	stage   []int8   // última etapa completada por índice (-1 = ninguna)
	outputs []string // nombres de salida en orden de finalización
	started time.Time
	history []historyEntry
	bench   benchState

	thumbMu sync.Mutex
	thumbs  map[string][]byte
}

func newServer(dataDir string) (*server, error) {
	s := &server{
		inDir:    filepath.Join(dataDir, "entrada"),
		outDir:   filepath.Join(dataDir, "salida"),
		benchDir: filepath.Join(dataDir, "medicion"),
		thumbs:   map[string][]byte{},
	}
	for _, d := range []string{s.inDir, s.outDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	page, _ := fs.Sub(uiFS, "ui")
	mux.Handle("GET /", http.FileServer(http.FS(page)))
	mux.HandleFunc("GET /api/imagenes", s.handleList)
	mux.HandleFunc("POST /api/imagenes", s.handleUpload)
	mux.HandleFunc("POST /api/ejemplos", s.handleSamples)
	mux.HandleFunc("DELETE /api/imagenes", s.handleClear)
	mux.HandleFunc("DELETE /api/imagenes/{name}", s.handleDelete)
	mux.HandleFunc("POST /api/ejecutar", s.handleRun)
	mux.HandleFunc("POST /api/medir", s.handleBench)
	mux.HandleFunc("POST /api/cancelar", s.handleCancel)
	mux.HandleFunc("GET /api/estado", s.handleStatus)
	mux.HandleFunc("GET /api/descargar", s.handleDownload)
	mux.HandleFunc("GET /miniatura/{dir}/{name}", s.handleThumb)
	mux.HandleFunc("GET /imagen/{dir}/{name}", s.handleImage)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// startJob reserva el único espacio de trabajo. Devuelve el contexto del
// trabajo y una función finish que la goroutine del trabajo debe diferir.
// Si ya hay un trabajo, devuelve ok = false. Debe llamarse con mu tomado.
func (s *server) startJob(kind string) (ctx context.Context, finish func(), ok bool) {
	if s.job != "" {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.job, s.cancel, s.done = kind, cancel, done
	return ctx, func() {
		cancel()
		s.mu.Lock()
		s.job, s.cancel = "", nil
		s.mu.Unlock()
		close(done) // único responsable del cierre
	}, true
}

// wait bloquea hasta que termine el trabajo en curso, si lo hay.
func (s *server) wait() {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

// stopJob cancela el trabajo en curso y espera a que termine.
func (s *server) stopJob() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.wait()
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		fail(w, http.StatusConflict, "No hay ningún procesamiento en curso.")
		return
	}
	cancel()
	writeJSON(w, http.StatusOK, map[string]bool{"cancelado": true})
}

func parseRun(r *http.Request) (runRequest, filters.Chain, error) {
	var req runRequest
	if err := decodeBody(r, &req); err != nil {
		return req, nil, errors.New("La solicitud no es JSON válido.")
	}
	chain, err := validateRun(req)
	return req, chain, err
}

func validateRun(req runRequest) (filters.Chain, error) {
	chain, err := filters.Parse(req.Filtros)
	if err != nil {
		return nil, errors.New("Revise los filtros: " + err.Error())
	}
	if req.Workers < 1 || req.Workers > 64 || req.Buffer < 0 || req.Buffer > 1024 || req.Franjas < 1 || req.Franjas > 64 {
		return nil, errors.New("Use de 1 a 64 goroutines por etapa, búfer de 0 a 1024 y de 1 a 64 franjas.")
	}
	return chain, nil
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	req, chain, err := parseRun(r)
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	list, err := imgio.List(s.inDir)
	if err != nil || len(list) == 0 {
		fail(w, http.StatusBadRequest, "Agregue al menos una imagen antes de ejecutar.")
		return
	}

	s.mu.Lock()
	ctx, finish, ok := s.startJob("run")
	if !ok {
		s.mu.Unlock()
		fail(w, http.StatusConflict, "Ya hay un procesamiento en curso. Espere a que termine o cancélelo.")
		return
	}
	os.RemoveAll(s.outDir)
	s.runID++
	id := s.runID
	s.req, s.started, s.outputs = req, time.Now(), nil
	s.names = make([]string, len(list))
	s.stage = make([]int8, len(list))
	for i, p := range list {
		s.names[i] = filepath.Base(p)
		s.stage[i] = -1
	}
	s.mu.Unlock()

	cfg := pipeline.Config{
		InputDir: s.inDir, OutputDir: s.outDir, Chain: chain,
		Decoders: req.Workers, Workers: req.Workers, Encoders: req.Workers,
		Buffer: req.Buffer, Strips: req.Franjas,
		OnStage: func(st pipeline.Stage, i int) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if i < len(s.stage) {
				s.stage[i] = int8(st)
			}
		},
		OnResult: func(res pipeline.Result) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.outputs = append(s.outputs, filepath.Base(res.Output))
		},
	}
	runFn, modo := pipeline.Run, "concurrente"
	if req.Secuencial {
		runFn, modo = pipeline.RunSequential, "secuencial"
	}

	// Goroutine de la ejecución. Dueño: el servidor (startJob). Termina
	// cuando el pipeline regresa: fin, primer error o cancelación.
	go func() {
		defer finish()
		st, err := runFn(ctx, cfg)
		e := historyEntry{
			ID: id, Modo: modo, Filtros: chain.String(), Workers: req.Workers, Buffer: req.Buffer,
			Franjas: req.Franjas, Imagenes: st.Processed, Total: len(list),
			Segundos: st.Elapsed.Seconds(), MaxEnVuelo: st.MaxInFlight, Estado: "completa",
		}
		if req.Secuencial {
			e.Workers, e.Buffer = 1, 0
		}
		for i, b := range st.Busy {
			e.Etapas[i] = b.Seconds()
		}
		switch {
		case errors.Is(err, context.Canceled):
			e.Estado = "cancelada"
		case err != nil:
			e.Estado, e.Error = "error", err.Error()
		}
		s.mu.Lock()
		s.history = append(s.history, e)
		s.mu.Unlock()
	}()
	writeJSON(w, http.StatusAccepted, map[string]int{"id": id})
}

type lane struct {
	Cantidad int      `json:"cantidad"`
	Nombres  []string `json:"nombres"`
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	// Carriles: imágenes que completaron la etapa k y aún no la k+1, es
	// decir, las que esperan en un canal o están siendo procesadas.
	var lanes [4]lane
	done := 0
	for i, st := range s.stage {
		switch {
		case st == int8(pipeline.StageEncode):
			done++
		case st >= 0:
			l := &lanes[st]
			l.Cantidad++
			if len(l.Nombres) < 10 {
				l.Nombres = append(l.Nombres, s.names[i])
			}
		}
	}
	elapsed := 0.0
	if s.job == "run" {
		elapsed = time.Since(s.started).Seconds()
	} else if n := len(s.history); n > 0 {
		elapsed = s.history[n-1].Segundos
	}
	// Slices no nulos: el JSON siempre lleva [] y no null.
	out := append([]string{}, s.outputs...)
	hist := append([]historyEntry{}, s.history...)
	for i := range lanes {
		if lanes[i].Nombres == nil {
			lanes[i].Nombres = []string{}
		}
	}
	bench := s.bench
	bench.Filas = make([]benchRow, len(s.bench.Filas)) // nunca nil
	for i, f := range s.bench.Filas {
		f.Tiempos = append([]float64{}, f.Tiempos...)
		bench.Filas[i] = f
	}
	resp := map[string]any{
		"trabajo": s.job, "id": s.runID, "total": len(s.names), "terminadas": done,
		"carriles": lanes, "salidas": out, "segundos": elapsed, "historial": hist,
		"solicitud": s.req, "medicion": bench,
		"gomaxprocs": runtime.GOMAXPROCS(0), "cpus": runtime.NumCPU(),
	}
	s.mu.Unlock()
	// La respuesta se escribe sin el candado: un cliente lento no bloquea
	// al pipeline (todo lo de resp ya son copias).
	writeJSON(w, http.StatusOK, resp)
}
