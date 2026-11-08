package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

// RunRequest es la configuración de una ejecución.
type RunRequest struct {
	Filters    string `json:"filtros"`
	Workers    int    `json:"workers"`
	Buffer     int    `json:"buf"`
	Strips     int    `json:"franjas"`
	Sequential bool   `json:"secuencial"`
}

// HistoryEntry resume una ejecución terminada.
type HistoryEntry struct {
	ID          int        `json:"id"`
	Mode        string     `json:"modo"`
	Filters     string     `json:"filtros"`
	Workers     int        `json:"workers"`
	Buffer      int        `json:"buf"`
	Strips      int        `json:"franjas"`
	Processed   int        `json:"imagenes"`
	Total       int        `json:"total"`
	Seconds     float64    `json:"segundos"`
	MaxInFlight int        `json:"max_en_vuelo"`
	StageBusy   [4]float64 `json:"etapas"`
	State       string     `json:"estado"`
	Error       string     `json:"error,omitempty"`
}

// config construye y valida la configuración del pipeline. Reutiliza
// pipeline.Config.Validate: la regla vive en un solo lugar.
func (s *Service) config(r RunRequest, sink pipeline.Sink) (pipeline.Config, error) {
	chain, err := filters.Parse(r.Filters)
	if err != nil {
		return pipeline.Config{}, userErr(ErrInvalid, "Revise los filtros: "+err.Error()+".")
	}
	if r.Workers < 1 || r.Strips < 1 {
		return pipeline.Config{}, userErr(ErrInvalid, "Use al menos 1 goroutine por etapa y 1 franja.")
	}
	cfg := pipeline.Config{
		Source: imgio.DirSource{Dir: s.inDir}, Sink: sink, Chain: chain,
		Decoders: r.Workers, Workers: r.Workers, Encoders: r.Workers,
		Buffer: r.Buffer, Strips: r.Strips,
	}
	if err := cfg.Validate(); err != nil {
		return pipeline.Config{}, userErr(ErrInvalid, "Configuración no válida: "+err.Error()+".")
	}
	return cfg, nil
}

func (s *Service) inputList() ([]string, error) {
	list, err := imgio.List(s.inDir)
	if err != nil || len(list) == 0 {
		return nil, userErr(ErrInvalid, "Agregue al menos una imagen antes de continuar.")
	}
	return list, nil
}

// StartRun inicia una ejecución en segundo plano y devuelve su número.
func (s *Service) StartRun(r RunRequest) (int, error) {
	cfg, err := s.config(r, imgio.NewDirSink(s.outDir, 90))
	if err != nil {
		return 0, err
	}
	list, err := s.inputList()
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	ctx, finish, err := s.beginJob("run")
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	os.RemoveAll(s.outDir)
	s.runID++
	id := s.runID
	s.req, s.started, s.outputs = r, time.Now(), nil
	s.names = make([]string, len(list))
	s.stage = make([]int8, len(list))
	for i, p := range list {
		s.names[i], s.stage[i] = filepath.Base(p), -1
	}
	s.mu.Unlock()

	cfg.OnStage = func(st pipeline.Stage, i int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if i < len(s.stage) {
			s.stage[i] = int8(st)
		}
	}
	cfg.OnResult = func(res pipeline.Result) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.outputs = append(s.outputs, filepath.Base(res.Output))
	}
	runFn, mode := pipeline.Run, "concurrente"
	if r.Sequential {
		runFn, mode = pipeline.RunSequential, "secuencial"
	}

	// Goroutine de la ejecución. Dueño: el servicio (beginJob). Termina
	// cuando el pipeline regresa: fin, primer error o cancelación.
	go func() {
		defer finish()
		st, err := runFn(ctx, cfg)
		e := HistoryEntry{
			ID: id, Mode: mode, Filters: cfg.Chain.String(), Workers: r.Workers, Buffer: r.Buffer,
			Strips: r.Strips, Processed: st.Processed, Total: len(list),
			Seconds: st.Elapsed.Seconds(), MaxInFlight: st.MaxInFlight, State: "completa",
		}
		if r.Sequential {
			e.Workers, e.Buffer = 1, 0
		}
		for i, b := range st.Busy {
			e.StageBusy[i] = b.Seconds()
		}
		switch {
		case errors.Is(err, context.Canceled):
			e.State = "cancelada"
		case err != nil:
			e.State, e.Error = "error", err.Error()
		}
		s.mu.Lock()
		s.history = append(s.history, e)
		s.mu.Unlock()
	}()
	return id, nil
}
