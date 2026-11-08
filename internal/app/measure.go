package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/fcorzo/pixelflow/internal/bench"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

// MeasureRequest configura una medición de speedup.
type MeasureRequest struct {
	Filters string `json:"filtros"`
	Reps    int    `json:"reps"`
	Buffer  int    `json:"buf"`
}

// Measurement es el estado de la medición más reciente.
type Measurement struct {
	Filters string         `json:"filtros"`
	Reps    int            `json:"reps"`
	Step    int            `json:"paso"`
	Total   int            `json:"total"`
	Current string         `json:"actual"`
	Rows    []bench.Row    `json:"filas"`
	State   string         `json:"estado"` // "", "midiendo", "completa", "cancelada", "error"
	Error   string         `json:"error,omitempty"`
	Images  int            `json:"imagenes"`
	Summary *bench.Summary `json:"resumen"`
}

func (m Measurement) copy() Measurement {
	c := m
	c.Rows = make([]bench.Row, len(m.Rows))
	for i, r := range m.Rows {
		r.Seconds = append([]float64{}, r.Seconds...)
		c.Rows[i] = r
	}
	if m.Summary != nil {
		sum := *m.Summary
		c.Summary = &sum
	}
	return c
}

// StartMeasure ejecuta en segundo plano reps repeticiones de la versión
// secuencial y del pipeline con GOMAXPROCS = 1, 2, 4 y 8 (paquete bench).
func (s *Service) StartMeasure(r MeasureRequest) error {
	if r.Reps < 1 || r.Reps > 10 {
		return userErr(ErrInvalid, "Use de 1 a 10 repeticiones.")
	}
	// El destino real lo crea PipelineRunner en cada ejecución; aquí solo
	// se usa uno provisional para validar la configuración.
	base, err := s.config(RunRequest{Filters: r.Filters, Workers: 1, Buffer: r.Buffer, Strips: 1}, imgio.NewDirSink(s.benchDir, 90))
	if err != nil {
		return err
	}
	list, err := s.inputList()
	if err != nil {
		return err
	}

	s.mu.Lock()
	ctx, finish, err := s.beginJob("bench")
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.measure = Measurement{
		Filters: base.Chain.String(), Reps: r.Reps, Total: r.Reps * len(bench.Default),
		State: "midiendo", Images: len(list), Rows: []bench.Row{},
	}
	s.mu.Unlock()

	newSink := func() (pipeline.Sink, func(), error) {
		os.RemoveAll(s.benchDir)
		return imgio.NewDirSink(s.benchDir, 90), func() { os.RemoveAll(s.benchDir) }, nil
	}
	progress := func(step, total int, c bench.Config, rep int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.measure.Step = step
		s.measure.Current = fmt.Sprintf("%s, repetición %d de %d", c.Label, rep, r.Reps)
	}

	// Goroutine de la medición. Dueño: el servicio (beginJob). Termina al
	// completar las repeticiones, ante el primer error o al cancelar.
	// bench.Run restaura GOMAXPROCS al salir.
	go func() {
		defer finish()
		rows, err := bench.Run(ctx, bench.Default, r.Reps, bench.PipelineRunner(base, newSink), progress)
		sum, ok := bench.Summarize(rows, runtime.NumCPU())
		s.mu.Lock()
		defer s.mu.Unlock()
		s.measure.Rows, s.measure.Current, s.measure.State = rows, "", "completa"
		if ok {
			s.measure.Summary = &sum
		}
		switch {
		case errors.Is(err, context.Canceled):
			s.measure.State = "cancelada"
		case err != nil:
			s.measure.State, s.measure.Error = "error", err.Error()
		}
	}()
	return nil
}
