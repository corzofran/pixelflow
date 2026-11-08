package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"

	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

func jsonDecode(r io.Reader, v any) error { return json.NewDecoder(r).Decode(v) }

// benchConfigs son las configuraciones que pide la rúbrica: la versión
// secuencial como referencia y GOMAXPROCS = 1, 2, 4 y 8 con el mismo número
// de goroutines por etapa.
var benchConfigs = []struct {
	label string
	p     int
}{{"Secuencial", 0}, {"P = 1", 1}, {"P = 2", 2}, {"P = 4", 4}, {"P = 8", 8}}

// handleBench inicia una medición: reps repeticiones intercaladas de cada
// configuración sobre las imágenes cargadas.
func (s *server) handleBench(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filtros string `json:"filtros"`
		Reps    int    `json:"reps"`
		Buffer  int    `json:"buf"`
	}
	if err := decodeBody(r, &req); err != nil || req.Reps < 1 || req.Reps > 10 || req.Buffer < 0 || req.Buffer > 1024 {
		fail(w, http.StatusBadRequest, "Use de 1 a 10 repeticiones.")
		return
	}
	chain, err := validateRun(runRequest{Filtros: req.Filtros, Workers: 1, Buffer: req.Buffer, Franjas: 1})
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	list, _ := imgio.List(s.inDir)
	if len(list) == 0 {
		fail(w, http.StatusBadRequest, "Agregue al menos una imagen antes de medir.")
		return
	}

	s.mu.Lock()
	ctx, finish, ok := s.startJob("bench")
	if !ok {
		s.mu.Unlock()
		fail(w, http.StatusConflict, "Ya hay un procesamiento en curso. Espere a que termine o cancélelo.")
		return
	}
	s.bench = benchState{
		Filtros: chain.String(), Reps: req.Reps, Total: req.Reps * len(benchConfigs),
		Estado: "midiendo", Imagenes: len(list),
	}
	for _, c := range benchConfigs {
		s.bench.Filas = append(s.bench.Filas, benchRow{Etiqueta: c.label, P: c.p})
	}
	s.mu.Unlock()

	// Goroutine de la medición. Dueño: el servidor (startJob). Termina al
	// completar todas las repeticiones, ante el primer error o al cancelar.
	// Restaura GOMAXPROCS al salir.
	go func() {
		defer finish()
		orig := runtime.GOMAXPROCS(0)
		defer runtime.GOMAXPROCS(orig)
		state, msg := "completa", ""
	loop:
		for rep := 0; rep < req.Reps; rep++ {
			for i, c := range benchConfigs {
				s.mu.Lock()
				s.bench.Paso++
				s.bench.Actual = fmt.Sprintf("%s · repetición %d de %d", c.label, rep+1, req.Reps)
				s.mu.Unlock()

				os.RemoveAll(s.benchDir)
				cfg := pipeline.Config{InputDir: s.inDir, OutputDir: s.benchDir, Chain: chain, Buffer: req.Buffer}
				var st pipeline.Stats
				var err error
				if c.p == 0 {
					runtime.GOMAXPROCS(1)
					st, err = pipeline.RunSequential(ctx, cfg)
				} else {
					runtime.GOMAXPROCS(c.p)
					cfg.Decoders, cfg.Workers, cfg.Encoders = c.p, c.p, c.p
					st, err = pipeline.Run(ctx, cfg)
				}
				switch {
				case errors.Is(err, context.Canceled):
					state = "cancelada"
					break loop
				case err != nil:
					state, msg = "error", err.Error()
					break loop
				}
				s.mu.Lock()
				s.bench.Filas[i].Tiempos = append(s.bench.Filas[i].Tiempos, st.Elapsed.Seconds())
				s.mu.Unlock()
			}
		}
		os.RemoveAll(s.benchDir)
		s.mu.Lock()
		s.bench.Estado, s.bench.Error, s.bench.Actual = state, msg, ""
		s.mu.Unlock()
	}()
	writeJSON(w, http.StatusAccepted, map[string]int{"pasos": req.Reps * len(benchConfigs)})
}

func runtimeProcs() int { return runtime.GOMAXPROCS(0) }
