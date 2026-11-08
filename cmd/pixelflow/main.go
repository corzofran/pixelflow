// Comando pixelflow: procesa concurrentemente las imágenes de una carpeta.
//
//	pixelflow -in data/in -out data/out -filtros gray,blur:2,resize:50% -workers 4
//
// Ctrl+C (SIGINT) o SIGTERM cancelan el contexto raíz: todas las etapas se
// detienen, se imprime un resumen parcial y el programa termina con código 130.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/fcorzo/pixelflow/internal/bench"
	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pixelflow", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		in       = fs.String("in", "data/in", "carpeta de entrada")
		out      = fs.String("out", "data/out", "carpeta de salida")
		spec     = fs.String("filtros", "gray,blur:2,resize:50%", "cadena de filtros: gray, blur[:r], resize:WxH|P%, sobel")
		workers  = fs.Int("workers", runtime.GOMAXPROCS(0), "goroutines de la etapa de filtrado")
		decoders = fs.Int("decoders", 0, "goroutines de decodificación (0 = igual que -workers)")
		encoders = fs.Int("encoders", 0, "goroutines de codificación (0 = igual que -workers)")
		buffer   = fs.Int("buf", 4, "capacidad de cada canal intermedio")
		strips   = fs.Int("franjas", 1, "franjas paralelas dentro de cada imagen")
		quality  = fs.Int("calidad", 90, "calidad JPEG de salida (1-100)")
		timeout  = fs.Duration("timeout", 0, "plazo máximo de ejecución (0 = sin plazo)")
		seq      = fs.Bool("secuencial", false, "usar la versión secuencial de referencia")
		cpuprof  = fs.String("cpuprofile", "", "escribir perfil de CPU en este archivo")
		quiet    = fs.Bool("q", false, "no imprimir cada imagen procesada")
		medir    = fs.Int("medir", 0, "medir speedup: N repeticiones de secuencial y GOMAXPROCS 1, 2, 4, 8")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	chain, err := filters.Parse(*spec)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	if *decoders <= 0 {
		*decoders = *workers
	}
	if *encoders <= 0 {
		*encoders = *workers
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	cfg := pipeline.Config{
		Source: imgio.DirSource{Dir: *in}, Sink: imgio.NewDirSink(*out, *quality), Chain: chain,
		Decoders: *decoders, Workers: *workers, Encoders: *encoders,
		Buffer: *buffer, Strips: *strips,
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	if *medir > 0 {
		return measure(ctx, cfg, *medir, stdout, stderr)
	}
	if !*quiet {
		// OnResult se invoca desde varias goroutines codificadoras. Un
		// io.Writer arbitrario (p. ej. bytes.Buffer) no es seguro para uso
		// concurrente: go test -race detectó esta carrera en la primera
		// versión. El Mutex serializa las escrituras de progreso.
		var mu sync.Mutex
		cfg.OnResult = func(r pipeline.Result) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Fprintf(stdout, "  ✓ %s\n", r.Output)
		}
	}

	mode := "concurrente"
	runFn := pipeline.Run
	if *seq {
		mode, runFn = "secuencial", pipeline.RunSequential
	}
	fmt.Fprintf(stdout, "PixelFlow · modo %s · filtros=%s · GOMAXPROCS=%d · D/W/E=%d/%d/%d · buf=%d · franjas=%d\n",
		mode, chain, runtime.GOMAXPROCS(0), *decoders, *workers, *encoders, *buffer, *strips)

	stats, err := runFn(ctx, cfg)
	printStats(stdout, stats)

	switch {
	case err == nil:
		return 0
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(stderr, "interrumpido: el pipeline se detuvo de forma ordenada (resumen parcial arriba)")
		return 130
	case errors.Is(err, context.DeadlineExceeded):
		fmt.Fprintln(stderr, "plazo agotado: el pipeline se detuvo de forma ordenada (resumen parcial arriba)")
		return 124
	default:
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
}

func printStats(w io.Writer, s pipeline.Stats) {
	thr := 0.0
	if s.Elapsed > 0 {
		thr = float64(s.Processed) / s.Elapsed.Seconds()
	}
	fmt.Fprintf(w, "Resumen: %d/%d imágenes en %v (%.1f img/s)\n",
		s.Processed, s.Listed, s.Elapsed.Round(time.Millisecond), thr)
	var total time.Duration
	for _, b := range s.Busy {
		total += b
	}
	for st, b := range s.Busy {
		pct := 0.0
		if total > 0 {
			pct = 100 * float64(b) / float64(total)
		}
		fmt.Fprintf(w, "  %-12s %10v  %5.1f %%\n", pipeline.Stage(st), b.Round(time.Millisecond), pct)
	}
}

// measure ejecuta la medición de speedup (paquete bench) e imprime la tabla y
// el ajuste de Amdahl. Las salidas se escriben en una carpeta temporal.
func measure(ctx context.Context, cfg pipeline.Config, reps int, stdout, stderr io.Writer) int {
	newSink := func() (pipeline.Sink, func(), error) {
		dir, err := os.MkdirTemp("", "pixelflow-medir-")
		return imgio.NewDirSink(dir, 90), func() { os.RemoveAll(dir) }, err
	}
	fmt.Fprintf(stdout, "Midiendo %d repeticiones de %d configuraciones (filtros=%s, buf=%d)\n", reps, len(bench.Default), cfg.Chain, cfg.Buffer)
	rows, err := bench.Run(ctx, bench.Default, reps, bench.PipelineRunner(cfg, newSink),
		func(step, total int, c bench.Config, rep int) {
			fmt.Fprintf(stdout, "  [%2d/%d] %-10s repetición %d\n", step, total, c.Label, rep)
		})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	s, _ := bench.Summarize(rows, runtime.NumCPU())
	fmt.Fprintf(stdout, "\n%-12s %10s %9s %9s %10s\n", "config", "media (s)", "σ (s)", "speedup", "eficiencia")
	fmt.Fprintf(stdout, "%-12s %10.3f %9.3f %8.2f× %10s\n", "secuencial", s.Sequential.Mean, s.Sequential.SD, 1.0, "—")
	for _, p := range s.Points {
		fmt.Fprintf(stdout, "%-12s %10.3f %9.3f %8.2f× %9.0f%%\n", fmt.Sprintf("P = %d", p.Procs), p.Mean, p.SD, p.Speedup, 100*p.Efficiency)
	}
	if s.SerialFraction >= 0 {
		fmt.Fprintf(stdout, "\nAmdahl (ajuste con P = %d de %d núcleos): f = %.1f %%, predice %.2f× con 4, %.2f× con 8, máximo %.1f×\n",
			s.FitProcs, s.CPUs, 100*s.SerialFraction, s.Predicted[4], s.Predicted[8], s.MaxSpeedup)
	}
	return 0
}
