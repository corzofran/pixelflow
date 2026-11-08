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

	"github.com/fcorzo/pixelflow/internal/filters"
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
		InputDir: *in, OutputDir: *out, Chain: chain,
		Decoders: *decoders, Workers: *workers, Encoders: *encoders,
		Buffer: *buffer, Strips: *strips, Quality: *quality,
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
