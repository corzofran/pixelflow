// Package pipeline implementa el pipeline concurrente de PixelFlow:
//
//	listar ─▶ paths ─▶ [D decodificadores] ─▶ decoded ─▶ [W filtros] ─▶ filtered ─▶ [E codificadores]
//	 (1 g)    (buf)        fan-out              (buf)     fan-out/in      (buf)        fan-in a disco
//
// Reglas de propiedad (ver también docs/ARQUITECTURA.md):
//
//   - Cada canal tiene un único responsable de cierre. paths lo cierra el
//     lister; decoded y filtered los cierra una goroutine "closer" de cada
//     etapa que espera (sync.WaitGroup) a que terminen todos los workers de la
//     etapa. Ningún worker cierra canales: así es imposible el pánico por
//     cierre múltiple.
//   - Todo envío y toda recepción que puede bloquearse está dentro de un select
//     con ctx.Done(), por lo que la cancelación nunca deja goroutines atascadas.
//   - Todas las goroutines se lanzan con errgroup: el primer error cancela el
//     contexto compartido y Run no regresa hasta que todas terminaron.
//   - Los canales tienen búfer acotado (Config.Buffer): si los codificadores
//     son lentos, las etapas anteriores se bloquean (contrapresión) en lugar de
//     acumular imágenes decodificadas en memoria.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"image"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
)

// Config parametriza una ejecución del pipeline.
type Config struct {
	InputDir  string
	OutputDir string
	Chain     filters.Chain

	Decoders int // goroutines de decodificación (E/S + CPU)
	Workers  int // goroutines de filtrado (CPU)
	Encoders int // goroutines de codificación (CPU + E/S)
	Buffer   int // capacidad de cada canal intermedio
	Strips   int // franjas paralelas dentro de cada imagen (extensión)
	Quality  int // calidad JPEG de salida (1-100)

	// OnResult, si no es nil, se invoca tras escribir cada imagen. Se llama
	// desde varias goroutines codificadoras a la vez: debe ser segura para
	// uso concurrente.
	OnResult func(Result)

	// OnStage, si no es nil, se invoca cada vez que una imagen (por su
	// índice) completa una etapa: StageList justo antes de que el lister la envíe,
	// StageDecode, StageFilter y StageEncode. Igual que OnResult, se llama
	// desde varias goroutines y debe ser segura para uso concurrente y
	// rápida (se ejecuta en la ruta caliente).
	OnStage func(stage Stage, index int)
}

func (c *Config) notify(s Stage, i int) {
	if c.OnStage != nil {
		c.OnStage(s, i)
	}
}

// Result describe una imagen ya escrita.
type Result struct {
	Index  int
	Input  string
	Output string
}

// Stage identifica una etapa para las métricas.
type Stage int

// Etapas del pipeline.
const (
	StageList Stage = iota
	StageDecode
	StageFilter
	StageEncode
	numStages
)

var stageNames = [numStages]string{"listar", "decodificar", "filtrar", "codificar"}

func (s Stage) String() string { return stageNames[s] }

// Stats resume una ejecución. Los contadores se actualizan con sync/atomic
// desde varias goroutines; Run devuelve una copia ya estable.
type Stats struct {
	Listed    int
	Processed int
	Elapsed   time.Duration
	// MaxInFlight es el máximo de imágenes decodificadas que estuvieron en
	// memoria a la vez (decodificadas y aún no escritas). Con búferes
	// acotados nunca supera D + W + E + 2·Buffer.
	MaxInFlight int
	// Busy es el tiempo acumulado de trabajo (suma de todas las goroutines)
	// en cada etapa. Sirve para estimar la fracción serial (Amdahl).
	Busy [numStages]time.Duration
}

type job struct {
	index int
	path  string
}

type decoded struct {
	job
	img    image.Image
	format string
}

type filtered struct {
	job
	img    *image.RGBA
	format string
}

type counters struct {
	listed, processed atomic.Int64
	inFlight, maxIn   atomic.Int64
	busy              [numStages]atomic.Int64
}

// enter registra una imagen más en memoria y actualiza el máximo con un
// ciclo CompareAndSwap (sin Mutex).
func (c *counters) enter() {
	n := c.inFlight.Add(1)
	for {
		m := c.maxIn.Load()
		if n <= m || c.maxIn.CompareAndSwap(m, n) {
			return
		}
	}
}

func (c *counters) leave() { c.inFlight.Add(-1) }

func (c *counters) track(s Stage, start time.Time) {
	c.busy[s].Add(int64(time.Since(start)))
}

func (c *Config) normalize() error {
	if c.InputDir == "" || c.OutputDir == "" {
		return errors.New("se requieren carpeta de entrada y de salida")
	}
	if len(c.Chain) == 0 {
		return errors.New("la cadena de filtros está vacía")
	}
	c.Decoders = max(1, c.Decoders)
	c.Workers = max(1, c.Workers)
	c.Encoders = max(1, c.Encoders)
	c.Buffer = max(0, c.Buffer)
	c.Strips = max(1, c.Strips)
	if c.Quality <= 0 || c.Quality > 100 {
		c.Quality = 90
	}
	return nil
}

// Run ejecuta el pipeline concurrente hasta procesar todas las imágenes, hasta
// el primer error o hasta que ctx se cancele. Siempre regresa después de que
// terminaron todas las goroutines que creó.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	if err := cfg.normalize(); err != nil {
		return Stats{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("crear carpeta de salida: %w", err)
	}

	start := time.Now()
	var cnt counters
	g, ctx := errgroup.WithContext(ctx)

	paths := make(chan job, cfg.Buffer)
	decodedCh := make(chan decoded, cfg.Buffer)
	filteredCh := make(chan filtered, cfg.Buffer)

	// Etapa 1 · listar. Una sola goroutine: es E/S de metadatos, barata, y
	// tener un único productor fija el índice de cada imagen.
	// Dueño: Run (vía errgroup). Termina al enviar todas las rutas o al
	// cancelarse ctx. Cierra paths (único responsable).
	g.Go(func() error {
		defer close(paths)
		return lister(ctx, &cfg, paths, &cnt)
	})
	// Etapa 2 · decodificar (fan-out de paths). Cierra decodedCh su closer.
	stage(g, cfg.Decoders, func() { close(decodedCh) }, func() error {
		return decodeWorker(ctx, &cfg, paths, decodedCh, &cnt)
	})
	// Etapa 3 · filtrar (fan-out de decodedCh, fan-in a filteredCh).
	stage(g, cfg.Workers, func() { close(filteredCh) }, func() error {
		return filterWorker(ctx, &cfg, decodedCh, filteredCh, &cnt)
	})
	// Etapa 4 · codificar y escribir (fan-in final; no produce canal).
	stage(g, cfg.Encoders, nil, func() error {
		return encodeWorker(ctx, &cfg, filteredCh, &cnt)
	})

	err := g.Wait()
	return cnt.snapshot(time.Since(start)), err
}

// lister envía cada ruta de la carpeta, en orden, a out. No cierra out: lo
// hace quien lo lanzó (único responsable).
func lister(ctx context.Context, cfg *Config, out chan<- job, cnt *counters) error {
	t := time.Now()
	list, err := imgio.List(cfg.InputDir)
	cnt.track(StageList, t)
	if err != nil {
		return err
	}
	cnt.listed.Store(int64(len(list)))
	for i, p := range list {
		// Se notifica antes de enviar: después del envío un decodificador
		// podría notificar StageDecode antes que el lister (orden invertido).
		cfg.notify(StageList, i)
		select {
		case out <- job{index: i, path: p}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// decodeWorker recibe rutas y envía imágenes decodificadas. Las firmas con
// canales direccionales impiden, en compilación, que un worker cierre o lea
// de su canal de salida.
func decodeWorker(ctx context.Context, cfg *Config, in <-chan job, out chan<- decoded, cnt *counters) error {
	for {
		var j job
		var ok bool
		select {
		case j, ok = <-in:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		t := time.Now()
		img, format, err := imgio.Decode(j.path)
		cnt.track(StageDecode, t)
		if err != nil {
			return err
		}
		cnt.enter()
		cfg.notify(StageDecode, j.index)
		select {
		case out <- decoded{job: j, img: img, format: format}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// filterWorker aplica la cadena de filtros (etapa limitada por CPU).
func filterWorker(ctx context.Context, cfg *Config, in <-chan decoded, out chan<- filtered, cnt *counters) error {
	for {
		var d decoded
		var ok bool
		select {
		case d, ok = <-in:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		t := time.Now()
		img := cfg.Chain.Apply(filters.ToRGBA(d.img), cfg.Strips)
		cnt.track(StageFilter, t)
		cfg.notify(StageFilter, d.index)
		select {
		case out <- filtered{job: d.job, img: img, format: d.format}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// encodeWorker codifica y escribe cada imagen (CPU de compresión + E/S).
func encodeWorker(ctx context.Context, cfg *Config, in <-chan filtered, cnt *counters) error {
	for {
		var f filtered
		var ok bool
		select {
		case f, ok = <-in:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		t := time.Now()
		outPath := imgio.OutputName(cfg.OutputDir, f.path, f.format)
		err := imgio.Encode(outPath, f.img, f.format, cfg.Quality)
		cnt.track(StageEncode, t)
		cnt.leave()
		if err != nil {
			return err
		}
		cnt.processed.Add(1)
		cfg.notify(StageEncode, f.index)
		if cfg.OnResult != nil {
			cfg.OnResult(Result{Index: f.index, Input: f.path, Output: outPath})
		}
	}
}

// stage lanza n workers de una etapa dentro de g y, si closeOut no es nil,
// una goroutine closer que lo invoca cuando todos los workers terminaron.
//
// Goroutines: n workers + 1 closer. Dueño: el errgroup de Run.
// Terminación de los workers: canal de entrada cerrado y vacío, error propio
// o ctx cancelado. Terminación del closer: justo después de que el último
// worker regresó. Como el closer es el único que cierra el canal de salida y
// solo lo hace cuando ya nadie puede enviar, no hay envío tras cierre ni
// cierre múltiple.
func stage(g *errgroup.Group, n int, closeOut func(), work func() error) {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		g.Go(func() error {
			defer wg.Done()
			return work()
		})
	}
	if closeOut != nil {
		g.Go(func() error {
			wg.Wait()
			closeOut()
			return nil
		})
	}
}

func (c *counters) snapshot(elapsed time.Duration) Stats {
	s := Stats{
		Listed:    int(c.listed.Load()),
		Processed: int(c.processed.Load()),
		Elapsed:   elapsed,

		MaxInFlight: int(c.maxIn.Load()),
	}
	for i := range s.Busy {
		s.Busy[i] = time.Duration(c.busy[i].Load())
	}
	return s
}

// RunSequential procesa las mismas imágenes en una sola goroutine, sin
// canales. Es la referencia para verificar resultados y la línea base T(1)
// para calcular el speedup.
func RunSequential(ctx context.Context, cfg Config) (Stats, error) {
	if err := cfg.normalize(); err != nil {
		return Stats{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("crear carpeta de salida: %w", err)
	}
	start := time.Now()
	var cnt counters
	t := time.Now()
	list, err := imgio.List(cfg.InputDir)
	cnt.track(StageList, t)
	if err != nil {
		return Stats{}, err
	}
	cnt.listed.Store(int64(len(list)))
	for i, p := range list {
		if err := ctx.Err(); err != nil {
			return cnt.snapshot(time.Since(start)), err
		}
		cfg.notify(StageList, i)
		t = time.Now()
		img, format, err := imgio.Decode(p)
		cnt.track(StageDecode, t)
		if err != nil {
			return cnt.snapshot(time.Since(start)), err
		}
		cfg.notify(StageDecode, i)
		t = time.Now()
		out := cfg.Chain.Apply(filters.ToRGBA(img), cfg.Strips)
		cnt.track(StageFilter, t)
		cfg.notify(StageFilter, i)
		t = time.Now()
		outPath := imgio.OutputName(cfg.OutputDir, p, format)
		err = imgio.Encode(outPath, out, format, cfg.Quality)
		cnt.track(StageEncode, t)
		if err != nil {
			return cnt.snapshot(time.Since(start)), err
		}
		cnt.processed.Add(1)
		cfg.notify(StageEncode, i)
		if cfg.OnResult != nil {
			cfg.OnResult(Result{Index: i, Input: p, Output: outPath})
		}
	}
	return cnt.snapshot(time.Since(start)), nil
}
