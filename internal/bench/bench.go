// Package bench mide el speedup del pipeline y lo compara con la ley de
// Amdahl. Lo usan la CLI (flag -medir) y la interfaz web, de modo que el
// cálculo vive en un solo lugar y tiene pruebas.
package bench

import (
	"context"
	"math"
	"runtime"
	"time"

	"github.com/fcorzo/pixelflow/internal/pipeline"
)

// Config es una configuración a medir. Procs = 0 indica la versión
// secuencial; en otro caso GOMAXPROCS = Procs y Procs goroutines por etapa.
type Config struct {
	Label string `json:"etiqueta"`
	Procs int    `json:"p"`
}

// Default son las configuraciones que pide la rúbrica.
var Default = []Config{{"Secuencial", 0}, {"P = 1", 1}, {"P = 2", 2}, {"P = 4", 4}, {"P = 8", 8}}

// Row agrupa los tiempos medidos (en segundos) de una configuración.
type Row struct {
	Config
	Seconds []float64 `json:"tiempos"`
}

// Runner ejecuta una vez la configuración c y devuelve su duración.
type Runner func(ctx context.Context, c Config) (time.Duration, error)

// Progress recibe el avance antes de cada ejecución (step empieza en 1).
type Progress func(step, total int, c Config, rep int)

// Run ejecuta reps repeticiones intercaladas (C1 C2 … C1 C2 …) para que
// una perturbación pasajera del equipo afecte a todas las configuraciones
// por igual. Restaura GOMAXPROCS al terminar. Ante un error o cancelación
// devuelve lo medido hasta ese momento junto con el error.
func Run(ctx context.Context, cfgs []Config, reps int, run Runner, progress Progress) ([]Row, error) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	rows := make([]Row, len(cfgs))
	for i, c := range cfgs {
		rows[i] = Row{Config: c, Seconds: []float64{}}
	}
	step, total := 0, reps*len(cfgs)
	for rep := 1; rep <= reps; rep++ {
		for i, c := range cfgs {
			if err := ctx.Err(); err != nil {
				return rows, err
			}
			step++
			if progress != nil {
				progress(step, total, c, rep)
			}
			d, err := run(ctx, c)
			if err != nil {
				return rows, err
			}
			rows[i].Seconds = append(rows[i].Seconds, d.Seconds())
		}
	}
	return rows, nil
}

// PipelineRunner devuelve un Runner que ejecuta el pipeline con base,
// ajustando GOMAXPROCS y las goroutines por etapa según la configuración.
// newSink crea un destino nuevo para cada ejecución.
func PipelineRunner(base pipeline.Config, newSink func() (pipeline.Sink, func(), error)) Runner {
	return func(ctx context.Context, c Config) (time.Duration, error) {
		sink, cleanup, err := newSink()
		if err != nil {
			return 0, err
		}
		defer cleanup()
		cfg := base
		cfg.Sink = sink
		var st pipeline.Stats
		if c.Procs <= 0 {
			runtime.GOMAXPROCS(1)
			st, err = pipeline.RunSequential(ctx, cfg)
		} else {
			runtime.GOMAXPROCS(c.Procs)
			cfg.Decoders, cfg.Workers, cfg.Encoders = c.Procs, c.Procs, c.Procs
			st, err = pipeline.Run(ctx, cfg)
		}
		return st.Elapsed, err
	}
}

// Point resume una configuración.
type Point struct {
	Procs      int     `json:"p"`
	Mean       float64 `json:"media"`
	SD         float64 `json:"sd"`
	N          int     `json:"n"`
	Speedup    float64 `json:"speedup"`
	SpeedupErr float64 `json:"speedup_err"`
	Efficiency float64 `json:"eficiencia"`
}

// Summary es el resultado del análisis.
type Summary struct {
	Sequential Point   `json:"secuencial"`
	Points     []Point `json:"puntos"`
	// SerialFraction es la f de Amdahl estimada con el speedup medido en
	// FitProcs, el mayor P ≤ núcleos disponibles; -1 si no se pudo estimar
	// (equipo de un núcleo o sin datos).
	SerialFraction float64         `json:"f"`
	FitProcs       int             `json:"p_ajuste"`
	Predicted      map[int]float64 `json:"prediccion"`
	MaxSpeedup     float64         `json:"maximo"`
	CPUs           int             `json:"cpus"`
	Best           *Point          `json:"mejor,omitempty"`
}

// MeanSD devuelve la media y la desviación estándar muestral.
func MeanSD(xs []float64) (mean, sd float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	if len(xs) < 2 {
		return mean, 0
	}
	for _, x := range xs {
		sd += (x - mean) * (x - mean)
	}
	return mean, math.Sqrt(sd / float64(len(xs)-1))
}

// Amdahl devuelve el speedup predicho con fracción serial f y p procesadores.
func Amdahl(f float64, p int) float64 { return 1 / (f + (1-f)/float64(p)) }

// SerialFraction despeja f de S = 1 / (f + (1-f)/p), acotada a [0, 1].
func SerialFraction(p int, s float64) float64 {
	if p <= 1 || s <= 0 {
		return -1
	}
	f := (float64(p)/s - 1) / float64(p-1)
	return math.Min(1, math.Max(0, f))
}

// Summarize calcula speedup, eficiencia y el ajuste de Amdahl. Devuelve
// ok = false si todavía no hay tiempos de la versión secuencial.
func Summarize(rows []Row, cpus int) (Summary, bool) {
	sum := Summary{SerialFraction: -1, Predicted: map[int]float64{}, CPUs: cpus, Points: []Point{}}
	var seq *Row
	for i := range rows {
		if rows[i].Procs == 0 && len(rows[i].Seconds) > 0 {
			seq = &rows[i]
		}
	}
	if seq == nil {
		return sum, false
	}
	m, s := MeanSD(seq.Seconds)
	sum.Sequential = Point{Procs: 0, Mean: m, SD: s, N: len(seq.Seconds), Speedup: 1}
	for _, r := range rows {
		if r.Procs == 0 || len(r.Seconds) == 0 {
			continue
		}
		pm, ps := MeanSD(r.Seconds)
		sp := m / pm
		rel := math.Hypot(s/m, ps/pm)
		pt := Point{Procs: r.Procs, Mean: pm, SD: ps, N: len(r.Seconds), Speedup: sp, SpeedupErr: sp * rel, Efficiency: sp / float64(r.Procs)}
		sum.Points = append(sum.Points, pt)
		if sum.Best == nil || pt.Speedup > sum.Best.Speedup {
			b := pt
			sum.Best = &b
		}
		if r.Procs > 1 && r.Procs <= cpus && r.Procs > sum.FitProcs {
			sum.FitProcs = r.Procs
			sum.SerialFraction = SerialFraction(r.Procs, sp)
		}
	}
	if sum.SerialFraction >= 0 {
		for _, p := range []int{2, 4, 8, 16} {
			sum.Predicted[p] = Amdahl(sum.SerialFraction, p)
		}
		// MaxSpeedup = 0 significa «sin límite» (f = 0); así el valor
		// siempre se puede serializar a JSON (Inf no se puede).
		if sum.SerialFraction > 0 {
			sum.MaxSpeedup = 1 / sum.SerialFraction
		}
	}
	return sum, true
}
