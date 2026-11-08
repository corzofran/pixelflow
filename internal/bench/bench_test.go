package bench

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/pipeline"
	"github.com/fcorzo/pixelflow/internal/synth"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestStatistics(t *testing.T) {
	m, s := MeanSD([]float64{2, 4, 4, 4, 5, 5, 7, 9})
	if !near(m, 5) || !near(s, math.Sqrt(32.0/7)) {
		t.Fatalf("media %v sd %v", m, s)
	}
	if m, s := MeanSD(nil); m != 0 || s != 0 {
		t.Fatal("vacío")
	}
	// f = 0.05: S(2) = 1/(0.05+0.475) = 1.90476…
	s2 := Amdahl(0.05, 2)
	if f := SerialFraction(2, s2); !near(f, 0.05) {
		t.Fatalf("f = %v", f)
	}
	if SerialFraction(1, 1) != -1 || SerialFraction(4, 10) != 0 || SerialFraction(4, 0.5) != 1 {
		t.Fatal("casos límite de SerialFraction")
	}
}

func TestSummarize(t *testing.T) {
	if _, ok := Summarize([]Row{{Config: Config{Procs: 2}, Seconds: []float64{1}}}, 2); ok {
		t.Fatal("sin secuencial no hay resumen")
	}
	rows := []Row{
		{Config{"Secuencial", 0}, []float64{4, 4}},
		{Config{"P = 1", 1}, []float64{4.1, 4.1}},
		{Config{"P = 2", 2}, []float64{2.1, 2.1}},
		{Config{"P = 4", 4}, []float64{2.2, 2.2}},
	}
	s, ok := Summarize(rows, 2)
	if !ok || len(s.Points) != 3 || s.FitProcs != 2 || s.Best.Procs != 2 {
		t.Fatalf("%+v", s)
	}
	if want := SerialFraction(2, 4/2.1); !near(s.SerialFraction, want) || !near(s.Predicted[2], 4/2.1) {
		t.Fatalf("f=%v pred=%v", s.SerialFraction, s.Predicted)
	}
	if !near(s.Points[1].Efficiency, 4/2.1/2) {
		t.Fatal("eficiencia")
	}
	one, _ := Summarize(rows[:2], 1)
	if one.SerialFraction != -1 || len(one.Predicted) != 0 {
		t.Fatal("con un núcleo no se ajusta Amdahl")
	}
}

func TestRunInterleavedAndRestores(t *testing.T) {
	before := runtime.GOMAXPROCS(0)
	var order []int
	run := func(ctx context.Context, c Config) (time.Duration, error) {
		order = append(order, c.Procs)
		runtime.GOMAXPROCS(max(1, c.Procs))
		return time.Duration(c.Procs+1) * time.Millisecond, nil
	}
	steps := 0
	rows, err := Run(context.Background(), Default[:3], 2, run, func(step, total int, c Config, rep int) { steps = step })
	if err != nil || steps != 6 || len(rows[2].Seconds) != 2 {
		t.Fatalf("err=%v pasos=%d filas=%v", err, steps, rows)
	}
	if want := []int{0, 1, 2, 0, 1, 2}; len(order) != 6 || order[3] != want[3] {
		t.Fatalf("orden no intercalado: %v", order)
	}
	if runtime.GOMAXPROCS(0) != before {
		t.Fatal("no restauró GOMAXPROCS")
	}

	boom := errors.New("falla")
	rows, err = Run(context.Background(), Default, 1, func(ctx context.Context, c Config) (time.Duration, error) {
		if c.Procs == 2 {
			return 0, boom
		}
		return time.Millisecond, nil
	}, nil)
	if !errors.Is(err, boom) || len(rows[1].Seconds) != 1 {
		t.Fatal("debía devolver el error y lo medido")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, Default, 1, run, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelación")
	}
}

func TestPipelineRunner(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	if _, err := synth.WriteSet(in, 6, 32, 24, 1); err != nil {
		t.Fatal(err)
	}
	chain, _ := filters.Parse("gray,blur:1")
	base := pipeline.Config{Source: imgio.DirSource{Dir: in}, Chain: chain, Buffer: 2}
	newSink := func() (pipeline.Sink, func(), error) {
		dir, err := os.MkdirTemp(t.TempDir(), "out")
		return imgio.NewDirSink(dir, 90), func() { os.RemoveAll(dir) }, err
	}
	rows, err := Run(context.Background(), Default, 1, PipelineRunner(base, newSink), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := Summarize(rows, runtime.NumCPU())
	if !ok || len(s.Points) != 4 {
		t.Fatalf("%+v", s)
	}
}
