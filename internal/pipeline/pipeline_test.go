package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/imgio"
	"github.com/fcorzo/pixelflow/internal/synth"
)

// checkNoLeaks falla si, tras la prueba, quedan más goroutines que al
// inicio. No usa time.Sleep: cede el procesador con runtime.Gosched hasta un
// plazo, porque una goroutine que ya hizo wg.Done puede tardar unos
// instantes en salir del planificador.
func checkNoLeaks(t *testing.T) {
	t.Helper()
	base := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(2 * time.Second)
		for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		if n := runtime.NumGoroutine(); n > base {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Errorf("fuga de goroutines: %d al inicio, %d al final\n%s", base, n, buf)
		}
	})
}

func dataset(t testing.TB, n int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "in")
	if _, err := synth.WriteSet(dir, n, 64, 48, 42); err != nil {
		t.Fatal(err)
	}
	return dir
}

func mustChain(t testing.TB, spec string) filters.Chain {
	t.Helper()
	c, err := filters.Parse(spec)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = b
	}
	return files
}

// TestConcurrentMatchesSequential: los archivos de salida son idénticos, byte
// a byte, a los de la versión secuencial para cualquier combinación de
// workers, búfer y franjas.
func TestConcurrentMatchesSequential(t *testing.T) {
	checkNoLeaks(t)
	in := dataset(t, 24)
	chain := mustChain(t, "gray,blur:2,resize:50%")
	seqOut := filepath.Join(t.TempDir(), "seq")
	st, err := RunSequential(context.Background(), Config{Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(seqOut, 90), Chain: chain})
	if err != nil || st.Processed != 24 {
		t.Fatalf("secuencial: %v, %d procesadas", err, st.Processed)
	}
	want := readDir(t, seqOut)

	for _, w := range []int{1, 2, 4, 8} {
		for _, buf := range []int{0, 1, 16} {
			for _, strips := range []int{1, 3} {
				name := fmt.Sprintf("w=%d/buf=%d/franjas=%d", w, buf, strips)
				t.Run(name, func(t *testing.T) {
					out := filepath.Join(t.TempDir(), "out")
					st, err := Run(context.Background(), Config{
						Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(out, 90), Chain: chain,
						Decoders: w, Workers: w, Encoders: w, Buffer: buf, Strips: strips,
					})
					if err != nil {
						t.Fatal(err)
					}
					if st.Processed != 24 || st.Listed != 24 {
						t.Fatalf("procesadas %d/%d", st.Processed, st.Listed)
					}
					got := readDir(t, out)
					if len(got) != len(want) {
						t.Fatalf("%d archivos, se esperaban %d", len(got), len(want))
					}
					for name, b := range want {
						if !bytes.Equal(got[name], b) {
							t.Errorf("%s difiere de la versión secuencial", name)
						}
					}
				})
			}
		}
	}
}

// TestEachImageExactlyOnce ejercita la concurrencia con muchas goroutines
// sobre el mismo recurso (el callback y los canales) y verifica dos
// invariantes: cada índice se entrega exactamente una vez, y la memoria en
// vuelo nunca supera la cota D + W + E + 2·Buffer.
func TestEachImageExactlyOnce(t *testing.T) {
	checkNoLeaks(t)
	const n = 60
	in := dataset(t, n)
	var mu sync.Mutex
	seen := map[int]int{}
	cfg := Config{
		Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "out"), 90),
		Chain:    mustChain(t, "gray,blur:1"),
		Decoders: 16, Workers: 32, Encoders: 16, Buffer: 2,
		OnResult: func(r Result) {
			mu.Lock()
			defer mu.Unlock()
			seen[r.Index]++
		},
	}
	st, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != n {
		t.Fatalf("se entregaron %d índices distintos, se esperaban %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		if seen[i] != 1 {
			t.Errorf("índice %d entregado %d veces", i, seen[i])
		}
	}
	bound := cfg.Decoders + cfg.Workers + cfg.Encoders + 2*cfg.Buffer
	if st.MaxInFlight < 1 || st.MaxInFlight > bound {
		t.Fatalf("MaxInFlight=%d fuera de la cota [1,%d]", st.MaxInFlight, bound)
	}
}

// TestBackpressureBound: con un worker por etapa y búfer 1, nunca hay más de
// 1+1+1+2 = 5 imágenes en memoria aunque haya 40 en la entrada.
func TestBackpressureBound(t *testing.T) {
	checkNoLeaks(t)
	st, err := Run(context.Background(), Config{
		Source: imgio.DirSource{Dir: dataset(t, 40)}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "out"), 90),
		Chain: mustChain(t, "blur:3"), Decoders: 1, Workers: 1, Encoders: 1, Buffer: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.MaxInFlight > 5 {
		t.Fatalf("MaxInFlight=%d > 5: el búfer no está acotando", st.MaxInFlight)
	}
}

// TestFirstErrorCancelsAll: una imagen corrupta provoca que Run regrese ese
// error, que el resto del pipeline se cancele y que no queden goroutines.
func TestFirstErrorCancelsAll(t *testing.T) {
	checkNoLeaks(t)
	in := dataset(t, 40)
	bad := filepath.Join(in, "img_0005.jpg")
	if err := os.WriteFile(bad, []byte("no soy un jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, w := range []int{1, 4} {
		st, err := Run(context.Background(), Config{
			Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "out"), 90),
			Chain: mustChain(t, "gray"), Decoders: w, Workers: w, Encoders: w, Buffer: 1,
		})
		if err == nil || !strings.Contains(err.Error(), "img_0005.jpg") {
			t.Fatalf("w=%d: se esperaba el error de img_0005.jpg, se obtuvo %v", w, err)
		}
		if errors.Is(err, context.Canceled) {
			t.Fatalf("errgroup debe devolver el primer error, no la cancelación derivada")
		}
		if st.Processed >= 40 {
			t.Fatalf("w=%d: se procesaron %d imágenes; la cancelación no detuvo el resto", w, st.Processed)
		}
	}
	_, err := RunSequential(context.Background(), Config{
		Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "s"), 90), Chain: mustChain(t, "gray"),
	})
	if err == nil {
		t.Fatal("la versión secuencial también debe fallar")
	}
}

// TestCancellationStopsQuickly simula Ctrl+C: tras la tercera imagen se
// cancela el contexto. Run debe regresar context.Canceled en menos de 1 s y
// sin dejar goroutines vivas.
func TestCancellationStopsQuickly(t *testing.T) {
	checkNoLeaks(t)
	const n = 200
	in := dataset(t, n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var done atomic.Int32
	var cancelledAt atomic.Int64
	outDir := filepath.Join(t.TempDir(), "out")
	cfg := Config{
		Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(outDir, 90),
		Chain:    mustChain(t, "gray,blur:4"),
		Decoders: 2, Workers: 4, Encoders: 2, Buffer: 2,
		OnResult: func(Result) {
			if done.Add(1) == 3 {
				cancelledAt.Store(time.Now().UnixNano())
				cancel()
			}
		},
	}
	st, err := Run(ctx, cfg)
	stopped := time.Since(time.Unix(0, cancelledAt.Load()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("se esperaba context.Canceled, se obtuvo %v", err)
	}
	if st.Processed >= n {
		t.Fatal("la cancelación no detuvo el procesamiento")
	}
	if stopped > time.Second {
		t.Fatalf("tardó %v en detenerse tras la cancelación", stopped)
	}
	// No quedan temporales a medio escribir.
	matches, _ := filepath.Glob(filepath.Join(outDir, ".pixelflow-*"))
	if len(matches) != 0 {
		t.Fatalf("quedaron temporales: %v", matches)
	}
}

func TestDeadlineAndPreCancelled(t *testing.T) {
	checkNoLeaks(t)
	in := dataset(t, 30)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st, err := Run(ctx, Config{Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "a"), 90), Chain: mustChain(t, "gray")})
	if !errors.Is(err, context.Canceled) || st.Processed != 0 {
		t.Fatalf("contexto ya cancelado: err=%v procesadas=%d", err, st.Processed)
	}
	_, err = RunSequential(ctx, Config{Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "b"), 90), Chain: mustChain(t, "gray")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("secuencial con contexto cancelado: %v", err)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer dcancel()
	<-dctx.Done()
	_, err = Run(dctx, Config{Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "c"), 90), Chain: mustChain(t, "blur:5")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("se esperaba DeadlineExceeded, se obtuvo %v", err)
	}
}

func TestConfigErrors(t *testing.T) {
	checkNoLeaks(t)
	chain := mustChain(t, "gray")
	tmp := t.TempDir()
	cases := []Config{
		{Sink: imgio.NewDirSink(tmp, 90), Chain: chain},
		{Source: imgio.DirSource{Dir: tmp}, Chain: chain},
		{Source: imgio.DirSource{Dir: tmp}, Sink: imgio.NewDirSink(tmp, 90), Chain: chain, Workers: -1},
		{Source: imgio.DirSource{Dir: tmp}, Sink: imgio.NewDirSink(tmp, 90), Chain: chain, Buffer: MaxBuffer + 1},
		{Source: imgio.DirSource{Dir: tmp}, Sink: imgio.NewDirSink(tmp, 90), Chain: chain, Strips: MaxStrips + 1},
		{Source: imgio.DirSource{Dir: tmp}, Sink: imgio.NewDirSink(tmp, 90)},
	}
	for i, c := range cases {
		if _, err := Run(context.Background(), c); err == nil {
			t.Errorf("caso %d: Run debía fallar", i)
		}
		if _, err := RunSequential(context.Background(), c); err == nil {
			t.Errorf("caso %d: RunSequential debía fallar", i)
		}
	}
	missing := Config{Source: imgio.DirSource{Dir: filepath.Join(tmp, "no-existe")}, Sink: imgio.NewDirSink(filepath.Join(tmp, "o"), 90), Chain: chain}
	if _, err := Run(context.Background(), missing); err == nil {
		t.Error("carpeta inexistente: Run debía fallar")
	}
	if _, err := RunSequential(context.Background(), missing); err == nil {
		t.Error("carpeta inexistente: RunSequential debía fallar")
	}
	empty := Config{Source: imgio.DirSource{Dir: tmp}, Sink: imgio.NewDirSink(filepath.Join(tmp, "o2"), 90), Chain: chain}
	if st, err := Run(context.Background(), empty); err != nil || st.Processed != 0 {
		t.Errorf("carpeta vacía: err=%v procesadas=%d", err, st.Processed)
	}
}

func TestStageNamesAndOrder(t *testing.T) {
	names := []string{StageList.String(), StageDecode.String(), StageFilter.String(), StageEncode.String()}
	if strings.Join(names, ",") != "listar,decodificar,filtrar,codificar" {
		t.Fatal(names)
	}
	// El orden del listado es estable (sort) y determina los índices.
	in := dataset(t, 5)
	var mu sync.Mutex
	var got []string
	_, err := Run(context.Background(), Config{
		Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "o"), 90), Chain: mustChain(t, "gray"), Workers: 3,
		OnResult: func(r Result) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, fmt.Sprintf("%d:%s", r.Index, filepath.Base(r.Input)))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	if got[0] != "0:img_0000.png" || got[4] != "4:img_0004.png" {
		t.Fatalf("índices inesperados: %v", got)
	}
}

// TestOnStageOrder: cada imagen pasa por las cuatro etapas exactamente una
// vez y en orden (listar < decodificar < filtrar < codificar), tanto en la
// versión concurrente como en la secuencial.
func TestOnStageOrder(t *testing.T) {
	checkNoLeaks(t)
	const n = 30
	in := dataset(t, n)
	for _, seq := range []bool{false, true} {
		var mu sync.Mutex
		seen := map[int][]Stage{}
		cfg := Config{
			Source: imgio.DirSource{Dir: in}, Sink: imgio.NewDirSink(filepath.Join(t.TempDir(), "o"), 90), Chain: mustChain(t, "gray"),
			Decoders: 3, Workers: 5, Encoders: 3, Buffer: 1,
			OnStage: func(s Stage, i int) {
				mu.Lock()
				defer mu.Unlock()
				seen[i] = append(seen[i], s)
			},
		}
		run := Run
		if seq {
			run = RunSequential
		}
		if _, err := run(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		if len(seen) != n {
			t.Fatalf("seq=%v: %d índices", seq, len(seen))
		}
		for i, st := range seen {
			if fmt.Sprint(st) != fmt.Sprint([]Stage{StageList, StageDecode, StageFilter, StageEncode}) {
				t.Fatalf("seq=%v: imagen %d recorrió %v", seq, i, st)
			}
		}
	}
}
