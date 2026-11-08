package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fcorzo/pixelflow/internal/synth"
)

// Conjunto de benchmark: 100 imágenes 320×240 generadas una sola vez.
var (
	benchOnce sync.Once
	benchDir  string
	benchErr  error
)

func benchInput(b *testing.B) string {
	b.Helper()
	benchOnce.Do(func() {
		benchDir, benchErr = os.MkdirTemp("", "pixelflow-bench-")
		if benchErr == nil {
			_, benchErr = synth.WriteSet(filepath.Join(benchDir, "in"), 100, 320, 240, 9)
		}
	})
	if benchErr != nil {
		b.Fatal(benchErr)
	}
	return filepath.Join(benchDir, "in")
}

func TestMain(m *testing.M) {
	code := m.Run()
	if benchDir != "" {
		os.RemoveAll(benchDir)
	}
	os.Exit(code)
}

// BenchmarkPipeline compara la versión secuencial con el pipeline usando
// 1, 2, 4 y 8 goroutines por etapa. Ejecútese con -cpu 1,2,4,8 para variar
// GOMAXPROCS:
//
//	go test ./internal/pipeline -run '^$' -bench Pipeline -cpu 1,2,4,8 -count 5 -benchmem
func BenchmarkPipeline(b *testing.B) {
	in := benchInput(b)
	chain := mustChain(b, "gray,blur:2,resize:50%")
	b.Run("secuencial", func(b *testing.B) {
		out := b.TempDir()
		for i := 0; i < b.N; i++ {
			if _, err := RunSequential(context.Background(), Config{InputDir: in, OutputDir: out, Chain: chain}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(100*b.N)/b.Elapsed().Seconds(), "img/s")
	})
	for _, w := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			out := b.TempDir()
			for i := 0; i < b.N; i++ {
				_, err := Run(context.Background(), Config{
					InputDir: in, OutputDir: out, Chain: chain,
					Decoders: w, Workers: w, Encoders: w, Buffer: 4,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(100*b.N)/b.Elapsed().Seconds(), "img/s")
		})
	}
}
