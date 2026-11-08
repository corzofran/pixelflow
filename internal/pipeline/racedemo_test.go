//go:build racedemo

package pipeline

import (
	"sync"
	"testing"
)

// TestRaceDemo introduce A PROPÓSITO una carrera de datos para la defensa
// oral (pregunta común 4). No forma parte de la suite normal: solo se
// compila con la etiqueta racedemo.
//
//	go test -race -tags racedemo -run RaceDemo ./internal/pipeline
//
// Versión correcta: usar atomic.Int64 como en counters.processed.
func TestRaceDemo(t *testing.T) {
	var processed int // ← contador compartido SIN sincronización
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				processed++ // lectura-modificación-escritura no atómica
			}
		}()
	}
	wg.Wait()
	t.Logf("processed = %d (debería ser 8000)", processed)
}
