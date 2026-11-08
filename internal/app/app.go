// Package app es la capa de servicio (casos de uso) de la interfaz web:
// administra las imágenes de entrada, lanza ejecuciones y mediciones y
// expone instantáneas del estado. No sabe nada de HTTP; el paquete web es
// solo un adaptador que traduce peticiones a llamadas de Service.
//
// Concurrencia
//
//   - Todo el estado compartido lo protege Service.mu (sync.Mutex). Las
//     secciones críticas solo copian o actualizan contadores y slices; nunca
//     se procesa una imagen con el candado tomado. Los callbacks OnStage y
//     OnResult del pipeline también toman mu, así que su costo es O(1).
//   - Hay a lo más un trabajo (ejecución o medición) a la vez. Su goroutine
//     la crea beginJob; termina cuando el pipeline regresa y avisa cerrando
//     done, del que es la única responsable.
//   - La caché de miniaturas tiene su propio candado (thumbMu) para que
//     servir imágenes no compita con las actualizaciones del pipeline.
//   - Snapshot devuelve copias: quien la usa puede serializarla sin candado.
package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/fcorzo/pixelflow/internal/bench"
	"github.com/fcorzo/pixelflow/internal/pipeline"
)

// Tipos de error del servicio. El adaptador HTTP los traduce a códigos.
var (
	ErrInvalid  = errors.New("solicitud no válida")
	ErrBusy     = errors.New("hay un procesamiento en curso")
	ErrNotFound = errors.New("no encontrado")
)

// UserError es un error con un mensaje para mostrar a la persona usuaria.
// Unwrap devuelve su tipo (ErrInvalid, ErrBusy o ErrNotFound).
type UserError struct {
	Kind error
	Msg  string
}

func (e *UserError) Error() string { return e.Msg }
func (e *UserError) Unwrap() error { return e.Kind }

func userErr(kind error, msg string) error { return &UserError{Kind: kind, Msg: msg} }

var errBusy = userErr(ErrBusy, "Ya hay un procesamiento en curso. Espere a que termine o cancélelo.")

// Service es la capa de servicio. Úsese con New.
type Service struct {
	inDir, outDir, benchDir string

	mu      sync.Mutex // protege los campos siguientes hasta thumbMu
	job     string     // "", "run" o "bench"
	cancel  context.CancelFunc
	done    chan struct{}
	runID   int
	req     RunRequest
	names   []string // nombres de entrada por índice
	stage   []int8   // última etapa completada por índice (-1 = ninguna)
	outputs []string // nombres de salida en orden de finalización
	started time.Time
	history []HistoryEntry
	measure Measurement

	thumbMu sync.Mutex
	thumbs  map[string][]byte
}

// New crea el servicio con su carpeta de trabajo (entrada/, salida/, medicion/).
func New(dataDir string) (*Service, error) {
	s := &Service{
		inDir:    filepath.Join(dataDir, "entrada"),
		outDir:   filepath.Join(dataDir, "salida"),
		benchDir: filepath.Join(dataDir, "medicion"),
		thumbs:   map[string][]byte{},
		measure:  Measurement{Rows: []bench.Row{}},
	}
	for _, d := range []string{s.inDir, s.outDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// beginJob reserva el único espacio de trabajo. Debe llamarse con mu tomado.
// Devuelve el contexto del trabajo y finish, que la goroutine del trabajo
// debe diferir: libera el espacio y cierra done (su único responsable).
func (s *Service) beginJob(kind string) (context.Context, func(), error) {
	if s.job != "" {
		return nil, nil, errBusy
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.job, s.cancel, s.done = kind, cancel, done
	return ctx, func() {
		cancel()
		s.mu.Lock()
		s.job, s.cancel = "", nil
		s.mu.Unlock()
		close(done)
	}, nil
}

// busy devuelve errBusy si hay un trabajo en curso.
func (s *Service) busy() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job != "" {
		return userErr(ErrBusy, "Espere a que termine el procesamiento en curso para cambiar las imágenes.")
	}
	return nil
}

// Cancel cancela el trabajo en curso.
func (s *Service) Cancel() error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		return userErr(ErrBusy, "No hay ningún procesamiento en curso.")
	}
	cancel()
	return nil
}

// Wait bloquea hasta que termine el trabajo en curso, si lo hay.
func (s *Service) Wait() {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

// Stop cancela el trabajo en curso y espera a que termine (apagado).
func (s *Service) Stop() {
	s.Cancel()
	s.Wait()
}

// Lane es un carril de la vista en vivo: imágenes que completaron una etapa
// y aún no la siguiente (esperan en un canal o están en proceso).
type Lane struct {
	Count int      `json:"cantidad"`
	Names []string `json:"nombres"`
}

// Snapshot es una instantánea consistente del estado, ya copiada.
type Snapshot struct {
	Job        string         `json:"trabajo"`
	ID         int            `json:"id"`
	Total      int            `json:"total"`
	Done       int            `json:"terminadas"`
	Lanes      [3]Lane        `json:"carriles"`
	Outputs    []string       `json:"salidas"`
	Seconds    float64        `json:"segundos"`
	History    []HistoryEntry `json:"historial"`
	Request    RunRequest     `json:"solicitud"`
	Measure    Measurement    `json:"medicion"`
	GOMAXPROCS int            `json:"gomaxprocs"`
	CPUs       int            `json:"cpus"`
}

// Snapshot devuelve el estado actual. Todos los slices son copias no nulas.
func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{
		Job: s.job, ID: s.runID, Total: len(s.names), Request: s.req,
		Outputs: append([]string{}, s.outputs...), History: append([]HistoryEntry{}, s.history...),
		GOMAXPROCS: runtime.GOMAXPROCS(0), CPUs: runtime.NumCPU(),
	}
	for i := range snap.Lanes {
		snap.Lanes[i].Names = []string{}
	}
	for i, st := range s.stage {
		switch {
		case st == int8(pipeline.StageEncode):
			snap.Done++
		case st >= 0:
			l := &snap.Lanes[st]
			l.Count++
			if len(l.Names) < 10 {
				l.Names = append(l.Names, s.names[i])
			}
		}
	}
	if s.job == "run" {
		snap.Seconds = time.Since(s.started).Seconds()
	} else if n := len(s.history); n > 0 {
		snap.Seconds = s.history[n-1].Seconds
	}
	snap.Measure = s.measure.copy()
	return snap
}
