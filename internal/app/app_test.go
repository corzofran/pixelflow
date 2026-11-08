package app

import (
	"bytes"
	"errors"
	"image"
	"image/png"
	"path/filepath"
	"testing"
)

func newSvc(t *testing.T) *Service {
	s, err := New(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSafeName(t *testing.T) {
	cases := map[string]string{
		"Mi foto.png": "Mi_foto.png", `..\..\win.jpg`: "win.jpg", "../../etc/x.png": "x.png",
		"ñandú.jpg": "and.jpg", "...": "imagen", ".oculta.png": "oculta.png",
	}
	for in, want := range cases {
		if got := SafeName(in); got != want {
			t.Errorf("SafeName(%q) = %q, se esperaba %q", in, got, want)
		}
	}
}

func TestImagesLifecycle(t *testing.T) {
	s := newSvc(t)
	var b bytes.Buffer
	png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 10, 8)))
	n1, rej, err := s.AddImage("a.png", b.Bytes())
	n2, _, _ := s.AddImage("a.png", b.Bytes())
	if err != nil || rej != nil || n1 != "a.png" || n2 != "a_2.png" {
		t.Fatalf("%v %v %s %s", err, rej, n1, n2)
	}
	if _, rej, _ := s.AddImage("b.gif", b.Bytes()); rej == nil {
		t.Fatal("debía rechazar GIF")
	}
	imgs, total := s.Images()
	if len(imgs) != 2 || total == 0 || imgs[0].Width != 10 {
		t.Fatalf("%v %d", imgs, total)
	}
	if _, err := s.AddSamples(-1, 0, 0, false); !errors.Is(err, ErrInvalid) {
		t.Fatal("muestras inválidas")
	}
	if _, err := s.AddSamples(MaxImages, 16, 16, false); !errors.Is(err, ErrInvalid) {
		t.Fatal("debía rechazar superar el máximo")
	}
	if _, err := s.Path("otra", "a.png"); !errors.Is(err, ErrNotFound) {
		t.Fatal("carpeta inválida")
	}
	if err := s.RemoveImage("../a.png"); !errors.Is(err, ErrInvalid) {
		t.Fatal("ruta inválida")
	}
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	if imgs, _ := s.Images(); len(imgs) != 0 {
		t.Fatal("no se vació")
	}
	if _, err := s.StartRun(RunRequest{Filters: "gray", Workers: 1, Strips: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("sin imágenes debía ser inválido")
	}
	var buf bytes.Buffer
	if err := s.WriteZip(&buf); !errors.Is(err, ErrNotFound) {
		t.Fatal("zip sin salidas")
	}
	if err := s.Cancel(); !errors.Is(err, ErrBusy) {
		t.Fatal("cancelar sin trabajo")
	}
}

func TestCancelRun(t *testing.T) {
	s := newSvc(t)
	if _, err := s.AddSamples(60, 200, 150, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartRun(RunRequest{Filters: "blur:6", Workers: 1, Buffer: 1, Strips: 1}); err != nil {
		t.Fatal(err)
	}
	s.Stop() // cancela y espera
	snap := s.Snapshot()
	if snap.Job != "" || len(snap.History) != 1 || snap.History[0].State != "cancelada" {
		t.Fatalf("%+v", snap.History)
	}
	if snap.Done >= 60 {
		t.Fatal("la cancelación no detuvo la ejecución")
	}
}

func TestRunMeasureThumbAndZip(t *testing.T) {
	s := newSvc(t)
	if n, err := s.AddSamples(6, 40, 30, false); err != nil || n != 6 {
		t.Fatal(n, err)
	}
	if _, err := s.StartRun(RunRequest{Filters: "nada", Workers: 1, Strips: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("filtro inválido")
	}
	if _, err := s.StartRun(RunRequest{Filters: "gray", Workers: 0, Strips: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("workers 0")
	}
	if _, err := s.StartRun(RunRequest{Filters: "gray", Workers: 1, Buffer: 99999, Strips: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("búfer fuera de rango (pipeline.Config.Validate)")
	}
	for _, seq := range []bool{true, false} {
		if _, err := s.StartRun(RunRequest{Filters: "gray,resize:50%", Workers: 2, Buffer: 1, Strips: 1, Sequential: seq}); err != nil {
			t.Fatal(err)
		}
		s.Wait()
	}
	snap := s.Snapshot()
	if snap.Done != 6 || len(snap.Outputs) != 6 || len(snap.History) != 2 || snap.History[0].Mode != "secuencial" || snap.History[1].State != "completa" {
		t.Fatalf("%+v", snap)
	}
	data, tag, err := s.Thumbnail(DirOutput, snap.Outputs[0])
	if err != nil || len(data) == 0 || tag == "" {
		t.Fatal(err)
	}
	if again, _, _ := s.Thumbnail(DirOutput, snap.Outputs[0]); !bytes.Equal(again, data) {
		t.Fatal("caché de miniaturas")
	}
	if _, _, err := s.Thumbnail(DirInput, "no-existe.png"); !errors.Is(err, ErrNotFound) {
		t.Fatal("miniatura inexistente")
	}
	var zipBuf bytes.Buffer
	if !s.HasOutputs() || s.WriteZip(&zipBuf) != nil || zipBuf.Len() == 0 {
		t.Fatal("zip")
	}
	if err := s.StartMeasure(MeasureRequest{Filters: "gray", Reps: 0}); !errors.Is(err, ErrInvalid) {
		t.Fatal("reps 0")
	}
	if err := s.StartMeasure(MeasureRequest{Filters: "gray", Reps: 1, Buffer: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartMeasure(MeasureRequest{Filters: "gray", Reps: 1}); !errors.Is(err, ErrBusy) {
		t.Fatal("dos mediciones a la vez")
	}
	if err := s.RemoveImage(snap.Outputs[0]); !errors.Is(err, ErrBusy) {
		t.Fatal("no debía permitir cambiar imágenes durante un trabajo")
	}
	s.Wait()
	m := s.Snapshot().Measure
	if m.State != "completa" || m.Summary == nil || len(m.Summary.Points) != 4 || m.Step != m.Total {
		t.Fatalf("%+v", m)
	}
	// Archivo dañado: el primer error queda en el historial.
	if _, err := s.AddSamples(0, 0, 0, true); err != nil {
		t.Fatal(err)
	}
	s.StartRun(RunRequest{Filters: "gray", Workers: 2, Strips: 1})
	s.Wait()
	h := s.Snapshot().History
	if last := h[len(h)-1]; last.State != "error" || last.Error == "" {
		t.Fatalf("%+v", last)
	}
	imgs, _ := s.Images()
	bad := 0
	for _, im := range imgs {
		if !im.Valid {
			bad++
		}
	}
	if bad != 1 {
		t.Fatal("la imagen dañada debe marcarse como no válida")
	}
}
