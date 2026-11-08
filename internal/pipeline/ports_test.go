package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"strings"
	"sync"
	"testing"

	"github.com/fcorzo/pixelflow/internal/filters"
	"github.com/fcorzo/pixelflow/internal/synth"
)

// Adaptadores en memoria: demuestran que el pipeline solo depende de los
// puertos Source y Sink, no del sistema de archivos.

type memSource struct {
	ids    []string
	imgs   map[string]image.Image
	bad    string // id que falla al decodificar
	listOK bool
}

func newMemSource(n int) *memSource {
	m := &memSource{imgs: map[string]image.Image{}, listOK: true}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("mem-%03d", i)
		m.ids = append(m.ids, id)
		m.imgs[id] = synth.Image(24, 16, int64(i))
	}
	return m
}

func (m *memSource) List(ctx context.Context) ([]string, error) {
	if !m.listOK {
		return nil, errors.New("origen no disponible")
	}
	return m.ids, nil
}

func (m *memSource) Decode(id string) (image.Image, string, error) {
	if id == m.bad {
		return nil, "", fmt.Errorf("decodificar %s: datos corruptos", id)
	}
	return m.imgs[id], "png", nil
}

type memSink struct {
	mu      sync.Mutex
	out     map[string]*image.RGBA
	failAt  string
	written int
}

func (s *memSink) Encode(id string, img image.Image, format string) (string, error) {
	if id == s.failAt {
		return "", errors.New("disco lleno")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.out == nil {
		s.out = map[string]*image.RGBA{}
	}
	s.out[id] = img.(*image.RGBA)
	s.written++
	return "mem://" + id, nil
}

func TestPortsInMemory(t *testing.T) {
	checkNoLeaks(t)
	src := newMemSource(40)
	chain := mustChain(t, "gray,blur:1,resize:50%")
	seq, conc := &memSink{}, &memSink{}
	if _, err := RunSequential(context.Background(), Config{Source: src, Sink: seq, Chain: chain}); err != nil {
		t.Fatal(err)
	}
	st, err := Run(context.Background(), Config{Source: src, Sink: conc, Chain: chain, Decoders: 4, Workers: 8, Encoders: 4, Buffer: 2})
	if err != nil || st.Processed != 40 || conc.written != 40 {
		t.Fatalf("err=%v procesadas=%d escritas=%d", err, st.Processed, conc.written)
	}
	for id, want := range seq.out {
		if !bytes.Equal(conc.out[id].Pix, want.Pix) {
			t.Fatalf("%s difiere de la versión secuencial", id)
		}
	}
	// La salida coincide con aplicar la cadena directamente.
	ref := chain.Apply(filters.ToRGBA(src.imgs["mem-007"]), 1)
	if !bytes.Equal(conc.out["mem-007"].Pix, ref.Pix) {
		t.Fatal("la salida no coincide con la cadena aplicada directamente")
	}
}

func TestPortErrorsCancelPipeline(t *testing.T) {
	checkNoLeaks(t)
	chain := mustChain(t, "gray")
	cases := map[string]func() Config{
		"decode": func() Config {
			s := newMemSource(30)
			s.bad = "mem-004"
			return Config{Source: s, Sink: &memSink{}, Chain: chain, Workers: 3}
		},
		"encode": func() Config {
			return Config{Source: newMemSource(30), Sink: &memSink{failAt: "mem-002"}, Chain: chain, Encoders: 3}
		},
		"list": func() Config {
			s := newMemSource(3)
			s.listOK = false
			return Config{Source: s, Sink: &memSink{}, Chain: chain}
		},
	}
	want := map[string]string{"decode": "mem-004", "encode": "disco lleno", "list": "origen no disponible"}
	for name, mk := range cases {
		for _, run := range []func(context.Context, Config) (Stats, error){Run, RunSequential} {
			_, err := run(context.Background(), mk())
			if err == nil || !strings.Contains(err.Error(), want[name]) {
				t.Fatalf("%s: se esperaba %q, se obtuvo %v", name, want[name], err)
			}
		}
	}
}

func TestValidate(t *testing.T) {
	ok := Config{Source: newMemSource(1), Sink: &memSink{}, Chain: mustChain(t, "gray")}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := ok
	bad.Encoders = MaxGoroutines + 1
	if bad.Validate() == nil {
		t.Fatal("debía rechazar demasiadas goroutines")
	}
}
