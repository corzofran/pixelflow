package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func call(t *testing.T, h http.Handler, method, url, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, url, bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// Flujo completo de la interfaz web: generar, ejecutar, consultar estado,
// servir imágenes; y una ejecución con imagen corrupta (errgroup).
func TestWebFlow(t *testing.T) {
	s := newServer(filepath.Join(t.TempDir(), "web"))
	h := s.routes()

	if code, _ := call(t, h, "GET", "/", ""); code != 200 {
		t.Fatalf("página: %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/run", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`); code != 400 {
		t.Fatalf("sin imágenes debía dar 400, dio %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/generate", `{"n":8,"w":40,"h":30}`); code != 200 {
		t.Fatalf("generar: %d", code)
	}
	if _, j := call(t, h, "GET", "/api/inputs", ""); len(j["imagenes"].([]any)) != 8 {
		t.Fatalf("inputs: %v", j)
	}
	for _, bad := range []string{`{"filtros":"nada","workers":2,"franjas":1}`, `{"filtros":"gray","workers":0,"franjas":1}`, `x`} {
		if code, _ := call(t, h, "POST", "/api/run", bad); code != 400 {
			t.Fatalf("%s: debía dar 400, dio %d", bad, code)
		}
	}
	if code, _ := call(t, h, "POST", "/api/cancel", ""); code != 409 {
		t.Fatalf("cancelar sin ejecución: %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/run", `{"filtros":"gray,blur:1,resize:50%","workers":3,"buf":2,"franjas":2}`); code != 202 {
		t.Fatalf("run: %d", code)
	}
	s.wait()
	_, st := call(t, h, "GET", "/api/status", "")
	if st["procesadas"].(float64) != 8 || st["running"].(bool) {
		t.Fatalf("estado: %v", st)
	}
	if code, _ := call(t, h, "GET", "/img/out/img_0000.png", ""); code != 200 {
		t.Fatalf("imagen de salida: %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/run", `{"filtros":"gray","workers":1,"buf":0,"franjas":1,"secuencial":true}`); code != 202 {
		t.Fatalf("secuencial: %d", code)
	}
	s.wait()

	call(t, h, "POST", "/api/generate", `{"n":6,"w":20,"h":20,"corrupta":2}`)
	call(t, h, "POST", "/api/run", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`)
	s.wait()
	_, st = call(t, h, "GET", "/api/status", "")
	hist := st["historial"].([]any)
	if len(hist) != 3 || hist[2].(map[string]any)["estado"] != "error" || st["error"] == "" {
		t.Fatalf("historial: %v", st)
	}
	if code, _ := call(t, h, "GET", "/api/run", ""); code != 405 {
		t.Fatal("GET /api/run debía dar 405")
	}
}
