package main

import (
	"bytes"
	"encoding/json"
	"image"
	"image/png"
	"mime/multipart"
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

func uploadReq(t *testing.T, files map[string][]byte) *http.Request {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for name, data := range files {
		w, _ := mw.CreateFormFile("imagenes", name)
		w.Write(data)
	}
	mw.Close()
	req := httptest.NewRequest("POST", "/api/imagenes", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func pngBytes(w, h int) []byte {
	var b bytes.Buffer
	png.Encode(&b, image.NewRGBA(image.Rect(0, 0, w, h)))
	return b.Bytes()
}

func newTestServer(t *testing.T) (*server, http.Handler) {
	s, err := newServer(filepath.Join(t.TempDir(), "web"))
	if err != nil {
		t.Fatal(err)
	}
	return s, s.routes()
}

// Flujo completo: subir, ejecutar, consultar carriles, miniaturas, ZIP.
func TestWebFlow(t *testing.T) {
	s, h := newTestServer(t)
	if code, _ := call(t, h, "GET", "/", ""); code != 200 {
		t.Fatalf("página: %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`); code != 400 {
		t.Fatalf("sin imágenes debía dar 400, dio %d", code)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, uploadReq(t, map[string][]byte{
		"Mi foto.png": pngBytes(40, 30), "../malo.png": pngBytes(8, 8), "nota.txt": []byte("x"), "falsa.jpg": []byte("x"),
	}))
	var up struct {
		Agregadas  []string
		Rechazadas []map[string]string
	}
	json.Unmarshal(rec.Body.Bytes(), &up)
	if rec.Code != 200 || len(up.Agregadas) != 2 || len(up.Rechazadas) != 2 {
		t.Fatalf("carga: %d %s", rec.Code, rec.Body)
	}
	if code, _ := call(t, h, "POST", "/api/ejemplos", `{"n":6,"ancho":40,"alto":30}`); code != 200 {
		t.Fatal("ejemplos")
	}
	_, list := call(t, h, "GET", "/api/imagenes", "")
	if n := len(list["imagenes"].([]any)); n != 8 {
		t.Fatalf("%d imágenes", n)
	}
	for _, bad := range []string{`{"filtros":"nada","workers":2,"franjas":1}`, `{"filtros":"gray","workers":0,"franjas":1}`, `x`} {
		if code, _ := call(t, h, "POST", "/api/ejecutar", bad); code != 400 {
			t.Fatalf("%s: %d", bad, code)
		}
	}
	if code, _ := call(t, h, "POST", "/api/cancelar", ""); code != 409 {
		t.Fatal("cancelar sin trabajo debía dar 409")
	}
	if code, _ := call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray,blur:1,resize:50%","workers":3,"buf":2,"franjas":2}`); code != 202 {
		t.Fatal("ejecutar")
	}
	s.wait()
	_, st := call(t, h, "GET", "/api/estado", "")
	if st["terminadas"].(float64) != 8 || st["trabajo"] != "" || len(st["salidas"].([]any)) != 8 {
		t.Fatalf("estado: %v", st)
	}
	name := st["salidas"].([]any)[0].(string)
	for _, u := range []string{"/miniatura/salida/" + name, "/miniatura/entrada/Mi_foto.png", "/imagen/salida/" + name, "/api/descargar"} {
		if code, _ := call(t, h, "GET", u, ""); code != 200 {
			t.Fatalf("%s: %d", u, code)
		}
	}
	if code, _ := call(t, h, "GET", "/miniatura/otro/x.png", ""); code != 404 {
		t.Fatal("directorio inválido debía dar 404")
	}
	if code, _ := call(t, h, "DELETE", "/api/imagenes/Mi_foto.png", ""); code != 200 {
		t.Fatal("eliminar")
	}

	// Archivo dañado: el primer error cancela todo.
	call(t, h, "POST", "/api/ejemplos", `{"n":0,"danada":true}`)
	call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`)
	s.wait()
	_, st = call(t, h, "GET", "/api/estado", "")
	hist := st["historial"].([]any)
	if last := hist[len(hist)-1].(map[string]any); last["estado"] != "error" || last["error"] == "" {
		t.Fatalf("se esperaba error: %v", last)
	}
	if code, _ := call(t, h, "DELETE", "/api/imagenes", ""); code != 200 {
		t.Fatal("vaciar")
	}
}

// Medición: 1 repetición de las 5 configuraciones y restauración de GOMAXPROCS.
func TestBench(t *testing.T) {
	s, h := newTestServer(t)
	call(t, h, "POST", "/api/ejemplos", `{"n":4,"ancho":32,"alto":24}`)
	if code, _ := call(t, h, "POST", "/api/medir", `{"filtros":"gray","reps":0}`); code != 400 {
		t.Fatal("reps=0 debía dar 400")
	}
	before := runtimeProcs()
	if code, _ := call(t, h, "POST", "/api/medir", `{"filtros":"gray","reps":1,"buf":2}`); code != 202 {
		t.Fatal("medir")
	}
	if code, _ := call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":1,"buf":0,"franjas":1}`); code != 409 {
		t.Fatal("no debía permitir dos trabajos")
	}
	s.wait()
	if runtimeProcs() != before {
		t.Fatal("GOMAXPROCS no se restauró")
	}
	_, st := call(t, h, "GET", "/api/estado", "")
	m := st["medicion"].(map[string]any)
	if m["estado"] != "completa" || len(m["filas"].([]any)) != 5 {
		t.Fatalf("medición: %v", m)
	}
	for _, f := range m["filas"].([]any) {
		if len(f.(map[string]any)["tiempos"].([]any)) != 1 {
			t.Fatalf("fila sin tiempo: %v", f)
		}
	}
	s.stopJob() // sin trabajo: no debe bloquear
}
