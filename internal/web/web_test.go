package web

import (
	"bytes"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/fcorzo/pixelflow/internal/app"
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

func uploadReq(files map[string][]byte) *http.Request {
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

func newTest(t *testing.T) (*app.Service, http.Handler) {
	svc, err := app.New(filepath.Join(t.TempDir(), "web"))
	if err != nil {
		t.Fatal(err)
	}
	return svc, New(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Flujo completo por HTTP: subir, ejecutar, estado, miniaturas, ZIP, error.
func TestHTTPFlow(t *testing.T) {
	svc, h := newTest(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 200 || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("página: %d, cabeceras %v", rec.Code, rec.Header())
	}
	if code, _ := call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`); code != 400 {
		t.Fatalf("sin imágenes debía dar 400, dio %d", code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, uploadReq(map[string][]byte{
		"Mi foto.png": pngBytes(40, 30), "../malo.png": pngBytes(8, 8), "nota.txt": []byte("x"), "falsa.jpg": []byte("x"),
	}))
	var up struct {
		Agregadas  []string
		Rechazadas []app.Rejected
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
	for _, bad := range []string{`{"filtros":"nada","workers":2,"franjas":1}`, `{"filtros":"gray","workers":0,"franjas":1}`, `{"filtros":"gray","workers":999,"franjas":1}`, `x`} {
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
	svc.Wait()
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
	// ETag: la segunda petición con If-None-Match devuelve 304.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/miniatura/salida/"+name, nil))
	req := httptest.NewRequest("GET", "/miniatura/salida/"+name, nil)
	req.Header.Set("If-None-Match", rec.Header().Get("ETag"))
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("ETag: %d", rec2.Code)
	}
	for _, u := range []string{"/miniatura/otro/x.png", "/imagen/entrada/..%2Fsecreto.png", "/miniatura/entrada/no-existe.png"} {
		if code, _ := call(t, h, "GET", u, ""); code != 404 {
			t.Fatalf("%s debía dar 404, dio %d", u, code)
		}
	}
	if code, _ := call(t, h, "DELETE", "/api/imagenes/Mi_foto.png", ""); code != 200 {
		t.Fatal("eliminar")
	}
	if code, _ := call(t, h, "DELETE", "/api/imagenes/Mi_foto.png", ""); code != 404 {
		t.Fatal("eliminar dos veces debía dar 404")
	}

	// Archivo dañado: el primer error cancela todo y la miniatura da 422.
	call(t, h, "POST", "/api/ejemplos", `{"n":0,"danada":true}`)
	_, list = call(t, h, "GET", "/api/imagenes", "")
	var bad string
	for _, im := range list["imagenes"].([]any) {
		if m := im.(map[string]any); !m["valida"].(bool) {
			bad = m["nombre"].(string)
		}
	}
	if code, _ := call(t, h, "GET", "/miniatura/entrada/"+bad, ""); code != 422 {
		t.Fatalf("miniatura dañada: %d", code)
	}
	call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":2,"buf":1,"franjas":1}`)
	svc.Wait()
	_, st = call(t, h, "GET", "/api/estado", "")
	hist := st["historial"].([]any)
	if last := hist[len(hist)-1].(map[string]any); last["estado"] != "error" || last["error"] == "" {
		t.Fatalf("se esperaba error: %v", last)
	}
	if code, _ := call(t, h, "DELETE", "/api/imagenes", ""); code != 200 {
		t.Fatal("vaciar")
	}
	if code, _ := call(t, h, "GET", "/api/descargar", ""); code != 404 {
		t.Fatal("descargar sin salidas debía dar 404")
	}
}

func TestHTTPMeasure(t *testing.T) {
	svc, h := newTest(t)
	call(t, h, "POST", "/api/ejemplos", `{"n":4,"ancho":32,"alto":24}`)
	if code, _ := call(t, h, "POST", "/api/medir", `{"filtros":"gray","reps":0}`); code != 400 {
		t.Fatal("reps=0 debía dar 400")
	}
	before := runtime.GOMAXPROCS(0)
	if code, _ := call(t, h, "POST", "/api/medir", `{"filtros":"gray","reps":1,"buf":2}`); code != 202 {
		t.Fatal("medir")
	}
	if code, _ := call(t, h, "POST", "/api/ejecutar", `{"filtros":"gray","workers":1,"buf":0,"franjas":1}`); code != 409 {
		t.Fatal("no debía permitir dos trabajos")
	}
	if code, _ := call(t, h, "POST", "/api/ejemplos", `{"n":1,"ancho":32,"alto":24}`); code != 409 {
		t.Fatal("no debía cambiar las imágenes durante un trabajo")
	}
	svc.Wait()
	if runtime.GOMAXPROCS(0) != before {
		t.Fatal("GOMAXPROCS no se restauró")
	}
	_, st := call(t, h, "GET", "/api/estado", "")
	m := st["medicion"].(map[string]any)
	if m["estado"] != "completa" || len(m["filas"].([]any)) != 5 || m["resumen"] == nil {
		t.Fatalf("medición: %v", m)
	}
	if pts := m["resumen"].(map[string]any)["puntos"].([]any); len(pts) != 4 {
		t.Fatalf("puntos: %v", pts)
	}
}

func TestRecoverer(t *testing.T) {
	h := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
		recoverer(slog.New(slog.NewTextHandler(io.Discard, nil))))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 500 {
		t.Fatalf("pánico: %d", rec.Code)
	}
}
