// Comando pixelflow-web: interfaz web local de PixelFlow.
//
//	go build -o bin/pixelflow-web ./cmd/pixelflow-web
//	./bin/pixelflow-web                 → abrir http://localhost:8080
//	./bin/pixelflow-web -addr localhost:9090 -data data/web
//
// Flujo de la interfaz: 1) cargar imágenes, 2) configurar filtros y
// concurrencia, 3) ejecutar y observar las etapas en vivo, 4) revisar y
// descargar resultados, 5) medir el speedup con GOMAXPROCS = 1, 2, 4 y 8.
//
// Solo usa la biblioteca estándar (net/http, embed, archive/zip).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "dirección de escucha")
	data := flag.String("data", filepath.Join("data", "web"), "carpeta de trabajo (entrada/, salida/, medicion/)")
	flag.Parse()

	s, err := newServer(*data)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: *addr, Handler: s.routes(), ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Goroutine del servidor HTTP. Dueño: main. Termina cuando Shutdown
	// cierra el listener (o si no puede escuchar); avisa por errc.
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	url := *addr
	if strings.HasPrefix(url, ":") {
		url = "localhost" + url
	}
	fmt.Printf("PixelFlow está listo en http://%s\nCtrl+C para detenerlo.\n", url)

	select {
	case err := <-errc:
		log.Fatal(err)
	case <-ctx.Done():
	}
	fmt.Println("\nDeteniendo: se cancela el trabajo en curso…")
	s.stopJob()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Println("apagado:", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Println(err)
	}
	fmt.Println("Detenido.")
}
