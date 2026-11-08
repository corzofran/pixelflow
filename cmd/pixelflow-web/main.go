// Comando pixelflow-web: interfaz web local de PixelFlow.
//
//	go build -o bin/pixelflow-web ./cmd/pixelflow-web
//	./bin/pixelflow-web                 → abrir http://localhost:8080
//	./bin/pixelflow-web -addr localhost:9090 -data data/web
//
// Este archivo solo cablea las capas (composition root):
//
//	cmd/pixelflow-web → internal/web (HTTP) → internal/app (casos de uso)
//	  → internal/pipeline + internal/bench → internal/filters, internal/imgio
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fcorzo/pixelflow/internal/app"
	"github.com/fcorzo/pixelflow/internal/web"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "dirección de escucha")
	data := flag.String("data", filepath.Join("data", "web"), "carpeta de trabajo (entrada/, salida/, medicion/)")
	verbose := flag.Bool("v", false, "registro detallado (nivel debug)")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	svc, err := app.New(*data)
	if err != nil {
		log.Error("no se pudo preparar la carpeta de trabajo", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           web.New(svc, log),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Goroutine del servidor HTTP. Dueño: main. Termina cuando Shutdown
	// cierra el listener (o si no puede escuchar) y avisa por errc.
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	url := *addr
	if strings.HasPrefix(url, ":") {
		url = "localhost" + url
	}
	fmt.Printf("PixelFlow está listo en http://%s\nCtrl+C para detenerlo.\n", url)

	select {
	case err := <-errc:
		log.Error("el servidor no pudo iniciar", "error", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	fmt.Println("\nDeteniendo: se cancela el trabajo en curso…")
	svc.Stop()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Warn("apagado con plazo vencido", "error", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("servidor", "error", err)
	}
	fmt.Println("Detenido.")
}
