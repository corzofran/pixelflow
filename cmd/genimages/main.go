// Comando genimages: genera un conjunto de imágenes sintéticas deterministas.
//
//	genimages -out data/in -n 120 -w 800 -h 600
//	genimages -out data/bad -n 10 -corrupta 3   # la imagen 3 queda corrupta
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/fcorzo/pixelflow/internal/synth"
)

func main() {
	out := flag.String("out", "data/in", "carpeta destino")
	n := flag.Int("n", 120, "número de imágenes")
	w := flag.Int("w", 800, "ancho")
	h := flag.Int("h", 600, "alto")
	seed := flag.Int64("seed", 1, "semilla")
	bad := flag.Int("corrupta", -1, "índice de una imagen a corromper (demostración de errores)")
	flag.Parse()

	paths, err := synth.WriteSet(*out, *n, *w, *h, *seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if *bad >= 0 && *bad < len(paths) {
		if err := os.WriteFile(paths[*bad], []byte("esto no es una imagen"), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("corrupta:", paths[*bad])
	}
	fmt.Printf("%d imágenes %dx%d en %s\n", len(paths), *w, *h, *out)
}
