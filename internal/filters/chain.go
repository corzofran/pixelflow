package filters

import (
	"fmt"
	"image"
	"strconv"
	"strings"
)

// Step es un paso de la cadena de filtros.
type Step struct {
	Name  string
	Apply func(src *image.RGBA, strips int) *image.RGBA
}

// Chain es una secuencia de filtros que se aplica en orden.
type Chain []Step

// Apply ejecuta todos los pasos sobre img. La cadena vacía devuelve img.
func (c Chain) Apply(img *image.RGBA, strips int) *image.RGBA {
	for _, s := range c {
		img = s.Apply(img, strips)
	}
	return img
}

// String devuelve la representación textual de la cadena (p. ej. "gray,blur:2").
func (c Chain) String() string {
	names := make([]string, len(c))
	for i, s := range c {
		names[i] = s.Name
	}
	return strings.Join(names, ",")
}

// Parse interpreta una especificación como "gray,blur:2,resize:640x480".
//
//	gray            escala de grises
//	blur[:r]        desenfoque de caja de radio r (por omisión 2)
//	resize:WxH      redimensionado a W×H píxeles
//	resize:P%       redimensionado proporcional (p. ej. 50%)
//	sobel           bordes de Sobel (extensión)
func Parse(spec string) (Chain, error) {
	var chain Chain
	for _, raw := range strings.Split(spec, ",") {
		part := strings.TrimSpace(strings.ToLower(raw))
		if part == "" {
			continue
		}
		name, arg, _ := strings.Cut(part, ":")
		switch name {
		case "gray", "grises":
			chain = append(chain, Step{Name: "gray", Apply: Grayscale})
		case "sobel":
			chain = append(chain, Step{Name: "sobel", Apply: Sobel})
		case "blur", "desenfoque":
			r := 2
			if arg != "" {
				v, err := strconv.Atoi(arg)
				if err != nil || v < 0 || v > 50 {
					return nil, fmt.Errorf("radio de desenfoque inválido %q (0-50)", arg)
				}
				r = v
			}
			chain = append(chain, Step{Name: fmt.Sprintf("blur:%d", r), Apply: func(s *image.RGBA, k int) *image.RGBA {
				return BoxBlur(s, r, k)
			}})
		case "resize", "redimensionar":
			step, err := parseResize(arg)
			if err != nil {
				return nil, err
			}
			chain = append(chain, step)
		default:
			return nil, fmt.Errorf("filtro desconocido %q", name)
		}
	}
	if len(chain) == 0 {
		return nil, fmt.Errorf("la cadena de filtros está vacía")
	}
	return chain, nil
}

func parseResize(arg string) (Step, error) {
	if pct, ok := strings.CutSuffix(arg, "%"); ok {
		p, err := strconv.Atoi(pct)
		if err != nil || p <= 0 || p > 400 {
			return Step{}, fmt.Errorf("porcentaje de redimensionado inválido %q", arg)
		}
		return Step{Name: "resize:" + arg, Apply: func(s *image.RGBA, k int) *image.RGBA {
			w := max(1, s.Rect.Dx()*p/100)
			h := max(1, s.Rect.Dy()*p/100)
			return Resize(s, w, h, k)
		}}, nil
	}
	ws, hs, ok := strings.Cut(arg, "x")
	w, err1 := strconv.Atoi(ws)
	h, err2 := strconv.Atoi(hs)
	if !ok || err1 != nil || err2 != nil || w <= 0 || h <= 0 || w > 20000 || h > 20000 {
		return Step{}, fmt.Errorf("tamaño de redimensionado inválido %q (use WxH o P%%)", arg)
	}
	return Step{Name: "resize:" + arg, Apply: func(s *image.RGBA, k int) *image.RGBA {
		return Resize(s, w, h, k)
	}}, nil
}
