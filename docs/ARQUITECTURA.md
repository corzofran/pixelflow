# Arquitectura concurrente de PixelFlow

## Goroutines

| Goroutine | Cuántas | La crea | Termina cuando |
|---|---|---|---|
| lister | 1 | `pipeline.Run` vía `errgroup` | envió todas las rutas, falló `ReadDir` o `ctx` se canceló |
| decodificador | D (`-decoders`) | `stage()` vía `errgroup` | `paths` cerrado y vacío, error de decodificación o `ctx` cancelado |
| closer de decodificación | 1 | `stage()` vía `errgroup` | justo después de que los D decodificadores regresaron (`wg.Wait`) |
| filtro | W (`-workers`) | `stage()` vía `errgroup` | `decoded` cerrado y vacío o `ctx` cancelado |
| closer de filtrado | 1 | `stage()` vía `errgroup` | justo después de que los W filtros regresaron |
| codificador | E (`-encoders`) | `stage()` vía `errgroup` | `filtered` cerrado y vacío, error de escritura o `ctx` cancelado |
| franja | `-franjas` por imagen | `filters.forRows` | al procesar su franja; `forRows` espera con `WaitGroup` |
| señales | 1 (runtime) | `signal.NotifyContext` en `main` | `stop()` diferido en `main` |

`Run` no regresa hasta `g.Wait()`, que espera a **todas** las goroutines del
grupo. Por eso ninguna puede sobrevivir a la llamada.

## Canales

| Canal | Tipo | Capacidad | Escribe | Lee | Cierra |
|---|---|---|---|---|---|
| `paths` | `chan job` | `-buf` | lister | decodificadores | lister |
| `decoded` | `chan decoded` | `-buf` | decodificadores | filtros | closer de decodificación |
| `filtered` | `chan filtered` | `-buf` | filtros | codificadores | closer de filtrado |

Las firmas internas no exponen los canales: cada closure solo usa su canal de
entrada para recibir y el de salida para enviar. El único `close` de cada
canal está en un único lugar del código, por lo que el pánico por cierre
múltiple es imposible.

## Primitivas y alternativas descartadas

| Necesidad | Primitiva elegida | Alternativa descartada y por qué |
|---|---|---|
| Pasar imágenes entre etapas | canales con búfer acotado | cola con `Mutex`+slice: no da contrapresión ni se combina con `select` |
| Cerrar el canal de salida de una etapa | `sync.WaitGroup` + goroutine closer | cerrar desde cada worker: pánico por cierre múltiple |
| Propagar el primer error | `errgroup.WithContext` | canal de errores manual: más código y fácil de dejar bloqueado |
| Contadores y máximo en vuelo | `sync/atomic` (`Add`, `CompareAndSwap`) | `Mutex`: contención innecesaria en la ruta caliente |
| Callback de progreso en la CLI | `sync.Mutex` | sin protección: carrera detectada por `-race` (ver informe) |
| Paralelismo dentro de una imagen | franjas + `WaitGroup` | un pool global de franjas: acoplaría etapas y complicaría la cancelación |

## Contrapresión

Con búfer `B` la memoria de imágenes decodificadas está acotada por
`D + W + E + 2·B` (cada worker retiene a lo más una imagen y cada canal
intermedio a lo más `B`). La prueba `TestBackpressureBound` lo verifica con el
medidor `Stats.MaxInFlight`.
