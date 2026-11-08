# Arquitectura de PixelFlow

## Capas y dependencias

Las dependencias apuntan siempre hacia adentro (de la interfaz al dominio).
Ningún paquete interno importa a uno de una capa exterior; las decisiones
están en [`docs/adr/`](adr/).

```
cmd/pixelflow        cmd/pixelflow-web          composición: flags, señales, slog
     │                     │
     │               internal/web               adaptador HTTP + middleware
     │                     │
     │               internal/app               casos de uso (sin HTTP)
     ├─────────────┬───────┘
internal/bench     │                            medición de speedup y Amdahl
     └──────► internal/pipeline                 motor concurrente
                   │  puertos: Source, Sink
        ┌──────────┴──────────┐
internal/filters        internal/imgio          dominio puro · adaptadores de disco
```

| Paquete | Responsabilidad | No sabe de |
|---|---|---|
| `filters` | filtros puros sobre `*image.RGBA` | archivos, goroutines del pipeline |
| `pipeline` | etapas, canales, errgroup, métricas; puertos `Source`/`Sink`; `Config.Validate` | disco, HTTP |
| `imgio` | adaptadores `DirSource`/`DirSink`, escritura atómica | `pipeline` (satisface sus interfaces implícitamente) |
| `bench` | repeticiones intercaladas, media/σ, speedup, ajuste de Amdahl | HTTP, interfaz |
| `app` | imágenes de entrada, ejecuciones, mediciones, estado; errores tipados | `net/http` |
| `web` | rutas, JSON, códigos HTTP, middleware (recuperación, registro, cabeceras) | reglas de negocio |
| `cmd/*` | leer flags, armar dependencias, apagar en orden | lógica |

# Arquitectura concurrente

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
| servidor HTTP | 1 + 1 por petición | `main` de `pixelflow-web` / `net/http` | `Shutdown` con plazo de 5 s |
| trabajo de la web | 0 o 1 | `app.Service.StartRun` o `StartMeasure` | el pipeline regresa; cierra `done` |

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
| Estado de la interfaz web | `sync.Mutex` + instantáneas copiadas | `RWMutex`: las escrituras por imagen son tan frecuentes como las lecturas |
| Avance en vivo en la web | consulta periódica de `/api/estado` | WebSockets/SSE: goroutines por conexión que hay que cerrar al apagar |

## Contrapresión

Con búfer `B` la memoria de imágenes decodificadas está acotada por
`D + W + E + 2·B` (cada worker retiene a lo más una imagen y cada canal
intermedio a lo más `B`). La prueba `TestBackpressureBound` lo verifica con el
medidor `Stats.MaxInFlight`.
