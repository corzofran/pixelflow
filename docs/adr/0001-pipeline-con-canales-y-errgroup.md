# ADR 0001 · Pipeline por etapas con canales acotados y errgroup

- Estado: aceptada
- Fecha: 2026-11-08

## Contexto
Hay que procesar cientos de imágenes con tres etapas de costo muy distinto
(decodificar, filtrar, codificar), cancelar ante el primer error o Ctrl+C y
medir el speedup con 1, 2, 4 y 8 núcleos.

## Decisión
Pipeline listar → decodificar → filtrar → codificar. Cada etapa es un worker
pool configurable (fan-out) conectado por canales con búfer acotado. Todas las
goroutines pertenecen a un `errgroup.WithContext`. El cierre de cada canal lo
hace una goroutine «closer» que espera con `sync.WaitGroup` a los workers de
su etapa. Todo envío o recepción va en `select` con `ctx.Done()`.

## Alternativas descartadas
- **Un solo worker pool que hace las tres operaciones por imagen**: más
  simple, pero no permite dimensionar etapas por separado ni observar la
  contrapresión entre ellas.
- **Cola con `Mutex` + `sync.Cond`**: no se combina con `select`, no es
  cancelable con contexto y no da una cota natural de memoria.
- **Canal de errores manual**: reimplementa errgroup y es fácil dejar un
  emisor bloqueado tras la cancelación.

## Consecuencias
Memoria acotada por D + W + E + 2·buf imágenes (probado). Imposible el
pánico por cierre múltiple. El costo es un pequeño sobrecosto con P = 1
(≈ 1–3 % frente a la versión secuencial).
