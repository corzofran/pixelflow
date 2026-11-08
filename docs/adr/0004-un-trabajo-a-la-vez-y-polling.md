# ADR 0004 · Un trabajo a la vez, estado con Mutex y consulta periódica

- Estado: aceptada
- Fecha: 2026-11-08

## Contexto
La interfaz necesita mostrar el avance en vivo y medir rendimiento cambiando
`GOMAXPROCS`, que es global al proceso.

## Decisión
- El servicio admite **un trabajo a la vez** (ejecución o medición). Dos
  mediciones simultáneas se contaminarían entre sí y `GOMAXPROCS` no puede
  tener dos valores.
- El estado lo protege un `sync.Mutex` con secciones críticas O(1); las
  instantáneas se copian y se serializan **sin** el candado.
- La página consulta `/api/estado` cada 250 ms durante un trabajo y cada
  1.5 s en reposo.

## Alternativas descartadas
- **WebSockets o Server-Sent Events**: menor latencia, pero más estado por
  cliente y goroutines por conexión que hay que cerrar al apagar; para un
  tablero local la consulta periódica basta.
- **`RWMutex`**: las escrituras (callbacks por imagen) son tan frecuentes
  como las lecturas; no aporta y complica.

## Consecuencias
Simple de razonar y probar con `-race`. Un cliente lento nunca bloquea al
pipeline porque la respuesta se escribe fuera del candado.
