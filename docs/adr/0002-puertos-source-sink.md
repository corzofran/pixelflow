# ADR 0002 · Puertos Source y Sink para la entrada y salida de imágenes

- Estado: aceptada
- Fecha: 2026-11-08

## Contexto
La primera versión del pipeline llamaba directamente a `imgio` (disco). Eso
acoplaba la lógica concurrente al sistema de archivos: no se podía probar sin
escribir archivos ni reutilizar con otro origen (memoria, red).

## Decisión
`pipeline` define dos interfaces pequeñas (puertos):
`Source{List, Decode}` y `Sink{Encode}`. `imgio.DirSource` e `imgio.DirSink`
son los adaptadores de disco; las pruebas usan adaptadores en memoria. La
dependencia apunta hacia adentro: `imgio` no importa `pipeline` (las
interfaces de Go se satisfacen implícitamente).

## Alternativas descartadas
- **Mantener rutas en `Config`**: más corto, pero acopla y obliga a usar disco
  en todas las pruebas.
- **Una sola interfaz `Storage` grande**: viola segregación de interfaces;
  el pipeline solo necesita tres métodos.

## Consecuencias
`TestPortsInMemory` y `TestPortErrorsCancelPipeline` prueban el pipeline sin
disco, incluidos errores de lectura, decodificación y escritura. Los métodos
de los adaptadores deben ser seguros para uso concurrente (documentado).
