# ADR 0003 · Capas: servicio (app) separado del adaptador HTTP (web)

- Estado: aceptada
- Fecha: 2026-11-08

## Contexto
La interfaz web nació en `package main` con handlers que validaban,
manejaban archivos, lanzaban goroutines y calculaban estadísticas.

## Decisión
Arquitectura en capas con dependencias hacia adentro:

```
cmd/pixelflow-web   composición (flags, slog, señales, apagado)
  └─ internal/web   adaptador HTTP: rutas, JSON, middleware
       └─ internal/app   casos de uso: imágenes, ejecuciones, mediciones
            ├─ internal/bench     medición y Amdahl (también la usa la CLI)
            └─ internal/pipeline  motor concurrente (puertos Source/Sink)
                 ├─ internal/filters  dominio puro
                 └─ internal/imgio    adaptadores de disco
```

`app` no importa `net/http`; reporta errores tipados (`ErrInvalid`,
`ErrBusy`, `ErrNotFound`) que `web` traduce a 400/409/404. La validación de
la configuración vive solo en `pipeline.Config.Validate`. El análisis de
speedup vive solo en `bench`.

## Alternativas descartadas
- **Todo en `main`**: rápido de escribir, difícil de probar y de explicar.
- **Framework web (gin, echo)**: dependencia externa no permitida por la
  propuesta; `net/http` de Go 1.22 ya tiene rutas con método y comodines.

## Consecuencias
Cada capa tiene pruebas propias (`app` sin HTTP, `web` con `httptest`).
El costo es más archivos y un paso de traducción de errores.
