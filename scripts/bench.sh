#!/usr/bin/env bash
# Reproduce los experimentos del informe. Uso: ./scripts/bench.sh [repeticiones]
# Resultados en results/: bench.txt, cli_runs.csv, cpu.prof, pprof_top.txt
set -euo pipefail
REPS=${1:-5}
N=${N:-120}; W=${W:-800}; H=${H:-600}
FILTROS=${FILTROS:-gray,blur:2,resize:50%}
cd "$(dirname "$0")/.."
mkdir -p results bin
go build -o bin/pixelflow ./cmd/pixelflow
go build -o bin/genimages ./cmd/genimages
DATA=$(mktemp -d); trap 'rm -rf "$DATA"' EXIT
./bin/genimages -out "$DATA/in" -n "$N" -w "$W" -h "$H" >/dev/null

{ echo "# equipo"; uname -srm; nproc; grep -m1 "model name" /proc/cpuinfo || true; go version; } > results/equipo.txt

echo "modo,gomaxprocs,workers,rep,segundos,decod_s,filtro_s,codif_s" > results/cli_runs.csv
measure() { # modo procs workers rep extra...
  local modo=$1 p=$2 w=$3 r=$4; shift 4
  out=$(GOMAXPROCS=$p ./bin/pixelflow -in "$DATA/in" -out "$DATA/out_$modo$p" -filtros "$FILTROS" -workers "$w" -q "$@")
  secs=$(echo "$out" | sed -n 's/.* en \([0-9.]*\)\(m\?s\) .*/\1 \2/p' | awk '{ if ($2=="ms") print $1/1000; else print $1 }')
  stage() { echo "$out" | awk -v s="$1" '$1==s { v=$2; if (v ~ /ms$/) { sub("ms","",v); v/=1000 } else if (v ~ /µs$/) { v=0 } else { sub("s","",v) }; print v }'; }
  echo "$modo,$p,$w,$r,$secs,$(stage decodificar),$(stage filtrar),$(stage codificar)" >> results/cli_runs.csv
}
for r in $(seq 1 "$REPS"); do
  measure secuencial 1 1 "$r" -secuencial
  for p in 1 2 4 8; do measure concurrente "$p" "$p" "$r"; done
  for w in 1 4 8; do [ "$w" = 2 ] || measure workers_p2 2 "$w" "$r"; done
done

# Perfil de CPU (GOMAXPROCS = núcleos disponibles)
./bin/pixelflow -in "$DATA/in" -out "$DATA/prof" -filtros "$FILTROS" -q -cpuprofile results/cpu.prof >/dev/null
go tool pprof -top -nodecount=30 bin/pixelflow results/cpu.prof > results/pprof_top.txt 2>/dev/null
go tool pprof -top -cum -nodecount=40 bin/pixelflow results/cpu.prof > results/pprof_cum.txt 2>/dev/null

# Benchmarks de go test con GOMAXPROCS = 1, 2, 4, 8
go test ./internal/pipeline -run '^$' -bench Pipeline -cpu 1,2,4,8 -count "$REPS" -benchmem | tee results/bench.txt
go test ./internal/filters -run '^$' -bench Filters -count 3 -benchmem | tee results/bench_filters.txt
echo "listo: ver results/"
