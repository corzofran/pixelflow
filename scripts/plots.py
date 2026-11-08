#!/usr/bin/env python3
"""Genera las gráficas del informe a partir de results/ (requiere matplotlib).

Uso: python3 scripts/plots.py   → results/fig_*.svg y results/resumen.json
"""
import collections
import csv
import json
import re
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = Path(__file__).resolve().parent.parent / "results"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
plt.rcParams.update({
    "font.family": "Inter", "font.size": 8.5, "axes.edgecolor": MUTED,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.linewidth": 0.6,
    "xtick.major.width": 0.6, "ytick.major.width": 0.6, "svg.fonttype": "path",
})

rows = list(csv.DictReader(open(R / "cli_runs.csv")))
g = collections.defaultdict(list)
for r in rows:
    g[(r["modo"], int(r["gomaxprocs"]), int(r["workers"]))].append(float(r["segundos"]))
t_seq = st.mean(g[("secuencial", 1, 1)])
P = [1, 2, 4, 8]
means = [st.mean(g[("concurrente", p, p)]) for p in P]
sds = [st.stdev(g[("concurrente", p, p)]) for p in P]
speed = [t_seq / m for m in means]
# Propagación simple del error relativo del cociente.
sd_seq = st.stdev(g[("secuencial", 1, 1)])
speed_err = [s * ((sd_seq / t_seq) ** 2 + (d / m) ** 2) ** 0.5 for s, d, m in zip(speed, sds, means)]

# Amdahl ajustado con el speedup medido en P = 2 (núcleos reales del equipo).
s2 = speed[1]
p_par = 2 * (1 - 1 / s2)
f_ser = 1 - p_par
amdahl = lambda n: 1 / (f_ser + p_par / n)

# --- Figura 1: speedup ---------------------------------------------------
fig, ax = plt.subplots(figsize=(3.55, 2.45))
ax.axvspan(2, 8.6, color="#f3f2ee", zorder=0)
ax.text(6.0, 2.75, "más goroutines que\nnúcleos físicos (2 vCPU)", ha="center", va="center", fontsize=7, color=MUTED)
ax.plot(P, P, ls=(0, (2, 2)), color=MUTED, lw=1.2, label="Ideal (lineal)")
xs = [1 + i * 0.05 for i in range(141)]
ax.plot(xs, [amdahl(x) for x in xs], color=ORANGE, lw=2, label=f"Amdahl (f = {f_ser*100:.1f} %)")
ax.errorbar(P, speed, yerr=speed_err, color=BLUE, lw=2, marker="o", ms=5.5, capsize=2.5,
            mec="white", mew=1.2, label="Medido (media ± 1σ)", zorder=5)
for p, s in zip(P, speed):
    ax.annotate(f"{s:.2f}×", (p, s), textcoords="offset points", xytext=(0, -13), ha="center", fontsize=7.2, color=INK)
ax.set_xticks(P)
ax.set_xlim(0.6, 8.6)
ax.set_ylim(0, 8.2)
ax.set_xlabel("GOMAXPROCS = goroutines por etapa")
ax.set_ylabel("Speedup  T_sec / T_P")
ax.grid(axis="y", color=GRID, lw=0.6)
ax.legend(frameon=False, fontsize=7, loc="upper left")
fig.tight_layout(pad=0.3)
fig.savefig(R / "fig_speedup.svg")

# --- Figura 2: tiempos ---------------------------------------------------
labels = ["Secuencial"] + [f"P = {p}" for p in P] + ["P=2 · W=1", "P=2 · W=4", "P=2 · W=8"]
keys = [("secuencial", 1, 1)] + [("concurrente", p, p) for p in P] + [("workers_p2", 2, w) for w in (1, 4, 8)]
vals = [st.mean(g[k]) for k in keys]
errs = [st.stdev(g[k]) for k in keys]
cols = [MUTED] + [BLUE] * 4 + [AQUA] * 3
fig, ax = plt.subplots(figsize=(3.55, 2.45))
y = list(range(len(labels)))[::-1]
ax.barh(y, vals, xerr=errs, color=cols, height=0.62, error_kw=dict(lw=0.8, capsize=2, ecolor=INK2))
for yi, v, e in zip(y, vals, errs):
    ax.text(v + e + 0.12, yi, f"{v:.2f} s", va="center", fontsize=7.2, color=INK)
ax.set_yticks(y, labels)
ax.set_xlim(0, 5.3)
ax.set_xlabel("Tiempo total (s), 120 imágenes 800×600")
ax.grid(axis="x", color=GRID, lw=0.6)
ax.tick_params(axis="y", length=0)
fig.tight_layout(pad=0.3)
fig.savefig(R / "fig_tiempos.svg")

# --- Figura 3: perfil por etapa -----------------------------------------
cum = {}
for line in open(R / "pprof_cum.txt"):
    m = re.search(r"([\d.]+)%\s+(\S+)(?: \(inline\))?$", line)
    if m:
        cum[m.group(2)] = float(m.group(1))
enc = cum.get("github.com/fcorzo/pixelflow/internal/imgio.Encode", 0)
png_enc = cum.get("image/png.(*Encoder).Encode", 0)
flt = cum.get("github.com/fcorzo/pixelflow/internal/filters.Chain.Apply", 0)
blur = cum.get("github.com/fcorzo/pixelflow/internal/filters.BoxBlur", 0)
dec = cum.get("github.com/fcorzo/pixelflow/internal/imgio.Decode", 0)
other = max(0.0, 100 - enc - flt - dec)
parts = [
    ("Codificar · PNG (deflate)", png_enc, ORANGE), ("Codificar · JPEG", enc - png_enc, "#f2a07d"),
    ("Filtrar · desenfoque", blur, BLUE), ("Filtrar · gris + resize", flt - blur, "#7fb0ea"),
    ("Decodificar", dec, AQUA), ("Runtime / GC / otros", other, "#c9c8c2"),
]
fig, ax = plt.subplots(figsize=(7.3, 1.05))
left = 0
for name, v, c in parts:
    ax.barh(0, v, left=left, color=c, height=0.55, edgecolor="white", lw=1.5)
    if v > 6:
        ax.text(left + v / 2, 0, f"{v:.0f} %", ha="center", va="center", fontsize=7.5, color="white" if c in (ORANGE, BLUE, AQUA) else INK)
    left += v
ax.set_xlim(0, 100)
ax.set_yticks([])
ax.spines["left"].set_visible(False)
ax.set_xlabel("% de las muestras de CPU (pprof, GOMAXPROCS = 2)")
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for _, _, c in parts]
ax.legend(handles, [n for n, _, _ in parts], ncol=6, frameon=False, fontsize=6.6,
          loc="lower center", bbox_to_anchor=(0.5, 1.0), handlelength=1, columnspacing=1.1)
fig.tight_layout(pad=0.2)
fig.savefig(R / "fig_perfil.svg")

# --- Resumen numérico para el informe -----------------------------------
b = collections.defaultdict(list)
for line in open(R / "bench.txt"):
    m = re.match(r"BenchmarkPipeline/(\S+?)(?:-(\d+))?\s+\d+\s+(\d+) ns/op", line)
    if m:
        b[f"{m.group(1)}|{m.group(2) or 1}"].append(float(m.group(3)) / 1e6)
summary = {
    "t_seq": t_seq, "sd_seq": sd_seq, "P": P, "means": means, "sds": sds, "speedup": speed,
    "speedup_err": speed_err, "f_serial": f_ser, "p_par": p_par,
    "amdahl": {p: amdahl(p) for p in P}, "amdahl_max": 1 / f_ser,
    "configs": {"|".join(map(str, k)): [st.mean(v), st.stdev(v), min(v), max(v), len(v)] for k, v in g.items()},
    "bench": {k: [st.mean(v), st.stdev(v), len(v)] for k, v in b.items()},
    "profile": {n: v for n, v, _ in parts},
}
json.dump(summary, open(R / "resumen.json", "w"), indent=1)
print(json.dumps({k: summary[k] for k in ("speedup", "f_serial", "amdahl", "amdahl_max", "profile")}, indent=1))
