***

# File Processor (CSV, JSON, LOG) — Elixir

## 1. Overview

This project processes **CSV**, **JSON**, and **LOG** files provided either as a directory or as a list of paths. It computes **per‑file metrics**, builds **CSV consolidated totals**, and generates a final **text report** strictly aligned to the internal “section 5.1” format. It supports:

*   **Sequential** mode
*   **Parallel** mode using `spawn` workers (timeouts per process + automatic retries)
*   **Compare** mode (runs sequential and parallel on the same input and includes a performance analysis block)

The report is saved to `reporte_final.txt` under an **output directory** which can be:

*   The sibling `output/` next to the input directory (default behavior), or
*   A custom directory supplied via CLI flag `--outdir`.

## 2. Requirements

*   Elixir `~> 1.19`
*   Mix (bundled with Elixir)

Dependencies (declared in `mix.exs`):

*   `nimble_csv` for RFC4180 CSV parsing
*   `jason` for JSON decoding

Install dependencies:

```bash
mix deps.get
```

## 3. Project Structure

    lib/
      procesador_archivos.ex    # Orchestrator: sequential, parallel (spawn + timeouts + retries), compare
      reader.ex                 # Readers for CSV/JSON/LOG
      metrics.ex                # Per-file metrics + CSV consolidated totals
      reporter.ex               # Report generator (section 5.1 formatting)
    ejecutable.ex               # CLI entry point (escript)
    output/                     # Generated reports (runtime)
    data/                       # Sample input files (optional, for local runs)
    test/
      procesador_archivos_test.exs
    mix.exs
    README.md

## 4. Building the Executable (escript)

From the project root:

```bash
mix deps.get
mix escript.build
```

This produces the `./procesador_archivos` executable in the project root.

> **Windows (PowerShell) example:**
>
> ```powershell
> escript .\procesador_archivos --mode sequential .\data
> ```

## 5. CLI Usage

    procesador_archivos [options] <directory | list_of_paths>

    Options:
      --mode sequential | parallel | compare
      --max <int>            (optional; parallel/compare) max concurrency
      --timeout <ms>         (optional; parallel/compare) coordinator receive timeout
      --per-timeout <ms>     (optional; parallel/compare) per-process timeout
      --retries <int>        (optional; parallel/compare) automatic retries per file
      --outdir <path>        (optional) output directory override
      --help

### 5.1 Examples

**Sequential (directory):**

```bash
./procesador_archivos --mode sequential ./data
```

**Parallel (list of files):**

```bash
./procesador_archivos --mode parallel ./data/ventas_enero.csv ./data/usuarios.json ./data/sistema.log
```

**Parallel (with timeouts, retries, and custom output dir):**

```bash
./procesador_archivos --mode parallel --per-timeout 3000 --retries 2 --outdir ./reportes ./data
```

**Compare (sequential + parallel with performance analysis):**

```bash
./procesador_archivos --mode compare --max 8 --timeout 20000 --per-timeout 12000 --retries 2 --outdir ./reportes ./data
```

On success, the CLI prints the path to the generated report, e.g.:

    Report generated: ./output/reporte_final.txt

(or to your custom directory if `--outdir` is provided).

## 6. Report Format (Section 5.1)

The final `reporte_final.txt` contains these blocks:

1.  **Header**: generation date/time, processed directory, processing mode
2.  **Executive Summary**: total files, per-type counts, total processing time, errors count, success rate
3.  **CSV Metrics (per file)** + **CSV Consolidated totals** (exact union of products across files)
4.  **JSON Metrics (per file)**
5.  **LOG Metrics (per file)**
6.  **Performance Analysis** (only in compare mode): sequential time, parallel time, speedup factor, processes used, max memory (placeholder)
7.  **Errors and Warnings**: list of files that failed or timed out (`%{file, detail}`)

## 7. Design Decisions

### 7.1 Parallelization with `spawn`

*   A **worker process** is created for each file via `spawn`. Results return to the **coordinator** through `send/receive`.
*   The coordinator maintains a **bounded pool** (size = `--max` or schedulers online) and keeps it **filled** while there are pending jobs.

### 7.2 Timeouts and Retries

*   **Per‑process timeout** (`--per-timeout`) schedules a message `{:worker_timeout, ref}`; if the worker still runs, it is **killed** and the job is **re‑enqueued** with decremented retries (until `--retries` reaches zero).
*   **Global receive timeout** (`--timeout`) protects the coordinator; if no messages arrive in the given period, the loop logs a warning and marks **all active jobs** as failed.
*   Errors are **normalized** into `%{file, detail}` and appended to `metrics.errors` for the report.

### 7.3 CSV Consolidation (Exact Uniques)

*   Each `csv_metrics/2` exposes per‑file metrics and the product set; consolidation performs a **set union** to compute exact cross‑file unique products.

### 7.4 Output Directory Resolution

*   By default, reports are written to an `output/` **sibling of the input base** (next to `data/`).
*   If `--outdir` is provided, the report is written **exactly** there.
*   A helper `resolve_out_dir/2` makes this behavior **consistent** for sequential, parallel, and compare flows.

### 7.5 JSON Reader Tolerance

*   The tests accept **atom** or **string** keys for `usuarios` and `sesiones`, to accommodate different decoder strategies.
*   Invalid JSON triggers a normalized error (e.g., `JSON read error: <message>`) and participates in the error list and counters.

## 8. Testing

Run all tests:

```bash
mix test
```

The suite creates **temporary fixtures** under a unique folder in your OS temp directory and asserts:

*   Reader correctness (CSV header columns; JSON keys; LOG entry fields)
*   Metrics presence and CSV consolidated totals
*   Reporter block presence (5.1)
*   Orchestrator behavior in sequential/parallel and compare, including:
    *   **Custom outdir**
    *   **Per‑process timeouts and retries**
    *   **Normalized errors** included in the final report

## 9. Troubleshooting

*   If you run on **Windows**, normalize paths when comparing strings in tests to avoid `\` vs `/` mismatches. In the suite, we use a helper: `path |> Path.expand() |> Path.split() |> Path.join()`.
*   For CLI flags, prefer dashed options (`--per-timeout`) which map to underscored keys (`:per_timeout`) in `OptionParser`.
*   Ensure the orchestrator merges module defaults with runtime options (`Map.merge(@default_opts, rt_opts)`) so all flows behave consistently with or without CLI flags.

## 10. License

MIT (or update to your organization’s preferred license).

***
