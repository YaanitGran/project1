# File Processor (CSV/JSON/LOG) — README

> **Language note**: public API, comments, and this README are in **English**.  
> Console output and report text are in **Spanish**, as required by the project brief.

***

## Table of Contents

*   \#overview
*   \#key-features
*   \#architecture
    *   \#public-api
    *   \#internal-modules
*   \#installation--requirements
*   \#data-contracts
    *   \#csv
    *   \#json
    *   \#log
*   \#how-to-use
    *   \#process-a-directory
    *   \#process-a-file-list
    *   \#process-with-options-spanish-wrapper
    *   \#errorhandlinginspection-that-always-writes-a-report
    *   \#benchmark
    *   \#where-is-the-report-written
*   \#return-shapes
*   \#execution-examples
*   \#design-decisions
    *   \#concurrency-model
    *   \#timeout-semantics
    *   \#argument-validation--ux
    *   \#reporter-design
    *   \#csv-policy
    *   \#json-error-normalization
    *   \#log-metrics
*   \#performance-notes
*   \#testing
*   \#troubleshooting

***

## Overview

This project processes **CSV**, **JSON**, and **LOG** files, computes per‑file metrics, and generates a human‑readable **Spanish** report. It supports **sequential** and **parallel** execution. The parallel mode is built on `Task.async_stream/3` with **robust timeout handling** that never aborts the whole run just because one file is slow or broken.

***

## Key Features

*   📁 Process a directory or an explicit **list of files** (mixed types).
*   ⚡ Parallel mode with `Task.async_stream/3`, `ordered: true`, and **per‑file isolation**.
*   ⏱️ **Timeouts** do not kill the whole run (errors are recorded and processing continues).
*   🧾 A single **Spanish report** with:
    *   Conditional “MÉTRICAS” sections (CSV/JSON/LOG printed **only if present**).
    *   **Grouped** error blocks per file (CSV line errors, JSON categories, timeouts).
    *   Summary with **unique error files** and **success rate**.
    *   “Tiempo total de procesamiento” printed in **seconds with 3 decimals**.
*   🧪 A comprehensive test suite covering public API, readers, metrics, reporter, and pipeline behavior.

***

## Architecture

### Public API

All public functions live in `ProcesadorArchivos`:

*   `process_directory/2`
*   `process_files/2`
*   `procesar_con_opciones/2`  *(Spanish wrapper; maps `timeout` → `timeout_ms`)*
*   `procesar_con_manejo_errores/1`  *(now **always** generates a report)*
*   `benchmark/2`  *(returns timings & stats)*
*   `benchmark_paralelo_vs_secuencial/1`  *(prints short timing summary)*

> **Only these six functions are meant to be used directly.**

### Internal modules

Internal implementation details are namespaced under something like `PAInternal.*` (readers, metrics, classifier, pipeline). The **reporter** module is public (`ProcesadorArchivos.Reporter`) because it’s a reusable renderer, although you generally consume it via the API.

*   Readers: `CSVReader`, `JSONReader`, `LogReader`
*   Metrics: `CSVMetrics`, `JSONMetrics`, `LOGMetrics`
*   Orchestration: `Classifier` (type detection), `Pipeline` (sequential/parallel run)
*   Rendering: `ProcesadorArchivos.Reporter`

***

## Installation & Requirements

*   **Elixir**: developed and tested with **Elixir 1.19.x** and recent OTP.
*   **Dependencies** (declared in `mix.exs`):
    *   `:jason` (JSON parsing)
    *   `:nimble_csv` (CSV parsing)

Install deps and run:

```bash
mix deps.get
mix compile
```

***

## Data Contracts

### CSV

**Expected header** (exact match, in Spanish):

    fecha,producto,categoria,precio_unitario,cantidad,descuento

Per‑row validations include (non‑exhaustive):

*   `producto` and `categoria` must be **non‑empty**.
*   `precio_unitario` must be a valid positive float.
*   `cantidad` must be a valid positive integer.
*   `descuento` must be a valid float in **\[0, 100]**.
*   **Exactly 6 columns** per row.

> **CSV policy:** if a CSV has **any** invalid row(s), the **whole file** is treated as **error** (no metrics returned for that file). The final report **groups** the per‑line messages under a single block for that file.

### JSON

Expected shape:

```json
{
  "usuarios": [ ... ],
  "sesiones": [ ... ]
}
```

*   If the JSON is **malformed** (decoder error), the reader returns **syntax categories** (e.g. “Comillas faltantes”, “Comas faltantes”, “Llaves sin comillas”, “Comentarios no válidos en JSON”, “Corchetes/llaves sin cerrar”).
*   If the JSON is structurally valid, the reader validates **element‑level** fields (types and domain constraints) and collects element errors. The pipeline still computes **metrics** for valid data (unless you choose otherwise).
*   The reporter **groups** categories per file.

### LOG

Expected format (one entry per line):

    YYYY-MM-DD HH:MM:SS [LEVEL] [COMPONENT] Message

*   Recognized levels include: `DEBUG, INFO, WARN, ERROR, FATAL`.
*   The metrics include distributions, **top error component**, **hourly distribution**, **top messages**, and **average seconds between ERROR/FATAL events**.

***

## How to Use

> All examples below assume you run `iex -S mix` at the repo root.

### Process a directory

```elixir
{:ok, out} =
  ProcesadorArchivos.process_directory("./data", %{
    mode: :parallel,          # or :sequential
    max_workers: 8,           # default: System.schedulers_online()
    timeout_ms: 10_000,       # per-file timeout
    progress: true,           # print "Procesados X/Y"
    out: "output/reporte_final.txt"
  })
```

### Process a file list

```elixir
files = [
  "data/ventas_enero.csv",
  "data/usuarios.json",
  "data/sistema.log"
]

{:ok, out} = ProcesadorArchivos.process_files(files, %{mode: :parallel})
```

> If some files don’t exist, they are added to the error list as  
> `"<path>: No se encontró el archivo"`, and the rest are processed normally.

### Process with options (Spanish wrapper)

```elixir
# 'timeout' is mapped to 'timeout_ms' internally.
ProcesadorArchivos.procesar_con_opciones("./data", %{mode: :parallel, timeout: 10_000})
# => :ok
```

### Error‑handling/inspection that **always** writes a report

```elixir
# Accepts a single file, a directory, or a list of files.
# ALWAYS writes a report and returns the same shape as process_files/2.
{:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores("data/usuarios_malformado.json")

{:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores(["a.csv","b.json"], %{mode: :parallel})

{:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores("./data")
```

### Benchmark

```elixir
# Structured result (timings in ms, speedup, processes used, etc.)
{:ok, b} = ProcesadorArchivos.benchmark("./data", %{progress: false})
IO.inspect(b)
```

```elixir
# Spanish wrapper that prints in seconds (3 decimals)
ProcesadorArchivos.benchmark_paralelo_vs_secuencial("./data")
# Secuencial: 0.123 s
# Paralelo:   0.045 s
# Mejora:     2.73x
```

### Where is the report written?

By default to `output/reporte_final.txt`. You can override it via `out: "path/to/report.txt"` in the options.

***

## Return Shapes

All processing functions (`process_directory/2`, `process_files/2`, `procesar_con_manejo_errores/1`) return:

```elixir
{:ok,
  %{
    results: [{path, type, metrics_map, payload_map}, ...],
    errors:  [string(), ...],
    duration_ms: integer(),
    out: "path/to/report.txt"
  }
}
```

*   `type` is one of `:csv | :json | :log`.
*   `metrics_map` varies per type (see Metrics modules).
*   `payload_map` contains raw error collections (e.g., `row_errors`, `element_errors`, `line_errors`) if applicable.
*   `errors` are human‑readable strings for file‑level problems, **grouped** by the reporter.

***

## Execution Examples

**Directory (parallel)**

```elixir
{:ok, out} = ProcesadorArchivos.process_directory("./data", %{mode: :parallel})
IO.puts(out.out)
# => output/reporte_final.txt
```

**File list (sequential)**

```elixir
{:ok, out} =
  ProcesadorArchivos.process_files(
    ["./data/ventas_ok.csv", "./data/usuarios.json"],
    %{mode: :sequential, out: "output/reporte_ejec1.txt"}
  )
```

**Error‑handling/inspection (single file)**

```elixir
{:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores("./data/usuarios_malformado.json")
```

**Benchmark**

```elixir
ProcesadorArchivos.benchmark_paralelo_vs_secuencial("./data")
```

***

## Design Decisions

### Concurrency model

*   **Why `Task.async_stream/3`?**  
    It offers **bounded concurrency** with minimal boilerplate and good fault‑isolation for per‑item work.
*   **`ordered: true` + `Enum.zip/2`:**  
    We pair each finished task with its original path to identify which file timed out or crashed without extra mutable state.
*   **Retries & timeouts:**  
    `Task.async_stream/3` is used with `on_timeout: :kill_task`. The stream keeps going and yields `{:exit, :timeout}` for that item.

### Timeout semantics

*   **Do not abort** the whole run on a per‑file timeout.
*   Record a friendly error:  
    `"<path>: Tiempo de espera excedido (timeout)"`.
*   Continue processing the rest of the files.

### Argument validation & UX

*   **Helpful `ArgumentError` messages** with usage examples:
    *   Directory must exist for `process_directory/2` and wrappers.
    *   File lists must be lists of **string paths** to **regular files**.
    *   Missing files are converted to errors and reported in the output.

### Reporter design

*   **Conditional metric sections**: only print CSV/JSON/LOG sections if there are results of those types.
*   **Grouped errors per file**:
    *   CSV: parse a single aggregated message (`csv_has_corrupt_lines -> ...`) into one block with **one bullet per line error**.
    *   JSON: group **syntax/validation categories** under one block.
    *   Timeouts: group all timeout messages by file.
*   **Success rate** uses the **count of unique error files** (not the count of messages).
*   **Duration formatting**: seconds with **3 decimals** everywhere for consistency.

### CSV policy

*   If **any** row is invalid, the **entire file** is treated as error (no metrics).
*   The report still shows all row‑level diagnostics under the file block.

> Implementation detail: the CSV reader enforces header and per‑row validations and returns `rows` + `row_errors`. The pipeline escalates to a **file‑level error** when `row_errors != []`.

### JSON error normalization

*   Malformed JSON (decoder errors) is mapped to **categories** by inspecting the decoder message and the raw content (e.g., **missing quotes/commas**, **unquoted keys**, **comments**, **unclosed braces/brackets**).
*   Element‑level (semantic) errors are categorized (e.g., **invalid types**, **negative durations**) and kept in the payload; the reporter can show them grouped.

### LOG metrics

*   `LOGMetrics` computes totals, distributions, **top error component**, hourly histogram, frequent messages, and **average seconds between ERROR/FATAL**.
*   We switched the “critical” window to **ERROR/FATAL** (instead of FATAL‑only) as requested, making the metric applicable to more files.

***

## Performance Notes

*   For **very small datasets**, parallel overhead may dominate.
*   The benchmark runner disables progress printing (`progress: false`) for fair measurements.
*   Good starting point for parallelism: `max_workers = min(file_count, 2 * System.schedulers_online())`.
*   **I/O bound** datasets (lots of files on the same drive) may prefer fewer workers.

***

## Testing

Run the whole suite:

```bash
mix test
```

Notes:

*   Tests normalize CRLF→LF to avoid surprises on Windows.
*   Example fixtures intentionally include corrupt data to verify error paths, grouping, and metrics.
*   If you customize data formats (e.g., LOG line regex), update the **samples in tests** accordingly.

***

## Troubleshooting

*   **“KeyError :out”** → The public API merges defaults; ensure you pass a map for options or rely on the default `out: "output/reporte_final.txt"`.
*   **“cannot pipe ... into ... <> ...”** → In tests, don’t pipe into `<>`. Use a helper that trims and concatenates in two steps.
*   **“FunctionClauseError in Kernel.=\~” (in tests)** → Remember `assert_raise/2` returns the **exception struct**; use `Exception.message(exc)` (tests already do).
*   **No metrics for a CSV with any row error** → This is by design (file‑level error if \`row\_errors != #csv-policy.
*   **JSON malformed produces multiple lines** → It’s expected; the reporter **groups** them under one block per file.

***
