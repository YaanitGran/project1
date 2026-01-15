
defmodule ProcesadorArchivos do
  alias ProcesadorArchivos.{Reader, Metrics, Reporter}
  require Logger

  @moduledoc """
  Procesa archivos CSV/JSON/LOG y genera un reporte en `output/reporte_final.txt`.
  """

  @type paths_or_dir :: String.t() | [String.t()]

  # Defaults for parallel mode (timeouts + retries + error logging)
  @default_opts %{
    max_concurrency: System.schedulers_online(),
    per_process_timeout_ms: 10_000,  # per worker timeout (10s)
    timeout_ms: 15_000,              # coordinator receive timeout (global)
    max_retries: 1,                  # automatic retries per file on failure/timeout
    out_dir: nil                     # optional override for output directory
  }

  # -------------------------
  # Public entrypoints
  # -------------------------

  @spec process(paths_or_dir) :: {:ok, String.t()} | {:error, String.t()}
  def process(paths_or_dir), do: process(paths_or_dir, :secuencial, %{})

  @spec process(paths_or_dir, :secuencial | :paralelo | :compare) ::
          {:ok, String.t()} | {:error, String.t()}
  def process(paths_or_dir, modo), do: process(paths_or_dir, modo, %{})

  @spec process(paths_or_dir, :secuencial | :paralelo | :compare, map()) ::
          {:ok, String.t()} | {:error, String.t()}
          # In procesador_archivos.ex
  def process(paths_or_dir, modo, opts) when modo in [:secuencial, :paralelo, :compare] and is_map(opts) do
    # Filter out nil values from CLI so they don't overwrite module defaults
    sanitized_opts = Map.filter(opts, fn {_, v} -> not is_nil(v) end)
    opts = Map.merge(@default_opts, sanitized_opts)


    with {:ok, validated_path} <- validate_input(paths_or_dir) do
      case modo do
        :secuencial -> process_sequential(validated_path, opts)
        :paralelo   -> process_parallel(validated_path, opts)
        :compare    -> compare(validated_path, opts)
      end
    end
  end
  # -------------------------
  # Sequential processing (per-file metrics + 5.1 report)
  # -------------------------
  # NOTE (EN): Sequential mode accepts 'opts' to respect --outdir when provided.
  def process_sequential(paths_or_dir, opts \\ %{}) do
    start_ms = System.monotonic_time(:millisecond)

    paths = collect_paths(paths_or_dir)
    {csv_files, json_files, log_files} = classify_by_extension(paths)

    # CSV per-file
    csv_per_file =
      for file <- csv_files do
        rows = Reader.read_csv(file)
        Metrics.csv_metrics(rows, Path.basename(file))
      end

    csv_consolidated = Metrics.csv_consolidated(csv_per_file)

    # JSON per-file
    json_per_file =
      for file <- json_files, into: [] do
        case Reader.read_json(file) do
          {:ok, data} -> Metrics.json_metrics(data, Path.basename(file))
          _ -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    # LOG per-file
    log_per_file =
      for file <- log_files, into: [] do
        case Reader.read_log(file) do
          {:ok, entries} -> Metrics.log_metrics(entries, Path.basename(file))
          _ -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    stop_ms = System.monotonic_time(:millisecond)
    total_time_s = (stop_ms - start_ms) / 1000.0

    # Build 'meta' safely (avoid inline-if ambiguity in maps by using parentheses)
    meta = %{
      generated_at: NaiveDateTime.local_now(),
      mode: :secuencial,
      root_dir: (
        if is_binary(paths_or_dir) and File.dir?(paths_or_dir) do
          Path.expand(paths_or_dir)
        else
          "N/A"
        end
      ),
      total_time_s: total_time_s,
      counts: %{
        csv: length(csv_per_file),
        json: length(json_per_file),
        log: length(log_per_file),
        total: length(paths)
      },
      errors_count: 0,
      success_rate: 100.0
    }

    metrics = %{
      meta: meta,
      csv: %{per_file: csv_per_file, consolidated: csv_consolidated},
      json: %{per_file: json_per_file},
      log:  %{per_file: log_per_file},
      performance: nil,
      errors: []
    }

    report_text = Reporter.generate(metrics, :secuencial)

    # Write into output dir (opts-aware)
    out_dir = resolve_out_dir(paths_or_dir, opts)
    File.mkdir_p!(out_dir)
    out_path = Path.join(out_dir, "reporte_final.txt")
    File.write!(out_path, report_text)

    {:ok, out_path}
  end

  # -------------------------
  # Parallel processing (spawn workers + mailbox + per-process timeout + retries)
  # -------------------------
  defp process_parallel(paths_or_dir, opts) do
    start_ms = System.monotonic_time(:millisecond)

    paths = collect_paths(paths_or_dir)
    {csv_files, json_files, log_files} = classify_by_extension(paths)

    # Build job list: one job per file with its type and remaining retries
    max_retries = Map.get(opts, :max_retries, 1)

    jobs =
      Enum.map(csv_files,  &{:csv,  &1, max_retries}) ++
      Enum.map(json_files, &{:json, &1, max_retries}) ++
      Enum.map(log_files,  &{:log,  &1, max_retries})

    total           = length(jobs)
    max_concurrency = Map.get(opts, :max_concurrency, System.schedulers_online())
    timeout_ms      = Map.get(opts, :timeout_ms, 15_000)
    per_timeout_ms  = Map.get(opts, :per_process_timeout_ms, 10_000)

    parent      = self()
    queue       = jobs
    active      = %{}                 # ref -> %{job: {type,path,retries_left}, pid: pid}
    results     = %{csv: [], json: [], log: []}
    errors      = 0
    processed   = 0
    errors_list = []                  # [%{file, detail}]

    # Spawn a worker for the given job {type, path, retries_left}
    spawn_worker = fn {type, path, retries_left} ->
      ref = make_ref()

      pid =
        spawn(fn ->
          # Worker body: read + compute per-file metrics
          res =
            case type do
              :csv ->
                rows = Reader.read_csv(path)
                # Early CSV validation: RFC4180 header must have 6 columns
                if valid_csv_rows?(rows) do
                  Metrics.csv_metrics(rows, Path.basename(path))
                else
                  {:error, {:csv_invalid, Path.basename(path), "Invalid CSV header or column count"}}
                end

              :json ->
                case Reader.read_json(path) do
                  {:ok, data}   -> Metrics.json_metrics(data, Path.basename(path))
                  {:error, reason} ->
                    {:error, {:json_read_error, Path.basename(path), to_string(reason)}}
                end

              :log ->
                case Reader.read_log(path) do
                  {:ok, entries} -> Metrics.log_metrics(entries, Path.basename(path))
                  {:error, reason} ->
                    {:error, {:log_read_error, Path.basename(path), to_string(reason)}}
                end
            end

          send(parent, {:worker_done, ref, type, path, retries_left, res})
        end)

      # Schedule per-process timeout (to the coordinator mailbox)
      Process.send_after(parent, {:worker_timeout, ref}, per_timeout_ms)

      {ref, pid}
    end

    # Launch up to max_concurrency initial workers (avoid 1..0 range warning)
    initial = min(max_concurrency, length(queue))
    {active, queue} =
      if initial > 0 do
        Enum.reduce(1..initial, {active, queue}, fn _, {act, q} ->
          case q do
            [] -> {act, q}
            [job | rest] ->
              {ref, pid} = spawn_worker.(job)
              {Map.put(act, ref, %{job: job, pid: pid}), rest}
          end
        end)
      else
        {active, queue}
      end

    # Receive loop: collect results, handle timeouts, apply retries, keep the pool filled
    {results, errors, _processed, errors_list} =
      parallel_loop_retries(active, queue, results, errors, processed, total, timeout_ms, spawn_worker, errors_list)

    # Build per-file lists
    csv_per_file  = Enum.reverse(results.csv)
    json_per_file = Enum.reverse(results.json)
    log_per_file  = Enum.reverse(results.log)

    # Consolidate CSV totals
    csv_consolidated = Metrics.csv_consolidated(csv_per_file)

    stop_ms       = System.monotonic_time(:millisecond)
    total_time_s  = (stop_ms - start_ms) / 1000.0

    # Meta block (5.1 header + executive summary)
    meta = %{
      generated_at: NaiveDateTime.local_now(),
      mode: :paralelo,
      root_dir: (
        if is_binary(paths_or_dir) and File.dir?(paths_or_dir) do
          Path.expand(paths_or_dir)
        else
          "N/A"
        end
      ),
      total_time_s: total_time_s,
      counts: %{csv: length(csv_per_file), json: length(json_per_file), log: length(log_per_file), total: total},
      errors_count: length(errors_list),
      success_rate: (
        if total > 0 do
          (total - length(errors_list)) / total * 100.0
        else
          100.0
        end
      )
    }

    metrics = %{
      meta: meta,
      csv:  %{per_file: csv_per_file, consolidated: csv_consolidated},
      json: %{per_file: json_per_file},
      log:  %{per_file: log_per_file},
      performance: %{
        parallel_s: total_time_s,
        sequential_s: nil,
        speedup_x: nil,
        processes_used: System.schedulers_online(),
        max_memory_mb: nil
      },
      errors: errors_list
    }

    report_text = Reporter.generate(metrics, :paralelo)

    out_dir = resolve_out_dir(paths_or_dir, opts)
    File.mkdir_p!(out_dir)
    out_path = Path.join(out_dir, "reporte_final.txt")
    File.write!(out_path, report_text)

    {:ok, out_path}
  end

  # -------------------------
  # Mailbox loop (timeouts + retries)
  # -------------------------
  # NOTE (EN): Handles {:worker_done} and {:worker_timeout}; retries if retries_left > 0.
    defp parallel_loop_retries(active, queue, results, errors, processed, total, timeout_ms, spawn_worker, errors_list) do
      cond do
        # Base case: No more active workers and the queue is empty
        map_size(active) == 0 and queue == [] ->
          {results, errors, processed, errors_list}

        true ->
          receive do
            # 1. Worker finished its task (either success or controlled error)
            {:worker_done, ref, type, path, retries_left, res} ->
              processed = processed + 1
              pct = if total > 0, do: Float.round(processed / total * 100.0, 1), else: 100.0
              IO.puts("Processed #{processed}/#{total} files (#{pct}%) - #{Path.basename(path)}")

              # Update state based on the worker's result [cite: 73, 74]
              {new_results, new_errors, new_queue, new_errors_list} =
                case res do
                  {:error, detail} ->
                    if retries_left > 0 do
                      Logger.warning("Retrying #{Path.basename(path)} (remaining: #{retries_left - 1})")
                      # Put the job back in the queue with decremented retries [cite: 75]
                      {results, errors, [{type, path, retries_left - 1} | queue], errors_list}
                    else
                      Logger.error("Worker error on #{Path.basename(path)}: #{inspect(detail)}")
                      err_item = error_item_from_detail(detail)
                      {results, errors + 1, queue, [err_item | errors_list]}
                    end

                  m when is_map(m) ->
                    # Add valid metrics to the corresponding category [cite: 78, 79, 80]
                    case type do
                      :csv  -> {%{results | csv: [m | results.csv]}, errors, queue, errors_list}
                      :json -> {%{results | json: [m | results.json]}, errors, queue, errors_list}
                      :log  -> {%{results | log: [m | results.log]}, errors, queue, errors_list}
                    end
                end

              # Remove finished process from active map and fill pool [cite: 81]
              active_after_done = Map.delete(active, ref)
              {final_active, final_queue} = fill_pool(active_after_done, new_queue, spawn_worker)

              parallel_loop_retries(final_active, final_queue, new_results, new_errors, processed, total, timeout_ms, spawn_worker, new_errors_list)

            # 2. Per-process timeout fired (the worker took too long) [cite: 83]
            {:worker_timeout, ref} ->
              case Map.get(active, ref) do
                nil ->
                  # Late message: the worker finished just before the timeout was processed [cite: 84]
                  parallel_loop_retries(active, queue, results, errors, processed, total, timeout_ms, spawn_worker, errors_list)

                %{job: {type, path, retries_left}, pid: pid} ->
                  Logger.warning("Per-process timeout reached for #{Path.basename(path)}. Killing worker.")
                  Process.exit(pid, :kill)

                  # Determine if we should retry the timed-out file [cite: 85, 86, 88]
                  {new_queue, new_errors_list, new_errors} =
                    if retries_left > 0 do
                      Logger.warning("Retrying after timeout #{Path.basename(path)} (remaining: #{retries_left - 1})")
                      {[{type, path, retries_left - 1} | queue], errors_list, errors}
                    else
                      err_item = %{file: Path.basename(path), detail: "Timeout per process"}
                      {queue, [err_item | errors_list], errors + 1}
                    end

                  # Update state: Remove from active and spawn next available job [cite: 85, 89]
                  active_after_timeout = Map.delete(active, ref)
                  {final_active, final_queue} = fill_pool(active_after_timeout, new_queue, spawn_worker)

                  parallel_loop_retries(final_active, final_queue, results, new_errors, processed, total, timeout_ms, spawn_worker, new_errors_list)
              end

          after
            # 3. Global receive timeout (the coordinator hasn't heard from anyone) [cite: 91]
            timeout_ms ->
              Logger.warning("Global receive timeout reached (#{timeout_ms} ms). Terminating all active workers.")
              {final_errors_list, final_errors_count} =
                Enum.reduce(active, {errors_list, errors}, fn {_ref, %{job: {_, path, _}, pid: pid}}, {elist, ecount} ->
                  Process.exit(pid, :kill) # [cite: 92]
                  {[%{file: Path.basename(path), detail: "Global receive timeout"} | elist], ecount + 1}
                end)

              {results, final_errors_count, processed, final_errors_list}
          end
      end
    end

    # Helper to maintain the pool of active workers
    defp fill_pool(active, [], _spawn_worker), do: {active, []}
    defp fill_pool(active, [next_job | rest], spawn_worker) do
      # This ensures we always have a worker running for each available slot
      {new_ref, new_pid} = spawn_worker.(next_job)
      {Map.put(active, new_ref, %{job: next_job, pid: new_pid}), rest}
    end
  # --- Map low-level error detail to a Reporter-friendly item ---
  # NOTE: Converts internal tuples to %{file, detail} expected by Reporter.
  defp error_item_from_detail({:csv_invalid, file, msg}),     do: %{file: file, detail: msg}
  defp error_item_from_detail({:json_read_error, file, msg}), do: %{file: file, detail: "JSON read error: " <> msg}
  defp error_item_from_detail({:log_read_error,  file, msg}), do: %{file: file, detail: "Log read error: "  <> msg}
  defp error_item_from_detail(other),                         do: %{file: "N/A", detail: inspect(other)}

  # -------------------------
  # Compare sequential vs parallel on the same input (single final report)
  # -------------------------
  def compare(paths_or_dir, opts \\ %{}) do
    # Merge defaults with runtime opts
    opts = Map.merge(@default_opts, opts)

    # Run sequential and measure time
    seq_start = System.monotonic_time(:millisecond)
    {:ok, _seq_metrics} = collect_metrics_sequential(paths_or_dir)
    seq_stop  = System.monotonic_time(:millisecond)
    seq_s     = (seq_stop - seq_start) / 1000.0

    # Run parallel and measure time
    par_start = System.monotonic_time(:millisecond)
    {:ok, par_metrics} = collect_metrics_parallel(paths_or_dir, opts)
    par_stop  = System.monotonic_time(:millisecond)
    par_s     = (par_stop - par_start) / 1000.0

    speedup_x = if par_s > 0, do: seq_s / par_s, else: nil

    csv_per_file  = par_metrics.csv.per_file
    json_per_file = par_metrics.json.per_file
    log_per_file  = par_metrics.log.per_file

    csv_consolidated = Metrics.csv_consolidated(csv_per_file)

    meta = %{
      generated_at: NaiveDateTime.local_now(),
      mode: :paralelo,
      root_dir: (
        if is_binary(paths_or_dir) and File.dir?(paths_or_dir) do
          Path.expand(paths_or_dir)
        else
          "N/A"
        end
      ),
      total_time_s: par_s,  # header shows parallel time
      counts: %{
        csv: length(csv_per_file),
        json: length(json_per_file),
        log:  length(log_per_file),
        total: length(collect_paths(paths_or_dir))
      },
      errors_count: par_metrics.meta.errors_count,
      success_rate: par_metrics.meta.success_rate
    }

    metrics = %{
      meta: meta,
      csv:  %{per_file: csv_per_file, consolidated: csv_consolidated},
      json: %{per_file: json_per_file},
      log:  %{per_file: log_per_file},
      performance: %{
        sequential_s: seq_s,
        parallel_s:   par_s,
        speedup_x:    speedup_x,
        processes_used: System.schedulers_online(),
        max_memory_mb: nil
      },
      errors: par_metrics.errors
    }

    report_text = Reporter.generate(metrics, :paralelo)

    out_dir = resolve_out_dir(paths_or_dir, opts)
    File.mkdir_p!(out_dir)
    out_path = Path.join(out_dir, "reporte_final.txt")
    File.write!(out_path, report_text)

    {:ok, out_path}
  end

  # -------------------------
  # Collect helpers (no file writes)
  # -------------------------
  # NOTE (EN): Sequential collection reuses the same pipelines as process_sequential but returns maps.
  defp collect_metrics_sequential(paths_or_dir) do
    start_ms = System.monotonic_time(:millisecond)

    paths = collect_paths(paths_or_dir)
    {csv_files, json_files, log_files} = classify_by_extension(paths)

    csv_per_file =
      for file <- csv_files do
        rows = Reader.read_csv(file)
        Metrics.csv_metrics(rows, Path.basename(file))
      end

    json_per_file =
      for file <- json_files, into: [] do
        case Reader.read_json(file) do
          {:ok, data} -> Metrics.json_metrics(data, Path.basename(file))
          _ -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    log_per_file =
      for file <- log_files, into: [] do
        case Reader.read_log(file) do
          {:ok, entries} -> Metrics.log_metrics(entries, Path.basename(file))
          _ -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    stop_ms = System.monotonic_time(:millisecond)
    total_time_s = (stop_ms - start_ms) / 1000.0

    meta = %{
      generated_at: NaiveDateTime.local_now(),
      mode: :secuencial,
      root_dir: (
        if is_binary(paths_or_dir) and File.dir?(paths_or_dir) do
          Path.expand(paths_or_dir)
        else
          "N/A"
        end
      ),
      total_time_s: total_time_s,
      counts: %{
        csv: length(csv_per_file),
        json: length(json_per_file),
        log:  length(log_per_file),
        total: length(paths)
      },
      errors_count: 0,
      success_rate: 100.0
    }

    {:ok, %{
      meta: meta,
      csv:  %{per_file: csv_per_file},
      json: %{per_file: json_per_file},
      log:  %{per_file: log_per_file},
      errors: []
    }}
  end

  # NOTE (EN): Parallel collection mirrors process_parallel but does not write the report.
  defp collect_metrics_parallel(paths_or_dir, opts) do
    start_ms = System.monotonic_time(:millisecond)

    paths = collect_paths(paths_or_dir)
    {csv_files, json_files, log_files} = classify_by_extension(paths)

    max_retries = Map.get(opts, :max_retries, 1)

    jobs =
      Enum.map(csv_files,  &{:csv,  &1, max_retries}) ++
      Enum.map(json_files, &{:json, &1, max_retries}) ++
      Enum.map(log_files,  &{:log,  &1, max_retries})

    total           = length(jobs)
    max_concurrency = Map.get(opts, :max_concurrency, System.schedulers_online())
    timeout_ms      = Map.get(opts, :timeout_ms, 15_000)
    per_timeout_ms  = Map.get(opts, :per_process_timeout_ms, 10_000)

    parent      = self()
    queue       = jobs
    active      = %{}
    results     = %{csv: [], json: [], log: []}
    errors      = 0
    processed   = 0
    errors_list = []

    spawn_worker = fn {type, path, retries_left} ->
      ref = make_ref()

      pid =
        spawn(fn ->
          res =
            case type do
              :csv ->
                rows = Reader.read_csv(path)
                if valid_csv_rows?(rows) do
                  Metrics.csv_metrics(rows, Path.basename(path))
                else
                  {:error, {:csv_invalid, Path.basename(path), "Invalid CSV header or column count"}}
                end

              :json ->
                case Reader.read_json(path) do
                  {:ok, data} -> Metrics.json_metrics(data, Path.basename(path))
                  {:error, reason} -> {:error, {:json_read_error, Path.basename(path), to_string(reason)}}
                end

              :log ->
                case Reader.read_log(path) do
                  {:ok, entries} -> Metrics.log_metrics(entries, Path.basename(path))
                  {:error, reason} -> {:error, {:log_read_error, Path.basename(path), to_string(reason)}}
                end
            end

          send(parent, {:worker_done, ref, type, path, retries_left, res})
        end)

      Process.send_after(parent, {:worker_timeout, ref}, per_timeout_ms)
      {ref, pid}
    end

    initial = min(max_concurrency, length(queue))
    {active, queue} =
      if initial > 0 do
        Enum.reduce(1..initial, {active, queue}, fn _, {act, q} ->
          case q do
            [] -> {act, q}
            [job | rest] ->
              {ref, pid} = spawn_worker.(job)
              {Map.put(act, ref, %{job: job, pid: pid}), rest}
          end
        end)
      else
        {active, queue}
      end

    {results, errors, _processed, errors_list} =
      parallel_loop_retries(active, queue, results, errors, processed, total, timeout_ms, spawn_worker, errors_list)

    csv_per_file  = Enum.reverse(results.csv)
    json_per_file = Enum.reverse(results.json)
    log_per_file  = Enum.reverse(results.log)

    stop_ms       = System.monotonic_time(:millisecond)
    total_time_s  = (stop_ms - start_ms) / 1000.0
    success_rate  = (
      if total > 0 do
        (total - length(errors_list)) / total * 100.0
      else
        100.0
      end
    )

    meta = %{
      generated_at: NaiveDateTime.local_now(),
      mode: :paralelo,
      root_dir: (
        if is_binary(paths_or_dir) and File.dir?(paths_or_dir) do
          Path.expand(paths_or_dir)
        else
          "N/A"
        end
      ),
      total_time_s: total_time_s,
      counts: %{
        csv: length(csv_per_file),
        json: length(json_per_file),
        log:  length(log_per_file),
        total: total
      },
      errors_count: length(errors_list),
      success_rate: success_rate
    }

    {:ok, %{
      meta: meta,
      csv:  %{per_file: csv_per_file},
      json: %{per_file: json_per_file},
      log:  %{per_file: log_per_file},
      errors: errors_list
    }}
  end

  # -------------------------
  # Helpers: output dir, validation, file collection, classification
  # -------------------------

  # Wrapper (backwards-compatible)
  defp resolve_out_dir(paths_or_dir), do: resolve_out_dir(paths_or_dir, %{})

  # opts-aware version: respects --outdir when provided
  defp resolve_out_dir(paths_or_dir, opts) when is_map(opts) do
    case Map.get(opts, :out_dir) do
      dir when is_binary(dir) and dir != "" ->
        Path.expand(dir)

      _ ->
        cond do
          is_binary(paths_or_dir) and File.dir?(paths_or_dir) ->
            Path.join(Path.dirname(Path.expand(paths_or_dir)), "output")

          is_binary(paths_or_dir) and File.regular?(paths_or_dir) ->
            Path.join(Path.dirname(Path.expand(paths_or_dir)), "output")

          is_list(paths_or_dir) and paths_or_dir != [] ->
            Path.join(Path.dirname(Path.expand(List.first(paths_or_dir))), "output")

          true ->
            Path.expand("output")
        end
    end
  end

  defp validate_input(paths_or_dir) do
    cond do
      is_list(paths_or_dir) ->
        paths = paths_or_dir |> Enum.map(&Path.expand/1) |> Enum.filter(&File.exists?/1)
        if paths == [], do: {:error, "Lista vacía o rutas inexistentes"}, else: {:ok, paths}

      is_binary(paths_or_dir) and File.exists?(paths_or_dir) ->
        {:ok, paths_or_dir}

      true ->
        {:error, "Ruta inválida o no existe"}
    end
  end

  defp collect_paths(paths_or_dir) do
    cond do
      is_list(paths_or_dir) ->
        paths_or_dir |> Enum.map(&Path.expand/1) |> Enum.filter(&File.regular?/1)

      is_binary(paths_or_dir) and File.dir?(paths_or_dir) ->
        paths_or_dir
        |> Path.expand()
        |> File.ls!()
        |> Enum.map(&Path.join(paths_or_dir, &1))
        |> Enum.filter(&File.regular?/1)

      is_binary(paths_or_dir) and File.regular?(paths_or_dir) ->
        [Path.expand(paths_or_dir)]

      true -> []
    end
  end

  @csv_exts ~w(.csv .CSV)
  @json_exts ~w(.json .JSON)
  @log_exts ~w(.log .LOG)

  defp classify_by_extension(paths) do
    Enum.reduce(paths, {[], [], []}, fn path, {csvs, jsons, logs} ->
      ext = Path.extname(path)
      cond do
        ext in @csv_exts -> {[path | csvs], jsons, logs}
        ext in @json_exts -> {csvs, [path | jsons], logs}
        ext in @log_exts -> {csvs, jsons, [path | logs]}
        true -> {csvs, jsons, logs}
      end
    end)
    |> then(fn {c, j, l} -> {Enum.reverse(c), Enum.reverse(j), Enum.reverse(l)} end)
  end

  # CSV basic validation (header/columns)
  # NOTE (EN): RFC4180 header must have 6 columns
  defp valid_csv_rows?(rows) do
    case rows do
      [] -> false
      [header | _] -> length(header) == 6
    end
  end
end
