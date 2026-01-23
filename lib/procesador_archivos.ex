
defmodule ProcesadorArchivos do
  @moduledoc """
  Public API for processing directories or explicit file lists in sequential or
  parallel mode. It also provides a simple benchmark between both modes.

  All module/function names are in English as requested. Report and console
  messages appear in Spanish.
  """

  alias ProcesadorArchivos.{Classifier, Pipeline, Reporter}

  @type mode :: :sequential | :parallel

  # ------------------------------------------------------------
  # Defaults for options used across all public API functions.
  # ------------------------------------------------------------
  defp merge_defaults(opts) do
    defaults = %{
      mode: :parallel,
      max_workers: System.schedulers_online(),
      timeout_ms: 5_000,
      retries: 1,
      retry_delay_ms: 200,
      error_strategy: :mark_as_corrupt,
      progress: true,
      out: "output/reporte_final.txt",
      top_n_log_messages: 3
    }

    Map.merge(defaults, Map.new(opts))
  end

  # ------------------------------------------------------------
  # Process a directory (discover files) and generate a report.
  # ------------------------------------------------------------
  @doc """
  Processes all supported files in a directory.

  Options (all optional, defaults from config):
    * :mode - :sequential | :parallel (default :parallel)
    * :max_workers - integer
    * :timeout_ms - integer
    * :retries - integer
    * :retry_delay_ms - integer
    * :error_strategy - :skip | :mark_as_corrupt | :fail_fast
    * :progress - boolean
    * :out - output report path (string)
    * :top_n_log_messages - integer (Top N for LOG)

  Returns {:ok, %{results, errors, duration_ms, out}}.
  """
  def process_directory(dir, opts \\ %{}) when is_binary(dir) do
    opts = merge_defaults(opts)
    files = Classifier.discover(dir)
    process_files(files, Map.put(opts, :input_root, dir))
  end

  # ------------------------------------------------------------
  # Process an explicit list of files and generate a report.
  # ------------------------------------------------------------
  @doc """
  Processes an explicit list of files (mixed CSV/JSON/LOG).

  Returns {:ok, %{results, errors, duration_ms, out}}.
  """
  def process_files(files, opts \\ %{}) when is_list(files) do
    opts = merge_defaults(opts)
    start_ts = System.monotonic_time()

    {:ok, results, errors, runtime_info} = Pipeline.run(files, opts)

    duration_ms =
      System.convert_time_unit(System.monotonic_time() - start_ts, :native, :millisecond)

    # Build the final text report and write it to file (and/or stdout)
    text_report =
      Reporter.build_report(%{
        timestamp: DateTime.utc_now(),
        input_root: Map.get(opts, :input_root),
        mode: opts.mode,
        results: results,
        errors: errors,
        duration_ms: duration_ms,
        runtime: runtime_info,
        options: opts
      })

    out_path = Map.get(opts, :out, "output/reporte_final.txt")
    :ok = Reporter.write(text_report, out_path)

    {:ok,
     %{
       results: results,
       errors: errors,
       duration_ms: duration_ms,
       out: out_path
     }}
  end

  # ------------------------------------------------------------
  # Error inspection wrapper (NO report for single file).
  # ------------------------------------------------------------
  @doc """
  High–level error inspection function.

  * Accepts:
      - a single file
      - a directory
      - a list of files

  * Behavior:
      - If the input is a list or directory → reuse process_files/2 (NO custom inspection, NO individual report).
      - If the input is a single file     → return a detailed inspection map (NO report generation).
  """
  def procesar_con_manejo_errores(path_or_list, opts \\ %{}) do
    cond do
      # CASE 1: explicit list of files
      is_list(path_or_list) ->
        process_files(path_or_list, opts)

      # CASE 2: directory
      File.dir?(path_or_list) ->
        files = Classifier.discover(path_or_list)
        process_files(files, Map.put(opts, :input_root, path_or_list))

      # CASE 3: single file inspection
      File.regular?(path_or_list) ->
        inspect_single_file(path_or_list)

      # CASE 4: not valid
      true ->
        {:error, "Ruta inválida: #{path_or_list}"}
    end
  end

  # ----------------------------
  # Single–file inspection (NO reporter)
  # ----------------------------
  defp inspect_single_file(path) do
    type = Classifier.classify(path)

    case type do
      :csv  -> inspect_csv(path)
      :json -> inspect_json(path)
      :log  -> inspect_log(path)
      _     -> %{estado: :error, errores: ["Tipo de archivo no soportado: #{path}"]}
    end
  end

  # =============================
  # CSV inspection (single file)
  # =============================
  defp inspect_csv(path) do
    case ProcesadorArchivos.CSVReader.read(path) do
      {:ok, rows, row_errors} ->
        processed_lines = length(rows)
        error_lines     = length(row_errors)

        status = if error_lines == 0, do: :ok, else: :parcial

        parsed_errors =
          Enum.map(row_errors, fn msg ->
            case Regex.run(~r/^Línea\s+(\d+):\s+(.*)$/, msg) do
              [_, line, message] -> {String.to_integer(line), message}
              _ -> {:unknown, msg}
            end
          end)

        %{
          estado: status,
          lineas_procesadas: processed_lines,
          lineas_con_error: error_lines,
          errores: parsed_errors
        }

      _ ->
        %{estado: :error, errores: ["No se pudo leer #{path}"]}
    end
  end

  # =============================
  # JSON inspection (single file)
  # =============================
  defp inspect_json(path) do
    case ProcesadorArchivos.JSONReader.read(path) do
      # Structurally valid JSON (may have element-level errors)
      {:ok, _data, element_errors} ->
        parsed =
          Enum.map(element_errors, fn err ->
            {extract_json_index(err), err}
          end)

        status = if element_errors == [], do: :ok, else: :parcial

        %{
          estado: status,
          lineas_con_error: length(element_errors),
          errores: parsed
        }

      # Completely malformed JSON (DecodeError normalized to categories or messages)

      {:error, cats} ->
            human =
              Enum.map(cats, fn cat ->
                {:unknown, ProcesadorArchivos.JSONReader.humanize_category(cat)}
              end)

            %{estado: :parcial, lineas_con_error: length(human), errores: human}
        end
      end


  @doc false
  defp extract_json_index(err) do
    cond do
      Regex.match?(~r/\busuarios\[(\d+)\]/, err) ->
        [_, idx] = Regex.run(~r/\busuarios\[(\d+)\]/, err)
        {:usuarios, String.to_integer(idx)}

      Regex.match?(~r/\bsesiones\[(\d+)\]/, err) ->
        [_, idx] = Regex.run(~r/\bsesiones\[(\d+)\]/, err)
        {:sesiones, String.to_integer(idx)}

      true ->
        :unknown
    end
  end

  # =============================
  # LOG inspection (single file)
  # =============================
  defp inspect_log(path) do
    case ProcesadorArchivos.LogReader.read(path) do
      {:ok, entries, line_errors} ->
        status = if line_errors == [], do: :ok, else: :parcial

        parsed =
          Enum.map(line_errors, fn msg ->
            case Regex.run(~r/^Línea\s+(\d+):\s+(.*)$/, msg) do
              [_, line, message] -> {String.to_integer(line), message}
              _ -> {:unknown, msg}
            end
          end)

        %{
          estado: status,
          lineas_procesadas: length(entries),
          lineas_con_error: length(line_errors),
          errores: parsed
        }

      _ ->
        %{estado: :error, errores: ["No se pudo leer #{path}"]}
    end
  end

  # ------------------------------------------------------------
  # Benchmark (sequential vs parallel) - progress disabled here
  # ------------------------------------------------------------
  @doc """
  Benchmarks the same directory in sequential and parallel modes.
  Note: we force `progress: false` here to avoid console noise in timings.
  """
  def benchmark(dir, opts \\ %{}) when is_binary(dir) do
    opts  = merge_defaults(opts)
    files = Classifier.discover(dir)

    # Force silent runs for fair timing
    seq_opts = %{opts | mode: :sequential, progress: false}
    par_opts = %{opts | mode: :parallel,   progress: false}

    # Sequential run
    t0 = System.monotonic_time()
    {:ok, _r_seq, _e_seq, runtime_seq} = Pipeline.run(files, seq_opts)
    t_seq = System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)

    # Parallel run
    t1 = System.monotonic_time()
    {:ok, _r_par, _e_par, runtime_par} = Pipeline.run(files, par_opts)
    t_par = System.convert_time_unit(System.monotonic_time() - t1, :native, :millisecond)

    speedup = if t_par > 0, do: Float.round(t_seq / t_par, 2), else: :infinity

    {:ok,
     %{
       sequential: %{duration_ms: t_seq},
       parallel:   %{duration_ms: t_par},
       speedup:    speedup,
       processes_used: %{
         sequential: runtime_seq.process_count,
         parallel:   runtime_par.process_count
       },
       max_memory_mb: %{
         sequential: runtime_seq.max_memory_mb,
         parallel:   runtime_par.max_memory_mb
       }
     }}
  end

  # ------------------------------------------------------------
  # Spanish convenience wrappers (no report printed by default)
  # ------------------------------------------------------------
  @doc """
  Convenience wrapper for the document examples.

  It accepts `opciones = %{max_workers: ..., timeout: ...}`, mapping
  `timeout -> timeout_ms`. It runs the directory processing and returns :ok.
  """
  def procesar_con_opciones(dir, opciones \\ %{}) when is_binary(dir) and is_map(opciones) do
    opciones_norm =
      opciones
      |> Map.new()
      |> then(fn m ->
        case Map.pop(m, :timeout) do
          {nil, m2} -> m2
          {t, m2}   -> Map.put(m2, :timeout_ms, t)
        end
      end)

    _ = process_directory(dir, opciones_norm)
    :ok
  end

  @doc """
  Prints a short benchmark summary like in the document:
    Secuencial: <ms>
    Paralelo:   <ms>
    Mejora:     <x>x
  """
  def benchmark_paralelo_vs_secuencial(dir) when is_binary(dir) do
    {:ok, b} = benchmark(dir, %{progress: false})

    t_seq = b.sequential.duration_ms
    t_par = b.parallel.duration_ms
    sp    = b.speedup

    IO.puts("Secuencial: #{t_seq}ms")
    IO.puts("Paralelo:   #{t_par}ms")
    IO.puts("Mejora:     #{sp}x")

    :ok
  end
end
