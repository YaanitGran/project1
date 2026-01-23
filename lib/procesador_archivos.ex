
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


  # ---------- Argument & path validation helpers ----------

  # Ensures a directory path exists and is a directory.
  defp ensure_directory!(dir) when is_binary(dir) do
    cond do
      File.dir?(dir) ->
        :ok

      File.exists?(dir) ->
        raise ArgumentError, """
        La ruta no es un directorio: #{dir}

        Uso correcto de process_directory/2:
          ProcesadorArchivos.process_directory("./data", %{mode: :parallel})

        Sugerencia:
          - Si tienes una lista de archivos, usa process_files/2:
            ProcesadorArchivos.process_files(["a.csv","b.json"], %{mode: :parallel})
        """

      true ->
        raise ArgumentError, """
        Directorio no encontrado: #{dir}

        Verifica la ruta o crea el directorio.
        Ejemplo correcto:
          ProcesadorArchivos.process_directory("./data", %{mode: :parallel})
        """
    end
  end

  # Validates a files list: returns {valid_files, missing_paths, invalid_elems}
  defp validate_files_list(files) when is_list(files) do
    Enum.reduce(files, {[], [], []}, fn item, {ok, missing, invalid} ->
      cond do
        is_binary(item) and File.regular?(item) ->
          {[item | ok], missing, invalid}

        is_binary(item) and File.exists?(item) and not File.regular?(item) ->
          # It's a dir or something else; treat as invalid for process_files
          {ok, missing, [item | invalid]}

        is_binary(item) and not File.exists?(item) ->
          {ok, [item | missing], invalid}

        true ->
          # Non-string entries
          {ok, missing, [inspect(item) | invalid]}
      end
    end)
    |> then(fn {ok, miss, inv} -> {Enum.reverse(ok), Enum.reverse(miss), Enum.reverse(inv)} end)
  end

  # Examples (for error messages)
  defp usage_process_directory do
    """
    Uso correcto:
      ProcesadorArchivos.process_directory("./data", %{mode: :parallel})

    Sugerencia:
      - Si tienes una lista de archivos, usa process_files/2:
        ProcesadorArchivos.process_files(["a.csv","b.json"], %{mode: :parallel})
    """
  end

  defp usage_process_files do
    """
    Uso correcto:
      ProcesadorArchivos.process_files(["data/ventas.csv","data/usuarios.json"], %{mode: :parallel})

    Notas:
      - El primer argumento debe ser una lista de rutas (strings).
      - Cada ruta debe existir y ser archivo regular (no directorio).
    """
  end

  defp usage_procesar_con_manejo_errores do
    """
    Uso correcto:
      # Inspección de un archivo
      ProcesadorArchivos.procesar_con_manejo_errores("data/usuarios.json")

      # Directorio o lista de archivos (procesa y genera reporte)
      ProcesadorArchivos.procesar_con_manejo_errores("./data")
      ProcesadorArchivos.procesar_con_manejo_errores(["a.csv","b.json"])
    """
  end

  defp usage_benchmark do
    """
    Uso correcto:
      ProcesadorArchivos.benchmark("./data", %{progress: false, max_workers: 8})
    """
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

  # Function head with defaults (no guards here)
  def process_directory(dir, opts \\ %{})

  # Real clauses (no defaults here)
  def process_directory(dir, opts) when is_binary(dir) do
    # Validate directory and provide friendly suggestion if wrong
    ensure_directory!(dir)

    opts  = merge_defaults(opts)
    files = Classifier.discover(dir)
    process_files(files, Map.put(opts, :input_root, dir))
  end

  def process_directory(bad, _opts) do
    raise ArgumentError, """
    Argumentos inválidos para process_directory/2.

    Se esperaba:
      - dir: string (ruta de directorio)
      - opts: map (opcional)

    Recibido:
      #{inspect(bad)}

    #{usage_process_directory()}
    """
  end



  # ------------------------------------------------------------
  # Process an explicit list of files and generate a report.
  # ------------------------------------------------------------

  @doc """
  Processes an explicit list of files (mixed CSV/JSON/LOG) and generates a report.

  Returns {:ok, %{results, errors, duration_ms, out}}.

  Validations:
    * If `files` is not a list of strings → ArgumentError with usage example.
    * If some paths do not exist → they are reported as errors, processing continues for existing files.
    * If none of the files are valid regular files → ArgumentError with usage example.
  """
  # Function head with defaults
  def process_files(files, opts \\ %{})

  # Real clauses
  def process_files(files, opts) when is_list(files) do
    opts = merge_defaults(opts)

    {valid_files, missing, invalid} = validate_files_list(files)

    if invalid != [] do
      raise ArgumentError, """
      Argumentos inválidos en la lista (no string o no es archivo regular):
        #{Enum.map_join(invalid, "\n  - ", & &1) |> then(&if &1 == "", do: "(vacío)", else: "- " <> &1)}

      #{usage_process_files()}
      """
    end

    if valid_files == [] do
      raise ArgumentError, """
      No se encontraron archivos válidos en la lista.

      Faltantes:
        #{Enum.map_join(missing, "\n  - ", & &1) |> then(&if &1 == "", do: "(ninguno)", else: &1)}

      #{usage_process_files()}
      """
    end

    start_ts = System.monotonic_time()
    {:ok, results, errors_from_pipeline, runtime_info} = Pipeline.run(valid_files, opts)

    duration_ms =
      System.convert_time_unit(System.monotonic_time() - start_ts, :native, :millisecond)

    missing_errors = Enum.map(missing, fn path -> "#{path}: No se encontró el archivo" end)
    all_errors = errors_from_pipeline ++ missing_errors

    text_report =
      Reporter.build_report(%{
        timestamp: DateTime.utc_now,
        input_root: Map.get(opts, :input_root),
        mode: opts.mode,
        results: results,
        errors: all_errors,
        duration_ms: duration_ms,
        runtime: runtime_info,
        options: opts
      })

    out_path = Map.get(opts, :out, "output/reporte_final.txt")
    :ok = Reporter.write(text_report, out_path)

    {:ok, %{results: results, errors: all_errors, duration_ms: duration_ms, out: out_path}}
  end

  def process_files(bad, _opts) do
    raise ArgumentError, """
    Argumentos inválidos para process_files/2.

    Se esperaba:
      - files: lista de strings (rutas a archivos)
      - opts:  map (opcional)

    Recibido:
      #{inspect(bad)}

    #{usage_process_files()}
    """
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
      - If the input is a list or directory → reuse process_files/2 (report is generated).
      - If the input is a single file     → return a detailed inspection map (NO report).
  """

  def procesar_con_manejo_errores(path_or_list, opts \\ %{}) do
    cond do
      # CASE 1: explicit list → validate like process_files/2, generate report, return {:ok, map}
      is_list(path_or_list) ->
        {valid_files, missing, invalid} = validate_files_list(path_or_list)

        if invalid != [] do
          raise ArgumentError, """
          Argumentos inválidos en la lista de archivos (no string o no es archivo regular):
            #{Enum.map_join(invalid, "\n  - ", & &1) |> then(&if &1 == "", do: "(vacío)", else: "- " <> &1)}

          #{usage_procesar_con_manejo_errores()}
          """
        end

        if valid_files == [] do
          raise ArgumentError, """
          No se encontraron archivos válidos en la lista.

          Faltantes:
            #{Enum.map_join(missing, "\n  - ", & &1) |> then(&if &1 == "", do: "(ninguno)", else: &1)}

          #{usage_procesar_con_manejo_errores()}
          """
        end

        # Reutilizamos process_files/2 para generar el reporte y retornar {:ok, map}
        process_files(valid_files, opts)

      # CASE 2: directory → delega (ya valida ensure_directory!/1 adentro)
      File.dir?(path_or_list) ->
        process_directory(path_or_list, opts)

      # CASE 3: single file (exists & is regular file) → correr pipeline con 1 archivo, generar reporte, retornar {:ok, map}
      File.regular?(path_or_list) ->
        run_and_report_single(path_or_list, opts)

      # CASE 4: string pero no existe
      is_binary(path_or_list) and not File.exists?(path_or_list) ->
        raise ArgumentError, """
        Ruta no encontrada: #{path_or_list}

        #{usage_procesar_con_manejo_errores()}
        """

      # CASE 5: cualquier otro tipo de argumento
      true ->
        raise ArgumentError, """
        Argumentos inválidos para procesar_con_manejo_errores/1.

        Recibido:
          #{inspect(path_or_list)}

        #{usage_procesar_con_manejo_errores()}
        """
    end
  end

  # --------------------------------------------------------------------
  # Helper: run pipeline for a single file, build and write the report,
  #          and return {:ok, %{results, errors, duration_ms, out}}
  # --------------------------------------------------------------------
  defp run_and_report_single(path, opts) do
    opts      = merge_defaults(opts)
    start_ts  = System.monotonic_time()

    # Ejecutamos el pipeline con 1 archivo (respeta :mode, :progress, etc.)
    {:ok, results, errors, runtime_info} = Pipeline.run([path], opts)

    duration_ms =
      System.convert_time_unit(System.monotonic_time() - start_ts, :native, :millisecond)

    # Construimos el reporte estándar (igual que process_files/2)
    text_report =
      Reporter.build_report(%{
        timestamp: DateTime.utc_now(),
        input_root: path,             # para dejar claro que fue un file puntual
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
  # Benchmark (sequential vs parallel) - progress disabled here
  # ------------------------------------------------------------
  @doc """
  Benchmarks the same directory in sequential and parallel modes.
  Note: we force `progress: false` here to avoid console noise in timings.
  """

  # Function head with defaults
  def benchmark(dir, opts \\ %{})

  # Real clauses
  def benchmark(dir, opts) when is_binary(dir) do
    ensure_directory!(dir)
    opts  = merge_defaults(opts)
    files = Classifier.discover(dir)

    seq_opts = %{opts | mode: :sequential, progress: false}
    par_opts = %{opts | mode: :parallel,   progress: false}

    t0 = System.monotonic_time()
    {:ok, _r_seq, _e_seq, runtime_seq} = Pipeline.run(files, seq_opts)
    t_seq = System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)

    t1 = System.monotonic_time()
    {:ok, _r_par, _e_par, runtime_par} = Pipeline.run(files, par_opts)
    t_par = System.convert_time_unit(System.monotonic_time() - t1, :native, :millisecond)

    speedup = if t_par > 0, do: Float.round(t_seq / t_par, 2), else: :infinity

    {:ok,
     %{
       sequential: %{duration_ms: t_seq},
       parallel:   %{duration_ms: t_par},
       speedup:    speedup,
       processes_used: %{sequential: runtime_seq.process_count, parallel: runtime_par.process_count},
       max_memory_mb:  %{sequential: runtime_seq.max_memory_mb,  parallel: runtime_par.max_memory_mb}
     }}
  end

  def benchmark(bad, _opts) do
    raise ArgumentError, """
    Argumentos inválidos para benchmark/2.

    Se esperaba:
      - dir: string (ruta de directorio)
      - opts: map (opcional)

    Recibido:
      #{inspect(bad)}

    #{usage_benchmark()}
    """
  end


  # ------------------------------------------------------------
  # Spanish convenience wrappers (no report printed by default)
  # ------------------------------------------------------------

  @doc """
  Convenience wrapper for the document examples.

  Accepts `opciones = %{max_workers: ..., timeout: ...}` and maps `timeout -> timeout_ms`.
  Runs directory processing and returns :ok.

  Validations:
    * `dir` must be a binary path to an existing directory (suggest process_files/2 for lists).
    * `opciones` must be a map.
  """

  # Function head with defaults
  def procesar_con_opciones(dir, opciones \\ %{})

  # Real clauses
  def procesar_con_opciones(dir, opciones) when is_binary(dir) and is_map(opciones) do
    ensure_directory!(dir)

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

  def procesar_con_opciones(dir, bad_opts) when is_binary(dir) do
    raise ArgumentError, """
    Argumentos inválidos para procesar_con_opciones/2.

    Se esperaba:
      - dir: string (ruta de directorio)
      - opciones: map (opcional), ej. %{mode: :parallel, timeout: 10_000}

    Recibido `opciones`:
      #{inspect(bad_opts)}

    #{usage_process_directory()}
    """
  end

  def procesar_con_opciones(bad_dir, _opciones) do
    raise ArgumentError, """
    Argumentos inválidos para procesar_con_opciones/2.

    Se esperaba:
      - dir: string (ruta de directorio)
      - opciones: map (opcional)

    Recibido `dir`:
      #{inspect(bad_dir)}

    #{usage_process_directory()}
    """
  end


  @doc """
  Prints a short benchmark summary like in the document:
    Secuencial: <ms>
    Paralelo:   <ms>
    Mejora:     <x>x
  """
  def benchmark_paralelo_vs_secuencial(dir) when is_binary(dir) do
    ensure_directory!(dir)

    {:ok, b} = benchmark(dir, %{progress: false})

    t_seq = b.sequential.duration_ms
    t_par = b.parallel.duration_ms
    sp    = b.speedup

    IO.puts("Secuencial: " <> :erlang.float_to_binary(t_seq / 1000.0, decimals: 3) <> " s")
    IO.puts("Paralelo:   " <> :erlang.float_to_binary(t_par / 1000.0, decimals: 3) <> " s")
    IO.puts("Mejora:     #{sp}x")

    :ok
  end

  # Invalid `dir` (not a binary)
  def benchmark_paralelo_vs_secuencial(bad_dir) do
    raise ArgumentError, """
    Argumentos inválidos para benchmark_paralelo_vs_secuencial/1.

    Se esperaba:
      - dir: string (ruta de directorio)

    Recibido:
      #{inspect(bad_dir)}

    #{usage_benchmark()}
    """
  end

end
