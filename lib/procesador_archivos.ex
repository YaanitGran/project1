
defmodule ProcesadorArchivos do
  @moduledoc """
  Public API for processing directories or explicit file lists in sequential or
  parallel mode. It also provides a simple benchmark between both modes.

  All module/function names are in English as requested. Report and console
  messages appear in Spanish.
  """

  alias ProcesadorArchivos.{Classifier, Pipeline, Reporter}

  @type mode :: :sequential | :parallel


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

  Returns {:ok, consolidated_metrics_map}
  """
  def process_directory(dir, opts \\ %{}) when is_binary(dir) do
    opts = merge_defaults(opts)
    files = Classifier.discover(dir)
    process_files(files, Map.put(opts, :input_root, dir))
  end

  @doc """
  Processes an explicit list of files (mixed CSV/JSON/LOG).

  See `process_directory/2` for options.
  """
  def process_files(files, opts \\ %{}) when is_list(files) do
    opts = merge_defaults(opts)
    start_ts = System.monotonic_time()

    {:ok, results, errors, runtime_info} = Pipeline.run(files, opts)

    duration_ms =   System.convert_time_unit(System.monotonic_time() - start_ts, :native, :millisecond)
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

  @doc """
  Benchmarks the same directory in sequential and parallel modes and produces
  the performance section in the final report.
  """
  def benchmark(dir, opts \\ %{}) when is_binary(dir) do
    opts = merge_defaults(opts)

    files = Classifier.discover(dir)

    seq_opts = %{opts | mode: :sequential, progress: false}
    par_opts = %{opts | mode: :parallel,   progress: false}

    # Sequential run
    t0 = System.monotonic_time()
    {:ok, _r_seq, _e_seq, runtime_seq} = Pipeline.run(files, %{opts | mode: :sequential})
    t_seq = System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)


    # Parallel run
    t1 = System.monotonic_time()
    {:ok, _r_par, _e_par, runtime_par} = Pipeline.run(files, %{opts | mode: :parallel})
    t_par = System.convert_time_unit(System.monotonic_time() - t1, :native, :millisecond)


    speedup = if t_par > 0, do: Float.round(t_seq / t_par, 2), else: :infinity

    {:ok,
     %{
       sequential: %{duration_ms: t_seq},
       parallel: %{duration_ms: t_par},
       speedup: speedup,
       processes_used: %{sequential: runtime_seq.process_count, parallel: runtime_par.process_count},
       max_memory_mb: %{sequential: runtime_seq.max_memory_mb, parallel: runtime_par.max_memory_mb}
     }}
  end


  def procesar_con_opciones(dir, opciones \\ %{}) when is_binary(dir) and is_map(opciones) do
      # timeout -> timeout_ms
      opciones_norm =
        opciones
        |> Map.new()
        |> then(fn m ->
          case Map.pop(m, :timeout) do
            {nil, m2} -> m2
            {t, m2} -> Map.put(m2, :timeout_ms, t)
          end
        end)

      # Calls API
      _ = process_directory(dir, opciones_norm)
      :ok
    end


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
