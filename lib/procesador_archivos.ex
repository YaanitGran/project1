
defmodule ProcesadorArchivos do
  @moduledoc """
  Public API for processing directories or explicit file lists in sequential or
  parallel mode. It also provides a simple benchmark between both modes.

  All module/function names are in English as requested. Report and console
  messages appear in Spanish.
  """

  alias ProcesadorArchivos.{Discovery, Pipeline, Reporter}

  @type mode :: :sequential | :parallel

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
    files = Discovery.discover(dir)
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

    duration_ms = ms_since(start_ts)
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

    :ok = Reporter.write(text_report, opts.out)

    {:ok,
     %{
       results: results,
       errors: errors,
       duration_ms: duration_ms,
       out: opts.out
     }}
  end

  @doc """
  Benchmarks the same directory in sequential and parallel modes and produces
  the performance section in the final report.
  """
  def benchmark(dir, opts \\ %{}) when is_binary(dir) do
    opts = merge_defaults(opts)

    files = Discovery.discover(dir)

    # Sequential run
    t0 = System.monotonic_time()
    {:ok, _r_seq, _e_seq, runtime_seq} = Pipeline.run(files, %{opts | mode: :sequential})
    t_seq = ms_since(t0)

    # Parallel run
    t1 = System.monotonic_time()
    {:ok, _r_par, _e_par, runtime_par} = Pipeline.run(files, %{opts | mode: :parallel})
    t_par = ms_since(t1)

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

  defp merge_defaults(opts) do
    cfg = Application.get_env(:file_processor, __MODULE__, [])
    cfg_map = Map.new(cfg)
    Map.merge(cfg_map, Enum.into(opts, %{}))
    |> Map.update(:mode, :parallel, fn m -> m end)
  end

  defp ms_since(t0), do: System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)
end
