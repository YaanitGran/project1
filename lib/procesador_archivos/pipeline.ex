
defmodule PAI.Pipeline do
  @moduledoc """
  Orchestrates the execution in two modes:

    * :sequential  - single-process reduction
    * :parallel    - Task.async_stream/3 with per-file isolation and timeout handling

  Always returns:
    {:ok, results, errors, runtime_info}

  Where:
    * results :: [{path, type, per_file_metrics, payload}]
    * errors  :: [string]
  """

  alias PAI.Classifier
  alias PAI.{CSVReader, JSONReader, LogReader}
  alias PAI.{CSVMetrics, JSONMetrics, LOGMetrics}

  # ===============================
  # SEQUENTIAL MODE
  # ===============================

  @doc """
  Runs the pipeline in sequential mode (one file at a time).
  Prints 'Procesados X/Y' if `opts.progress` is true.
  """
  def run(files, %{mode: :sequential} = opts) do
    start_ts = System.monotonic_time()
    total     = length(files)
    show_prog = Map.get(opts, :progress, true)

    {results, errors, _k} =
      Enum.reduce(files, {[], [], 0}, fn path, {acc_r, acc_e, i} ->
        case do_one_file(path, opts) do
          {:ok, tuple} ->
            if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
            {[tuple | acc_r], acc_e, i + 1}

          {:error, errs} ->
            if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
            {acc_r, acc_e ++ errs, i + 1}
        end
      end)

    {:ok, Enum.reverse(results), errors, runtime_info(start_ts, 1)}
  end

  # ===============================
  # PARALLEL MODE (Task.async_stream)
  # ===============================


  def run(files_param, %{mode: :parallel} = opts) do
    # Keep a local 'files' variable to avoid scope mix-ups when copying patches.
    files     = files_param
    _start_ts  = System.monotonic_time()
    max_conc  = Map.get(opts, :max_workers, System.schedulers_online())
    timeout   = Map.get(opts, :timeout_ms, 5_000)
    show_prog = Map.get(opts, :progress, true)
    total     = length(files)

    # Worker function: per-file logic using the same policy as sequential.
    work = fn path ->
      case do_one_file(path, opts) do
        {:ok, tuple}     -> {:ok, tuple}
        {:error, errors} -> {:error, errors}
      end
    end

    stream =
      Task.async_stream(files, work,
        ordered: true,                 # keep input order to zip with 'files'
        max_concurrency: max_conc,
        timeout: timeout,
        on_timeout: :kill_task         # do not abort the whole stream on timeouts
      )

    {results, errors, _k} =
      files
      |> Enum.zip(stream)              # pair {path, result}
      |> Enum.reduce({[], [], 0}, fn
        {_path, {:ok, {:ok, tuple}}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          {[tuple | acc_r], acc_e, i + 1}

        {_path, {:ok, {:error, errs}}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          {acc_r, acc_e ++ errs, i + 1}

        {path, {:exit, :timeout}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          msg = "#{path}: Tiempo de espera excedido (timeout)"
          {acc_r, [msg | acc_e], i + 1}

        {path, {:exit, reason}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          msg = "#{path}: Task crash: #{inspect(reason)}"
          {acc_r, [msg | acc_e], i + 1}
      end)

    runtime_info = %{
      process_count: max_conc,
      max_memory_mb: Float.round(:erlang.memory(:total) / (1024 * 1024), 2)
    }

    {:ok, Enum.reverse(results), errors, runtime_info}
  end

  # ===============================
  # PER-FILE LOGIC (SHARED)
  # ===============================

  @doc false
  # Returns:
  #   {:ok, {path, type, metrics, payload}}
  #   {:error, [msg1, msg2, ...]}
  defp do_one_file(path, opts) do
    type = Classifier.classify(path)

    case type do
      :csv ->
        # Policy: if there is at least ONE corrupt row, the whole file is considered error (no metrics).
        case CSVReader.read(path) do
          {:ok, rows, row_errors} ->
            if row_errors != [] do
              {:error,
               [
                 "Archivo #{path} (csv): csv_has_corrupt_lines -> " <>
                   Enum.join(row_errors, " | ")
               ]}
            else
              metrics = CSVMetrics.per_file(path, rows)
              {:ok, {path, :csv, metrics, %{metrics: metrics, row_errors: []}}}
            end
        end


      :json ->
        case JSONReader.read(path) do
          {:ok, %{usuarios: users, sesiones: sess}, element_errors} ->
            # Structurally valid JSON → compute categories from element-level errors (if any)
            categories = JSONReader.categorize_errors(element_errors)
            metrics    = JSONMetrics.per_file(path, users, sess)

            {:ok, {path, :json, metrics,
              %{
                metrics: metrics,
                element_errors: element_errors,
                json_categories: categories  # keep categories in payload (useful for inspection/report)
              }
            }}

          {:error, categories} ->
            # Malformed JSON → convert categories to friendly messages
            msgs =
              Enum.map(categories, fn cat ->
                "Archivo #{path} (json): JSON mal formateado con: #{JSONReader.humanize_category(cat)}"
              end)

            {:error, msgs}
        end


      :log ->
        case LogReader.read(path) do
          {:ok, entries, line_errors} ->
            metrics = LOGMetrics.per_file(path, entries, opts)
            {:ok, {path, :log, metrics, %{metrics: metrics, line_errors: line_errors}}}
        end

      :unknown ->
        {:error, ["Extensión no soportada: #{path}"]}
    end
  end

  # ===============================
  # RUNTIME HELPERS
  # ===============================

  @doc false
  defp runtime_info(start_ts, proc_count_guess) do
    %{
      process_count: proc_count_guess,
      max_memory_mb: Float.round(:erlang.memory(:total) / (1024 * 1024), 2),
      duration_ms: elapsed_ms(start_ts)
    }
  end

  @doc false
  defp elapsed_ms(t0),
    do: System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)
end
