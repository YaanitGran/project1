
defmodule ProcesadorArchivos.Pipeline do
  @moduledoc """
  Orquestación de ejecución:
    - :sequential   → procesamiento en un solo proceso
    - :parallel     → procesamiento paralelo usando Task.async_stream/3 (sin rehacer arquitectura)

  Retorna siempre:
    {:ok, results, errors, runtime_info}

  * results: lista de {path, type, per_file_metrics, payload}
  * errors : lista de strings (mensajes de error acumulados)
  """

  alias ProcesadorArchivos.Classifier
  alias ProcesadorArchivos.{CSVReader, JSONReader, LogReader}
  alias ProcesadorArchivos.{CSVMetrics, JSONMetrics, LOGMetrics}

  # ===============================
  # sequential mode
  # ===============================
  @doc """
  Ejecuta el flujo en modo indicado en opts. Para :sequential reduce de a 1.
  """
  def run(files, %{mode: :sequential} = opts) do
    start_ts = System.monotonic_time()
    total = length(files)
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
  # Parallel mode (Task.async_stream)
  # ===============================
  def run(files, %{mode: :parallel} = opts) do
    start_ts = System.monotonic_time()

    max_conc = Map.get(opts, :max_workers, System.schedulers_online())
    timeout  = Map.get(opts, :timeout_ms, 5_000)
    show_prog = Map.get(opts, :progress, true)
    total = length(files)

    work = fn path ->

      case do_one_file(path, opts) do
        {:ok, tuple}     -> {:ok, tuple}
        {:error, errors} -> {:error, errors}
      end
    end

    stream =
      Task.async_stream(files, work,
        ordered: false,
        max_concurrency: max_conc,
        timeout: timeout
      )

    {results, errors, _k} =
      Enum.reduce(stream, {[], [], 0}, fn
        {:ok, {:ok, tuple}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          {[tuple | acc_r], acc_e, i + 1}

        {:ok, {:error, errs}}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          {acc_r, acc_e ++ errs, i + 1}

        {:exit, reason}, {acc_r, acc_e, i} ->
          if show_prog, do: IO.puts("Procesados #{i + 1}/#{total}")
          msg = "Task crash: #{inspect(reason)}"
          {acc_r, [msg | acc_e], i + 1}
      end)

    {:ok, Enum.reverse(results), errors, runtime_info(start_ts, max_conc)}
  end

  # Returns:
  #   {:ok, {path, type, metrics, payload}}
  #   {:error, [msg1, msg2, ...]}
  defp do_one_file(path, opts) do
    type = Classifier.classify(path)

    case type do
      :csv ->
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
            metrics = JSONMetrics.per_file(path, users, sess)
            {:ok, {path, :json, metrics, %{metrics: metrics, element_errors: element_errors}}}

          {:error, errs} ->
            {:error, ["Archivo #{path} (json): json_parse -> " <> Enum.join(errs, " | ")]}
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

  defp runtime_info(start_ts, proc_count_guess) do
    %{
      process_count: proc_count_guess,
      max_memory_mb: Float.round(:erlang.memory(:total) / (1024 * 1024), 2),
      duration_ms: elapsed_ms(start_ts)
    }
  end

  defp elapsed_ms(t0),
    do: System.convert_time_unit(System.monotonic_time() - t0, :native, :millisecond)
end
