
defmodule ProcesadorArchivos.Pipeline do
  @moduledoc """
  High-level orchestration to run sequential or parallel flows and gather
  results and errors for reporting.
  """

  alias ProcesadorArchivos.Classifier
  alias ProcesadorArchivos.Coordinator
  alias ProcesadorArchivos.{CSVReader, JSONReader, LogReader}
  alias ProcesadorArchivos.{CSVMetrics, JSONMetrics, LOGMetrics}

  @doc """
  Runs the selected mode over the list of files. Returns:
    {:ok, results, errors, runtime_info}

  * results: list of {path, type, per_file_metrics, payload}
  * errors: collected error messages
  """
  def run(files, %{mode: :sequential} = opts) do
    start_ts = System.monotonic_time()

    {results, errors} =
      Enum.reduce(files, {[], []}, fn path, {acc_r, acc_e} ->
        type = Classifier.classify(path)
        {res, errs} =
          case type do
            :csv ->
              case CSVReader.read(path) do
                {:ok, rows, row_errors} ->
                  metrics = CSVMetrics.per_file(path, rows)
                  {{path, type, metrics, %{metrics: metrics, row_errors: row_errors}}, row_errors}
                {:error, errs} -> {nil, errs}
              end

            :json ->
              case JSONReader.read(path) do
                {:ok, %{usuarios: users, sesiones: sess}, element_errors} ->
                  metrics = JSONMetrics.per_file(path, users, sess)
                  {{path, type, metrics, %{metrics: metrics, element_errors: element_errors}}, element_errors}
                {:error, errs} -> {nil, errs}
              end

            :log ->
              case LogReader.read(path) do
                {:ok, entries, errs} ->
                  metrics = LOGMetrics.per_file(path, entries, opts)
                  {{path, type, metrics, %{metrics: metrics, line_errors: errs}}, errs}
                {:error, errs} -> {nil, errs}
              end

            :unknown ->
              {nil, ["Extensión no soportada: #{path}"]}
          end

        acc_r = if res, do: [res | acc_r], else: acc_r
        acc_e = acc_e ++ errs

        # progress (only count)
        if Map.get(opts, :progress, true) do
          processed = length(acc_r) + count_failed(acc_e)
          IO.puts("Procesados #{processed}/#{length(files)}")
        end

        {acc_r, acc_e}
      end)

    _duration_ms = System.convert_time_unit(System.monotonic_time() - start_ts, :native, :millisecond)

    runtime_info = %{
      process_count: 1,
      max_memory_mb: Float.round(:erlang.memory(:total) / (1024 * 1024), 2)
    }

    {:ok, Enum.reverse(results), errors, runtime_info}
  end

  def run(files, %{mode: :parallel} = opts) do
    Coordinator.run(files, opts)
  end

  defp count_failed(_errs), do: 0
end
