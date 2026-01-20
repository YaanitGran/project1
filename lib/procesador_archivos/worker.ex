
defmodule ProcesadorArchivos.Worker do
  @moduledoc """
  Worker process that handles a single file with timeout and retries.
  It sends {:worker_result, self(), %{ok: boolean, file: path, type: type, result_or_error: ...}}
  back to the coordinator pid provided.
  """

  alias ProcesadorArchivos.Classsifier
  alias ProcesadorArchivos.{CSVReader, JSONReader, LogReader}
  alias ProcesadorArchivos.{CSVMetrics, JSONMetrics, LOGMetrics}

  require Logger

  @doc """
  Spawns the worker. Options include:
    :timeout_ms, :retries, :retry_delay_ms, :error_strategy, :top_n_log_messages
  """
  def spawn_worker(coord_pid, path, opts) do
    spawn(fn -> run(coord_pid, path, opts) end)
  end

  defp run(coord_pid, path, opts) do
    type = Classsifier.classify(path)
    {ok, payload} = attempt_with_retries(fn -> do_process(type, path, opts) end, opts)
    send(coord_pid, {:worker_result, self(), %{ok: ok, file: path, type: type, payload: payload}})
  end

  defp do_process(:csv, path, _opts) do
    case CSVReader.read(path) do
      {:ok, rows, row_errors} ->
        metrics = CSVMetrics.per_file(path, rows)
        {:ok, %{metrics: metrics, row_errors: row_errors}}

      {:error, errs} ->
        {:error, %{kind: :csv_parse, errors: errs}}
    end
  end

  defp do_process(:json, path, _opts) do
    case JSONReader.read(path) do
      {:ok, %{usuarios: users, sesiones: sess}, element_errors} ->
        metrics = FileProcessor.Metrics.JSONMetrics.per_file(path, users, sess)
        {:ok, %{metrics: metrics, element_errors: element_errors}}

      {:error, errs} ->
        {:error, %{kind: :json_parse, errors: errs}}
    end
  end

  defp do_process(:log, path, opts) do
    case LogReader.read(path) do
      {:ok, entries, errs} ->
        metrics = LOGMetrics.per_file(path, entries, opts)
        {:ok, %{metrics: metrics, line_errors: errs}}

      {:error, errs} ->
        {:error, %{kind: :log_parse, errors: errs}}
    end
  end

  defp do_process(:unknown, path, _opts), do: {:error, %{kind: :unsupported, errors: ["Extensión no soportada: #{path}"]}}

  # Retries + timeout wrapper
  defp attempt_with_retries(fun, opts) do
    retries = Map.get(opts, :retries, 1)
    delay = Map.get(opts, :retry_delay_ms, 200)
    timeout_ms = Map.get(opts, :timeout_ms, 5_000)

    attempt(fun, timeout_ms, retries, delay)
  end

  defp attempt(fun, timeout_ms, retries_left, delay) do
    caller = self()

    task_pid =
      spawn(fn ->
        # run and send back
        send(caller, {:attempt_result, self(), fun.()})
      end)

    ref = Process.monitor(task_pid)

    receive do
      {:attempt_result, ^task_pid, {:ok, res}} ->
        Process.demonitor(ref, [:flush])
        {true, res}

      {:attempt_result, ^task_pid, {:error, err}} ->
        Process.demonitor(ref, [:flush])
        maybe_retry({:error, err}, fun, timeout_ms, retries_left, delay)

      {:DOWN, ^ref, :process, ^task_pid, reason} ->
        maybe_retry({:error, %{kind: :crash, reason: reason}}, fun, timeout_ms, retries_left, delay)
    after
      timeout_ms ->
        Process.exit(task_pid, :kill)
        maybe_retry({:error, %{kind: :timeout, ms: timeout_ms}}, fun, timeout_ms, retries_left, delay)
    end
  end

  defp maybe_retry({:error, err}, fun, timeout_ms, retries_left, delay) do
    if retries_left > 0 do
      :timer.sleep(delay)
      attempt(fun, timeout_ms, retries_left - 1, delay)
    else
      {false, err}
    end
  end
end
