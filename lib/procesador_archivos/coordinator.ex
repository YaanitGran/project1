
defmodule ProcesadorArchivos.Coordinator do
  @moduledoc """
  Coordinator process that manages a pool of worker processes for files.
  It prints progress as 'Procesados X/Y' without filenames and aggregates results
  and errors to be returned to the caller.
  """

  alias ProcesadorArchivos.Worker

  require Logger

  @doc """
  Runs in the caller process by spawning a dedicated coordinator loop.
  Returns {:ok, results, errors, runtime_info}
  """
  def run(files, opts) do
    parent = self()
    _coord = spawn(fn -> loop(%{
      parent: parent,
      queue: files,
      in_flight: %{},
      processed: 0,
      total: length(files),
      results: [],
      errors: [],
      max_workers: Map.get(opts, :max_workers, System.schedulers_online()),
      start_ts: System.monotonic_time(),
      max_memory_mb: 0,
      process_count: 0,
      opts: opts
    }) end)

    receive do
      {:done, res, errs, runtime_info} ->
        {:ok, res, errs, runtime_info}
    after
      Map.get(opts, :timeout_ms, 5_000) * (length(files) + 1) ->
        # Hard stop fallback (shouldn't happen)
        {:ok, [], ["Timeout coordinador"], %{process_count: 0, max_memory_mb: 0}}
    end
  end

  defp loop(state) do
    state = maybe_spawn_workers(state)
    state = sample_runtime(state)

    receive do
      {:worker_result, pid, %{ok: ok, file: path, type: type, payload: payload}} ->
        Process.demonitor(state.in_flight[pid], [:flush])

        # progress
        processed = state.processed + 1
        total = state.total
        if Map.get(state.opts, :progress, true) do
          IO.puts("Procesados #{processed}/#{total}")
        end

        # aggregate

        {results, errors} =
          if ok do
            { [ {path, type, payload.metrics, payload} | state.results ] , state.errors ++ collect_errors(payload) }
          else
            err = format_error(path, type, payload)
            { state.results, state.errors ++ [err] }
          end


        new_state =
          state
          |> Map.put(:results, results)
          |> Map.put(:errors, errors)
          |> Map.put(:processed, processed)
          |> Map.update!(:in_flight, &Map.delete(&1, pid))

        finish_or_continue(new_state)

      {:DOWN, _ref, :process, pid, reason} ->
        # Worker crashed without sending result
        processed = state.processed + 1
        total = state.total
        if Map.get(state.opts, :progress, true) do
          IO.puts("Procesados #{processed}/#{total}")
        end

        errors = state.errors ++ ["Worker crash: #{inspect(reason)}"]
        new_state =
          state
          |> Map.put(:errors, errors)
          |> Map.put(:processed, processed)
          |> Map.update!(:in_flight, &Map.delete(&1, pid))

        finish_or_continue(new_state)
    after
      50 ->
        # small tick to keep spawning if capacity
        finish_or_continue(state)
    end
  end

  defp finish_or_continue(%{processed: p, total: t} = state) when p >= t do
    # final runtime info
    runtime_info = %{
      process_count: state.process_count + map_size(state.in_flight) + 1, # + coordinator itself
      max_memory_mb: state.max_memory_mb
    }
    send(state.parent, {:done, Enum.reverse(state.results), state.errors, runtime_info})
  end
  defp finish_or_continue(state), do: loop(maybe_spawn_workers(state))

  defp maybe_spawn_workers(%{queue: [], in_flight: in_flight, max_workers: max, opts: _opts} = s)
       when map_size(in_flight) >= max, do: s

  defp maybe_spawn_workers(%{queue: [], in_flight: _} = s), do: s

  defp maybe_spawn_workers(%{queue: [path | rest], in_flight: in_flight, max_workers: max} = s)
       when map_size(in_flight) < max do
    pid = Worker.spawn_worker(self(), path, s.opts)
    ref = Process.monitor(pid)
    in_flight = Map.put(in_flight, pid, ref)
    maybe_spawn_workers(%{s | queue: rest, in_flight: in_flight})
  end

  defp maybe_spawn_workers(s), do: s

  # Collect element/line errors from payloads (but don't print yet)
  defp collect_errors(%{row_errors: errs}) when is_list(errs), do: errs
  defp collect_errors(%{element_errors: errs}) when is_list(errs), do: errs
  defp collect_errors(%{line_errors: errs}) when is_list(errs), do: errs
  defp collect_errors(_), do: []

  defp format_error(path, type, payload) do
    kind = Map.get(payload, :kind, :unknown)
    detail =
      case payload do
        %{errors: errs} when is_list(errs) -> Enum.join(errs, " | ")
        other -> inspect(other)
      end

    "Archivo #{path} (#{type}): #{kind} -> #{detail}"
  end

  # Sample memory and processes (only app processes are counted approximately)
  defp sample_runtime(state) do
    # memory in MB
    mem_mb = :erlang.memory(:total) / (1024 * 1024)
    max_mb = max(state.max_memory_mb, mem_mb)
    # processes used by app (worker count + in-flight + coordinator)
    proc_count = map_size(state.in_flight) + 1
    state
    |> Map.put(:max_memory_mb, Float.round(max_mb, 2))
    |> Map.put(:process_count, proc_count)
  end
end
