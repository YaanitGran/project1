
defmodule ProcesadorArchivos.LOGMetrics do
  @moduledoc """
  Computes metrics for LOG files per file, including distribution by level,
  top components with errors, hourly distribution, time deltas between FATAL,
  and top N repeated messages.
  """

  @critical_levels ~w(FATAL)

  @doc """
  Returns a per-file metrics map from parsed log entries.

  Options:
    * :top_n_log_messages - integer (defaults to 3)
  """
  def per_file(path, entries, opts \\ %{}) do
    top_n = Map.get(opts, :top_n_log_messages, 3)

    total = length(entries)

    by_level =
      entries
      |> Enum.group_by(& &1.level)
      |> Enum.map(fn {lvl, es} -> {lvl, length(es)} end)
      |> Map.new()

    # Percentages over valid lines
    pct =
      for {lvl, cnt} <- by_level, into: %{} do
        {lvl, percent(cnt, total)}
      end

    # Components with most errors (ERROR + FATAL)
    error_components =
      entries
      |> Enum.filter(&(&1.level in ["ERROR", "FATAL"]))
      |> Enum.group_by(& &1.component)
      |> Enum.map(fn {comp, es} -> {comp, length(es)} end)
      |> Enum.sort_by(fn {comp, cnt} -> {-cnt, comp} end)

    top_error_component = if error_components == [], do: nil, else: hd(error_components)

    # Hourly distribution
    hourly =
      entries
      |> Enum.group_by(fn e -> e.dt.hour end)
      |> Enum.map(fn {h, es} -> {h, length(es)} end)
      |> Enum.sort_by(&elem(&1, 0))

    # Time between critical (FATAL) errors
    critical =
      entries
      |> Enum.filter(&(&1.level in @critical_levels))
      |> Enum.sort_by(& &1.dt, DateTime)

    avg_delta_sec =
      case critical do
        [] -> nil
        [_one] -> nil
        list ->
          deltas =
            list
            |> Enum.chunk_every(2, 1, :discard)
            |> Enum.map(fn [a, b] -> DateTime.diff(b.dt, a.dt, :second) end)

          Float.round(Enum.sum(deltas) / length(deltas), 2)
      end

    # Top N exact messages
    top_messages =
      entries
      |> Enum.map(& &1.message)
      |> Enum.frequencies()
      |> Enum.sort_by(fn {msg, cnt} -> {-cnt, msg} end)
      |> Enum.take(top_n)

    %{
      file: path,
      total_entries: total,
      by_level: by_level,
      by_level_pct: pct,
      top_error_component: top_error_component,
      hourly_distribution: hourly,
      avg_seconds_between_fatal: avg_delta_sec,
      top_messages: top_messages
    }
  end

  defp percent(cnt, total) when total > 0 do
    Float.round(cnt * 100.0 / total, 1)
  end
  defp percent(_cnt, _total), do: 0.0
end
