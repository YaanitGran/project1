
defmodule ProcesadorArchivos.JSONMetrics do
  @moduledoc """
  Computes metrics for JSON user/session data per file.
  No consolidation requested for JSON.
  """

  @doc """
  Given users and sessions (validated), returns per-file metrics map.
  """
  def per_file(path, users, sessions) do
    total_users = length(users)
    {active, inactive} = Enum.split_with(users, & &1.activo)
    total_pages = Enum.reduce(sessions, 0, fn s, acc -> acc + s.paginas_visitadas end)

    avg_session_sec =
      case sessions do
        [] -> 0.0
        _ -> sessions |> Enum.map(& &1.duracion_segundos) |> then(&Enum.sum(&1) / length(&1))
      end

    # Aggregate actions for top N
    actions_freq =
      sessions
      |> Enum.flat_map(& &1.acciones)
      |> Enum.frequencies()

    top_actions =
      actions_freq
      |> Enum.sort_by(fn {act, cnt} -> {-cnt, act} end)
      |> Enum.take(5)

    # Peak hour (0..23) from 'inicio'
    hours =
      sessions
      |> Enum.map(fn s ->
        {:ok, dt, _} = DateTime.from_iso8601(s.inicio)
        dt.hour
      end)

    peak_hour =
      case hours do
        [] -> nil
        _ ->
          hours
          |> Enum.frequencies()
          |> Enum.sort_by(fn {h, cnt} -> {-cnt, h} end)
          |> hd()
          |> elem(0)
      end

    %{
      file: path,
      total_users: total_users,
      active_users: length(active),
      inactive_users: length(inactive),
      avg_session_minutes: Float.round(avg_session_sec / 60.0, 2),
      total_pages: total_pages,
      top_actions: top_actions,
      peak_hour: peak_hour
    }
  end
end
