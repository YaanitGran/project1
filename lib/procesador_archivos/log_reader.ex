
defmodule PAI.LogReader do
  @moduledoc """
  LOG reader with strict pattern:
    YYYY-MM-DD HH:MM:SS [LEVEL] [COMPONENT] Message

  Levels: DEBUG, INFO, WARN, ERROR, FATAL
  """

  @regex ~r/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+\[(DEBUG|INFO|WARN|ERROR|FATAL)\]\s+\[([^\]]+)\]\s+(.+)$/

  @doc """
  Returns {:ok, entries, errors} where entries are maps:
    %{dt: DateTime, level: "INFO", component: "API", message: "Text"}
  """
  def read(path) do
    stream = File.stream!(path) |> Stream.with_index(1)

    {entries, errors} =
      Enum.reduce(stream, {[], []}, fn {line, idx}, {acc, errs} ->
        case Regex.run(@regex, String.trim_trailing(line)) do
          [_, d, t, lvl, comp, msg] ->
            case parse_dt("#{d} #{t}") do
              {:ok, dt} ->
                e = %{dt: dt, level: lvl, component: comp, message: msg}
                {[e | acc], errs}

              {:error, m} ->
                {acc, [err(idx, m) | errs]}
            end

          _ ->
            {acc, [err(idx, "formato inválido (YYYY-MM-DD HH:MM:SS [NIVEL] [COMPONENTE] Mensaje)")] }
        end
      end)

    {:ok, Enum.reverse(entries), Enum.reverse(errors)}
  end

  defp parse_dt(s) do
    case NaiveDateTime.from_iso8601(s) do
      {:ok, ndt} -> {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
      _ -> {:error, "timestamp inválido"}
    end
  end

  defp err(idx, msg), do: "Línea #{idx}: #{msg}"
end
