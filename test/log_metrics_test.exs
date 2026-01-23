
defmodule LOGMetricsTest do
  use ExUnit.Case, async: true

  alias PAI.LogReader
  alias PAI.LOGMetrics

  defp with_nl(s),
    do: s |> String.replace("\r\n", "\n") |> String.trim_trailing() |> then(&(&1 <> "\n"))

  defp write!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "log_metrics_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  test "per_file computes entries and avg_seconds_between_error (ERROR/FATAL)", %{dir: dir} do

    path =
      write!(Path.join(dir, "sistema.log"),
        with_nl("""
        2024-01-01 09:00:00 [INFO]  [App]  Started
        2024-01-01 09:01:00 [ERROR] [Auth] Failed
        2024-01-01 09:02:00 [FATAL] [DB]   Down
        """)
      )

    assert {:ok, entries, errs} = LogReader.read(path)
    assert errs == []   # <-- ya no debe marcar formato inválido
    m = LOGMetrics.per_file(path, entries, %{})
    avg = Map.get(m, :avg_seconds_between_error) || Map.get(m, :avg_seconds_between_fatal)
    assert avg == 60.0

    assert m.total_entries == 3
  end
end
