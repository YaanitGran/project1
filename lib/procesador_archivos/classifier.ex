
defmodule PAI.Classifier do
  @moduledoc """
  Discovers and classifies supported files by extension.
  """

  @supported ~w(.csv .json .log)

  @doc """
  Returns a list of absolute or relative file paths found under `dir`
  filtered by supported extensions (case-insensitive).
  """
  def discover(dir) do
    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&File.regular?/1)
    |> Enum.filter(fn p -> ext_supported?(p) end)
  end

  @doc """
  Returns :csv | :json | :log | :unknown for a given path by extension.
  """
  def classify(path) do
    ext =
      path
      |> Path.extname()
      |> String.downcase()

    case ext do
      ".csv" -> :csv
      ".json" -> :json
      ".log" -> :log
      _ -> :unknown
    end
  end

  defp ext_supported?(path) do
    path
    |> Path.extname()
    |> String.downcase()
    |> then(&(&1 in @supported))
  end
end
