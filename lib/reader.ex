defmodule ProcesadorArchivos.Reader do
  def read_csv(filename) do
    if File.exists?(filename) and File.regular?(filename) do
      File.stream!(filename)
      |> Stream.map(fn line ->
        # Replace the escape sequence \" with ''
        String.replace(line, "\"", "''")
      end)
      |> NimbleCSV.RFC4180.parse_stream()
      |> Enum.to_list()
    else
      [] # sends an empty list
    end
  end

  #this function parses json files
  def read_json(filename) do
    with {:ok, content} <- File.read(filename),
         {:ok, data}     <- Jason.decode(content) do
      usuarios = Map.get(data, "usuarios", [])
      sesiones = Map.get(data, "sesiones", [])
      {:ok, %{usuarios: usuarios, sesiones: sesiones}}
    else
      _ -> {:error, "JSON inválido o no legible: #{filename}"}
    end
  end



  @log_rx ~r/^(?<date>\d{4}-\d{2}-\d{2}) (?<time>\d{2}:\d{2}:\d{2}) \[(?<level>[A-Z]+)\] \[(?<comp>[^\]]+)\] (?<msg>.*)$/

  def read_log(filename) do
    case File.read(filename) do
      {:ok, content} ->
        lines = String.split(content, "\n", trim: true)

        entries =
          for line <- lines, reduce: [] do
            acc ->
              case Regex.named_captures(@log_rx, line) do
                %{"date" => d, "time" => t, "level" => lvl, "comp" => comp, "msg" => msg} ->
                  ts = NaiveDateTime.from_iso8601!("#{d} #{t}")
                  level = case lvl do
                    "DEBUG" -> :debug
                    "INFO"  -> :info
                    "WARN"  -> :warn
                    "ERROR" -> :error
                    "FATAL" -> :fatal
                    _       -> :info
                  end

                  [%{ts: ts, level: level, component: comp, msg: msg} | acc]

                _ ->
                  acc  # skip lines that do not match (Delivery 1: without advanced handling)
              end
          end
          |> Enum.reverse()

        {:ok, entries}

      {:error, reason} ->
        {:error, "Error al leer el archivo: #{reason}"}
    end
  end
end
