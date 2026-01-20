
defmodule ProcesadorArchivos.CSVReader do
  @moduledoc """
  CSV reader (ventas) with strict validation and specific error messages.

  Expected header:
    fecha,producto,categoria,precio_unitario,cantidad,descuento

  * Uses NimbleCSV with comma separator (based on provided good sample).
  * Computes per-line validation errors with line numbers.
  """

  alias NimbleCSV.RFC4180, as: CSV

  @header ~w(fecha producto categoria precio_unitario cantidad descuento)

  @doc """
  Streams and parses the CSV file returning:
    {:ok, rows, errors} where
      rows: list of valid maps
      errors: list of error strings
  """
  def read(path) do
    stream = File.stream!(path)

    # Validate header
    [first_line | _] = stream |> Enum.take(1)

    header =
      first_line
      |> String.trim()
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)

    if header != @header do
      {:ok, [], ["Encabezado inválido en #{path}. Se esperaba: #{Enum.join(@header, ",")} y se recibió: #{Enum.join(header, ",")}"]}
    else
      rows_stream =
        File.stream!(path)
        |> Stream.drop(1)
        |> CSV.parse_stream()
        |> Stream.with_index(2) # start at line 2 due to header

      {valids, errors} =
        Enum.reduce(rows_stream, {[], []}, fn {cols, line_no}, {acc_rows, acc_errs} ->
          case to_row(cols, line_no) do
            {:ok, row} -> {[row | acc_rows], acc_errs}
            {:error, msg} -> {acc_rows, [msg | acc_errs]}
          end
        end)

      {:ok, Enum.reverse(valids), Enum.reverse(errors)}
    end
  end

  # Convert raw columns into a validated map
  defp to_row(cols, line_no) do
    case cols do
      [fecha, producto, categoria, precio_unitario, cantidad, descuento] ->
        with {:ok, date} <- parse_date(fecha, line_no),
             {:ok, price} <- parse_positive_float(precio_unitario, line_no, "precio_unitario"),
             {:ok, qty} <- parse_positive_int(cantidad, line_no, "cantidad"),
             {:ok, disc} <- parse_discount(descuento, line_no),
             :ok <- not_empty(producto, line_no, "producto"),
             :ok <- not_empty(categoria, line_no, "categoria") do
          {:ok,
           %{
             date: date,
             product: producto,
             category: categoria,
             unit_price: price,
             quantity: qty,
             discount_pct: disc
           }}
        else
          {:error, _} = e -> e
          :error -> {:error, error_msg(line_no, "Campos vacíos inválidos")}
        end

      other ->
        {:error, error_msg(line_no, "Número de columnas inválido (#{length(other)} != 6)")}
    end
  end

  defp parse_date(s, line), do:
    case Date.from_iso8601(s) do
      {:ok, d} -> {:ok, d}
      _ -> {:error, error_msg(line, "fecha '#{s}' inválida (YYYY-MM-DD)")}
    end

  defp parse_positive_float(s, line, field) do
    case Float.parse(s) do
      {v, ""} when v > 0.0 -> {:ok, v}
      {v, ""} -> {:error, error_msg(line, "#{field} debe ser > 0, recibido #{v}")}
      _ -> {:error, error_msg(line, "#{field} no es un número válido: '#{s}'")}
    end
  end

  defp parse_positive_int(s, line, field) do
    case Integer.parse(s) do
      {v, ""} when v > 0 -> {:ok, v}
      {v, ""} -> {:error, error_msg(line, "#{field} debe ser > 0, recibido #{v}")}
      _ -> {:error, error_msg(line, "#{field} no es un entero válido: '#{s}'")}
    end
  end

  defp parse_discount(s, line) do
    case Float.parse(s) do
      {v, ""} when v >= 0.0 and v <= 100.0 -> {:ok, v}
      {v, ""} -> {:error, error_msg(line, "descuento=#{v} fuera de rango [0,100]")}
      _ -> {:error, error_msg(line, "descuento no es un número válido: '#{s}'")}
    end
  end

  defp not_empty(s, _line, _field) when is_binary(s) and byte_size(String.trim(s)) > 0, do: :ok
  defp not_empty(_s, line, field), do: {:error, error_msg(line, "#{field} no puede estar vacío")}

  defp error_msg(line, msg), do: "Línea #{line}: #{msg}"
end
