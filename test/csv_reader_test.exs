
defmodule CSVReaderTest do
  use ExUnit.Case, async: true

  alias PAI.CSVReader

  # Normalize CRLF to LF and ensure trailing \n
  defp with_nl(s),
    do: s |> String.replace("\r\n", "\n") |> String.trim_trailing() |> then(&(&1 <> "\n"))

  defp write!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "csv_reader_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  test "reads a valid CSV and returns rows with no errors", %{dir: dir} do
    path =
      write!(Path.join(dir, "ok.csv"),
        with_nl("""
        fecha,producto,categoria,precio_unitario,cantidad,descuento
        2024-01-01,Teclado,Periféricos,250.50,2,10
        2024-01-02,Mouse,Periféricos,120.00,1,0
        """)
      )
      assert {:ok, rows, errs} = CSVReader.read(path)
      assert length(rows) >= 1
      assert errs == []

    # Basic shape
    assert Enum.all?(rows, fn r ->
             Map.has_key?(r, :date) and Map.has_key?(r, :product) and
               Map.has_key?(r, :category) and Map.has_key?(r, :unit_price) and
               Map.has_key?(r, :quantity) and Map.has_key?(r, :discount_pct)
           end)
  end

  test "invalid header produces file-level error (no rows)", %{dir: dir} do
    path =
      write!(Path.join(dir, "bad_header.csv"),
        with_nl("""
        fecha,producto,categoria,precio,cantidad,descuento
        2024-01-01,Teclado,Periféricos,250.50,2,10
        """)
      )

    assert {:ok, rows, errs} = CSVReader.read(path)
    assert rows == []
    assert Enum.any?(errs, &String.contains?(&1, "Encabezado inválido"))
  end




end
