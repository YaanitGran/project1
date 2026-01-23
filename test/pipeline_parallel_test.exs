
defmodule PipelineParallelTest do
  use ExUnit.Case, async: false

  alias PAI.Pipeline

  defp with_nl(s),
    do: s |> String.replace("\r\n", "\n") |> String.trim_trailing() |> then(&(&1 <> "\n"))

  defp write!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "pipeline_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    ok_csv =
      write!(Path.join(dir, "ventas_ok.csv"),
        with_nl("""
        fecha,producto,categoria,precio_unitario,cantidad,descuento
        2024-01-01,Teclado,Periféricos,250.50,2,10
        """)
      )

      bad_csv =
        write!(Path.join(dir, "ventas_bad.csv"),
          with_nl("""
          fecha,producto,categoria,precio,cantidad,descuento
          2024-01-01,Teclado,Periféricos,250.50,2,10
          """)
        )

    ok_json =
      write!(Path.join(dir, "usuarios.json"),
        """
        {
          "usuarios": [
            { "id": 1, "nombre": "Ana", "email": "ana@test.com", "activo": true, "ultimo_acceso": "2024-01-01T10:00:00Z" }
          ],
          "sesiones": [
            { "usuario_id": 1, "inicio": "2024-01-01T09:00:00Z", "duracion_segundos": 300, "paginas_visitadas": 2, "acciones": ["login","logout"] }
          ]
        }
        """
      )

    ok_log =
      write!(Path.join(dir, "sistema.log"),
        with_nl("""
        2024-01-01T09:00:00Z INFO App Started
        2024-01-01T09:01:00Z ERROR Auth Failed
        """)
      )

    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, dir: dir, files: [ok_csv, bad_csv, ok_json, ok_log]}
  end

  test "sequential and parallel return expected shapes and errors", %{files: files} do
    {:ok, res_s, err_s, _rt_s} = Pipeline.run(files, %{mode: :sequential, progress: false})
    {:ok, res_p, err_p, _rt_p} = Pipeline.run(files, %{mode: :parallel, progress: false})

    # At least the OK files should appear in results in both runs
    assert length(res_s) >= 2
    assert length(res_p) >= 2

    # bad CSV should produce errors in both runs
    assert Enum.any?(err_s, &String.contains?(&1, "csv_has_corrupt_lines"))
    assert Enum.any?(err_p, &String.contains?(&1, "csv_has_corrupt_lines"))
  end
end
