
defmodule ProcesadorArchivosTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  # --------- Helpers to create temp folders & files ---------

  setup_all do
    base = Path.join(System.tmp_dir!(), "pa_tests_#{System.unique_integer([:positive])}")
    File.mkdir_p!(base)

    on_exit(fn -> File.rm_rf!(base) end)

    {:ok, base: base}
  end

  # Small helper to write a file atomically
  defp write!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  # ---- Minimal valid samples (compatible with your readers) ----

  # Normalize CRLF to LF, trim trailing newline from heredoc, and ensure final "\n"
  defp with_nl(s) when is_binary(s) do
    s
    |> String.replace("\r\n", "\n")
    |> String.trim_trailing()
    |> then(&(&1 <> "\n"))
  end

  defp sample_csv_valid do
    with_nl("""
    fecha,producto,categoria,precio_unitario,cantidad,descuento
    2024-01-01,Teclado,Periféricos,250.50,2,10
    2024-01-02,Mouse,Periféricos,120.00,1,0
    """)
  end

  defp sample_csv_corrupt_header do
    with_nl("""
    fecha,producto,categoria,precio,cantidad,descuento
    2024-01-01,Teclado,Periféricos,250.50,2,10
    """)
  end

  defp sample_log_valid do
    with_nl("""
    2024-01-01T09:00:00Z INFO App Started normal mode
    2024-01-01T09:01:00Z ERROR Auth Failed to login user
    2024-01-01T09:02:00Z FATAL DB Connection lost
    """)
  end

  defp sample_json_valid do
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
  end

  # Deliberately malformed JSON to trigger decoder categories
  defp sample_json_malformed do
    """
    {
      "usuarios": [
        { "id": 1, "nombre": "Ana, "email": "ana@test.com", "activo": true }
      ],
      "sesiones": [
        { "usuario_id": 1, "inicio": "2024-01-01T09:00:00Z", "duracion_segundos": "abc", "paginas_visitadas": 2 "acciones": ["login","logout"] }
      ]
      // invalid comment
    }
    """
  end

  # ------------------ File builders per test ------------------

  defp build_dataset!(base) do
    dir = Path.join(base, "data_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    ok_csv  = write!(Path.join(dir, "ventas_ok.csv"), sample_csv_valid())
    bad_csv = write!(Path.join(dir, "ventas_corrupto.csv"), sample_csv_corrupt_header())
    ok_json = write!(Path.join(dir, "usuarios.json"), sample_json_valid())
    bad_json= write!(Path.join(dir, "usuarios_malformado.json"), sample_json_malformed())
    ok_log  = write!(Path.join(dir, "sistema.log"), sample_log_valid())

    %{dir: dir, ok_csv: ok_csv, bad_csv: bad_csv, ok_json: ok_json, bad_json: bad_json, ok_log: ok_log}
  end

  defp read_file!(path), do: File.read!(path)

  defp exc_msg(exc) do
    exc
    |> Exception.message()
    |> String.replace("\r\n", "\n")
  end

  # --------------- Assertions helpers for reports ---------------

  defp assert_report_present_and_useful!(out_path, expectations \\ []) do
    assert File.exists?(out_path)
    report = read_file!(out_path)

    # basic header lines
    assert report =~ "REPORTE DE PROCESAMIENTO DE ARCHIVOS"
    assert report =~ "RESUMEN EJECUTIVO"
    assert report =~ "ERRORES Y ADVERTENCIAS"

    Enum.each(expectations, fn exp -> assert report =~ exp end)

    :ok
  end

  # =============================================================
  #                     Happy-path tests
  # =============================================================

  describe "process_directory/2" do
    test "processes mixed dataset and generates report", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.process_directory(ds.dir, %{mode: :parallel})

      assert is_map(out)
      assert is_list(out.results)
      assert is_list(out.errors)
      assert is_integer(out.duration_ms)
      assert is_binary(out.out)

      assert_report_present_and_useful!(out.out, [
        "Archivos CSV:", "Archivos JSON:", "Archivos LOG:"
      ])
    end
  end

  describe "process_files/2" do
    test "processes explicit list (csv, json, log) and reports", %{base: base} do
      ds = build_dataset!(base)
      files = [ds.ok_csv, ds.ok_json, ds.ok_log]

      {:ok, out} = ProcesadorArchivos.process_files(files, %{mode: :parallel})

      assert length(out.results) == 3
      assert_report_present_and_useful!(out.out)
    end

    test "reports missing files but continues with existing ones", %{base: base} do
      ds = build_dataset!(base)
      missing = Path.join(ds.dir, "no_existe.csv")

      {:ok, out} =
        ProcesadorArchivos.process_files([ds.ok_csv, missing], %{mode: :sequential})

      assert Enum.any?(out.results, fn {path, _type, _metrics, _payload} -> path == ds.ok_csv end)
      assert Enum.any?(out.errors, &String.contains?(&1, "No se encontró el archivo"))
      assert_report_present_and_useful!(out.out, ["No se encontró el archivo"])
    end
  end

  describe "procesar_con_manejo_errores/1 (always writes report)" do
    test "single file: runs pipeline with one file and writes report", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores(ds.ok_csv, %{mode: :parallel})
      assert length(out.results) == 1
      assert_report_present_and_useful!(out.out, [Path.basename(ds.ok_csv)])
    end

    test "directory: delegates to directory processing and writes report", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores(ds.dir, %{mode: :parallel})
      assert_report_present_and_useful!(out.out)
    end

    test "list: delegates to files processing and writes report", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.procesar_con_manejo_errores([ds.ok_csv, ds.ok_json], %{})
      assert length(out.results) == 2
      assert_report_present_and_useful!(out.out)
    end
  end

  describe "benchmark/2 and wrapper" do
    test "benchmark returns timing map with expected keys", %{base: base} do
      ds = build_dataset!(base)
      {:ok, b} = ProcesadorArchivos.benchmark(ds.dir, %{progress: false})

      assert is_map(b)
      assert b.sequential.duration_ms >= 0
      assert b.parallel.duration_ms >= 0
      assert Map.has_key?(b, :speedup)
      assert is_map(b.processes_used)
      assert is_map(b.max_memory_mb)
    end

    test "wrapper prints in seconds with 3 decimals and returns :ok", %{base: base} do
      ds = build_dataset!(base)
      assert :ok == ProcesadorArchivos.benchmark_paralelo_vs_secuencial(ds.dir)
    end
  end

  # =============================================================
  #                     Negative-path tests
  # =============================================================


  describe "argument validations & helpful messages" do
    test "process_directory/2 with non-directory binary raises with suggestion", %{base: base} do
      file = write!(Path.join(base, "not_a_dir.json"), sample_json_valid())

      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.process_directory(file, %{})
        end

      msg = exc_msg(exc)
      assert msg =~ "La ruta no es un directorio"
      assert msg =~ "Uso correcto de process_directory/2"
      assert msg =~ "Sugerencia"
    end

    test "process_directory/2 with non-binary arg raises with usage", _ do
      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.process_directory(123, %{})
        end

      msg = exc_msg(exc)
      assert msg =~ "Argumentos inválidos para process_directory/2"
      assert msg =~ "Uso correcto"
    end

    test "process_files/2 with non-list raises", _ do
      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.process_files("not-list", %{})
        end

      msg = exc_msg(exc)
      assert msg =~ "Argumentos inválidos para process_files/2"
    end

    test "process_files/2 with invalid entries in list raises", %{base: base} do
      dir = Path.join(base, "ad")
      File.mkdir_p!(dir)

      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.process_files([dir, 123], %{})
        end

      msg = exc_msg(exc)

      assert msg =~ "Argumentos inválidos en la lista (no string o no es archivo regular):"
      assert msg =~ "Uso correcto"
    end

    test "procesar_con_opciones/2 invalid opts raises", %{base: base} do
      ds = build_dataset!(base)

      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.procesar_con_opciones(ds.dir, [:bad])
        end

      msg = exc_msg(exc)
      assert msg =~ "Argumentos inválidos para procesar_con_opciones/2"
      assert msg =~ "opciones: map"
    end

    test "procesar_con_manejo_errores/1 with missing file raises helpful error", %{base: base} do
      missing = Path.join(base, "missing.json")

      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.procesar_con_manejo_errores(missing)
        end

      msg = exc_msg(exc)
      assert msg =~ "Ruta no encontrada"
      assert msg =~ "Uso correcto"
    end

    test "benchmark_paralelo_vs_secuencial/1 with non-binary raises", _ do
      exc =
        assert_raise ArgumentError, fn ->
          ProcesadorArchivos.benchmark_paralelo_vs_secuencial(123)
        end

      msg = exc_msg(exc)
      assert msg =~ "Argumentos inválidos para benchmark_paralelo_vs_secuencial/1"
      assert msg =~ "Uso correcto"
    end
  end


  # =============================================================
  #                     JSON malformed grouping
  # =============================================================

  describe "reporter groups JSON malformed errors per file" do
    test "malformed JSON shows grouped categories once per file", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.process_files([ds.bad_json], %{})

      assert_report_present_and_useful!(out.out, [
        Path.basename(ds.bad_json),
        "JSON mal formateado con:"
      ])
    end
  end

  # =============================================================
  #                     CSV corrupt lines grouping
  # =============================================================

  describe "reporter groups CSV line errors per file" do
    test "corrupt CSV shows one block with bullets per line", %{base: base} do
      ds = build_dataset!(base)
      {:ok, out} = ProcesadorArchivos.process_files([ds.bad_csv], %{})

      assert_report_present_and_useful!(out.out, [
        Path.basename(ds.bad_csv),
        "Líneas con error:"
      ])
    end
  end
end
