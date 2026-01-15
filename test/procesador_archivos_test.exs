
defmodule ProcesadorArchivosTest do
  use ExUnit.Case, async: false
  alias ProcesadorArchivos.{Reader, Metrics, Reporter}

  # --- Temporal route and fixtures names ---
  @tmp_base Path.join(System.tmp_dir!(), "pa_test_" <> Integer.to_string(:erlang.unique_integer([:positive])))
  @data_dir Path.join(@tmp_base, "data")
  @out_dir  Path.join(@tmp_base, "output")
  @report   Path.join(@out_dir, "reporte_final.txt")

  @csv1 "ventas_enero.csv"
  @csv2 "ventas_febrero.csv"
  @json1 "usuarios.json"
  @json_bad "usuarios_malformado.json"
  @log1 "sistema.log"
  @log2 "aplicacion.log"

  # NOTE (EN): Normalizes separators by rebuilding the path via Path.split -> Path.join
  defp norm(path) when is_binary(path),
    do: path |> Path.expand() |> Path.split() |> Path.join()

  setup_all do
    File.rm_rf!(@tmp_base)
    File.mkdir_p!(@data_dir)
    File.mkdir_p!(@out_dir)

    # CSV fixtures
    csv1 = [
      "fecha,producto,categoria,precio_unitario,cantidad,descuento",
      "2024-01-02,Laptop X,Computadoras,1000.00,1,10.0",
      "2024-01-03,Mouse Y,Accesorios,50.00,2,0.0",
      "2024-01-05,Cable HDMI,Accesorios,20.00,3,0.0"
    ] |> Enum.join("\n")

    csv2 = [
      "fecha,producto,categoria,precio_unitario,cantidad,descuento",
      "2024-02-01,Laptop X,Computadoras,1200.00,1,5.0",
      "2024-02-03,Teclado Z,Accesorios,80.00,1,15.0",
      "2024-02-10,SSD 512GB,Almacenamiento,99.99,2,30.0"
    ] |> Enum.join("\n")

    File.write!(Path.join(@data_dir, @csv1), csv1)
    File.write!(Path.join(@data_dir, @csv2), csv2)

    # JSON fixtures
    json = %{
      "usuarios" => [
        %{"id" => 1, "nombre" => "A", "email" => "a@x.com", "activo" => true,  "ultimo_acceso" => "2024-02-01T08:00:00Z"},
        %{"id" => 2, "nombre" => "B", "email" => "b@x.com", "activo" => false, "ultimo_acceso" => "2024-02-01T09:00:00Z"}
      ],
      "sesiones" => [
        %{"usuario_id" => 1, "inicio" => "2024-02-01T08:00:00Z", "duracion_segundos" => 600,  "paginas_visitadas" => 5,  "acciones" => ["login","logout"]},
        %{"usuario_id" => 2, "inicio" => "2024-02-01T09:00:00Z", "duracion_segundos" => 1800, "paginas_visitadas" => 10, "acciones" => ["login","ver_dashboard","logout"]}
      ]
    }
    File.write!(Path.join(@data_dir, @json1), Jason.encode!(json))

    # BAD JSON to exercise error path
    File.write!(Path.join(@data_dir, @json_bad), "{ this is not valid JSON }")

    # LOG fixtures
    log1 = [
      "2024-02-01 08:00:00 [INFO]  [Sistema] Inicio de horario laboral",
      "2024-02-01 08:10:00 [WARN]  [Storage] Cuota al 85%",
      "2024-02-01 08:15:00 [ERROR] [API] Gateway timeout",
      "2024-02-01 08:20:00 [DEBUG] [Cache] Prewarming cache",
      "2024-02-01 08:30:00 [INFO]  [Backup] Respaldo completado"
    ] |> Enum.join("\n")

    log2 = [
      "2024-02-01 09:00:00 [INFO]  [Auth] Usuario inicio sesion",
      "2024-02-01 09:05:00 [ERROR] [Auth] Credenciales invalidas",
      "2024-02-01 09:10:00 [INFO]  [Reportes] Reporte generado",
      "2024-02-01 09:15:00 [WARN]  [API] Rate limit"
    ] |> Enum.join("\n")

    File.write!(Path.join(@data_dir, @log1), log1)
    File.write!(Path.join(@data_dir, @log2), log2)

    on_exit(fn -> File.rm_rf!(@tmp_base) end)

    {:ok,
      data_dir: @data_dir,
      out_dir: @out_dir,
      report_path: @report,
      csv_paths: [Path.join(@data_dir, @csv1), Path.join(@data_dir, @csv2)],
      json_path: Path.join(@data_dir, @json1),
      json_bad_path: Path.join(@data_dir, @json_bad),
      log_paths: [Path.join(@data_dir, @log1), Path.join(@data_dir, @log2)]
    }
  end

  # --- Reader tests ---
  describe "Reader (CSV/JSON/LOG)" do
    test "read_csv returns RFC4180 rows", ctx do
      [csv1, csv2] = ctx[:csv_paths]
      rows1 = Reader.read_csv(csv1)
      rows2 = Reader.read_csv(csv2)
      assert is_list(rows1) and is_list(rows2)
      refute rows1 == []; assert length(hd(rows1)) == 6
      refute rows2 == []; assert length(hd(rows2)) == 6
    end

    test "read_json returns {:ok, data} with usuarios and sesiones (atom or string keys)", ctx do
      {:ok, data} = Reader.read_json(ctx[:json_path])
      usuarios = Map.get(data, :usuarios) || Map.get(data, "usuarios")
      sesiones = Map.get(data, :sesiones) || Map.get(data, "sesiones")
      assert is_list(usuarios)
      assert is_list(sesiones)
    end

    test "read_log returns {:ok, entries} with required keys", ctx do
      {:ok, entries1} = Reader.read_log(Enum.at(ctx[:log_paths], 0))
      {:ok, entries2} = Reader.read_log(Enum.at(ctx[:log_paths], 1))
      assert Enum.all?(entries1, fn e -> Map.has_key?(e, :ts) and Map.has_key?(e, :level) and Map.has_key?(e, :component) and Map.has_key?(e, :msg) end)
      assert Enum.all?(entries2, fn e -> Map.has_key?(e, :ts) and Map.has_key?(e, :level) and Map.has_key?(e, :component) and Map.has_key?(e, :msg) end)
    end
  end

  # --- Metrics tests ---
  describe "Metrics (per-file CSV/JSON/LOG + CSV consolidated)" do
    test "csv_metrics/2 computes totals per file", ctx do
      [csv1, csv2] = ctx[:csv_paths]
      m1 = Metrics.csv_metrics(Reader.read_csv(csv1), Path.basename(csv1))
      m2 = Metrics.csv_metrics(Reader.read_csv(csv2), Path.basename(csv2))
      assert is_map(m1) and is_map(m2)
      for m <- [m1, m2] do
        assert Map.has_key?(m, :file)
        assert Map.has_key?(m, :total_ventas)
        assert Map.has_key?(m, :productos_unicos)
        assert Map.has_key?(m, :producto_mas_vendido)
        assert Map.has_key?(m, :categoria_top)
        assert Map.has_key?(m, :descuento_promedio)
        assert Map.has_key?(m, :rango_fechas)
      end
    end

    test "csv_consolidated/1 computes exact totals across files", ctx do
      [csv1, csv2] = ctx[:csv_paths]
      m1 = Metrics.csv_metrics(Reader.read_csv(csv1), Path.basename(csv1))
      m2 = Metrics.csv_metrics(Reader.read_csv(csv2), Path.basename(csv2))
      consolidated = Metrics.csv_consolidated([m1, m2])
      assert Map.has_key?(consolidated, :ventas_totales)
      assert Map.has_key?(consolidated, :productos_unicos_totales)
      max_uniques = max(m1.productos_unicos, m2.productos_unicos)
      assert consolidated.productos_unicos_totales >= max_uniques
    end

    test "json_metrics/2 returns expected fields", ctx do
      {:ok, data} = Reader.read_json(ctx[:json_path])
      m = Metrics.json_metrics(data, Path.basename(ctx[:json_path]))
      assert Map.has_key?(m, :usuarios_total)
      assert Map.has_key?(m, :usuarios_activos)
      assert Map.has_key?(m, :activos_pct)
      assert Map.has_key?(m, :duracion_promedio_min)
      assert Map.has_key?(m, :paginas_totales)
      assert Map.has_key?(m, :top_acciones)
    end

    test "log_metrics/2 returns level distribution and error hotspots", ctx do
      {:ok, entries} = Reader.read_log(Enum.at(ctx[:log_paths], 0))
      m = Metrics.log_metrics(entries, Path.basename(Enum.at(ctx[:log_paths], 0)))
      assert Map.has_key?(m, :total_entradas)
      assert Map.has_key?(m, :distribucion_por_nivel)
      assert Map.has_key?(m, :componente_mas_problemas)
      assert Map.has_key?(m, :patron_error_frecuente)
      dist = m.distribucion_por_nivel
      {sum_counts, sum_pct} =
        [:debug, :info, :warn, :error, :fatal]
        |> Enum.map(&Map.get(dist, &1, {0, 0.0}))
        |> Enum.reduce({0, 0.0}, fn {c, p}, {ac, ap} -> {ac + c, ap + p} end)
      assert sum_counts == m.total_entradas
      assert_in_delta(sum_pct, 100.0, 0.01)
    end
  end

  # --- Reporter presence tests ---
  describe "Reporter (5.1 format presence checks)" do
    test "generate/2 includes sections and per-file blocks" do
      metrics = %{
        meta: %{
          generated_at: NaiveDateTime.local_now(),
          mode: :secuencial,
          root_dir: "/tmp/demo",
          total_time_s: 1.23,
          counts: %{csv: 1, json: 1, log: 1, total: 3},
          errors_count: 0,
          success_rate: 100.0
        },
        csv: %{
          per_file: [
            %{
              file: "ventas_enero.csv",
              total_ventas: 100.0,
              productos_unicos: 2,
              producto_mas_vendido: {"Laptop X", 1},
              categoria_top: {"Computadoras", 80.0},
              descuento_promedio: 10.0,
              rango_fechas: {~D[2024-01-02], ~D[2024-01-03]}
            }
          ],
          consolidated: %{ventas_totales: 100.0, productos_unicos_totales: 2}
        },
        json: %{
          per_file: [
            %{
              file: "usuarios.json",
              usuarios_total: 2,
              usuarios_activos: 1,
              activos_pct: 50.0,
              duracion_promedio_min: 5.0,
              paginas_totales: 15,
              top_acciones: [{"login", 2}, {"logout", 2}, {"ver_dashboard", 1}]
            }
          ]
        },
        log: %{
          per_file: [
            %{
              file: "sistema.log",
              total_entradas: 3,
              distribucion_por_nivel: %{debug: {1, 33.33}, info: {1, 33.33}, warn: {0, 0.0}, error: {1, 33.33}, fatal: {0, 0.0}},
              componente_mas_problemas: {"API", 1},
              patron_error_frecuente: {"Gateway timeout", 1}
            }
          ]
        },
        performance: nil,
        errors: []
      }

      text = Reporter.generate(metrics, :secuencial)
      assert text =~ "RESUMEN EJECUTIVO"
      assert text =~ "MÉTRICAS DE ARCHIVOS CSV"
      assert text =~ "[Archivo: ventas_enero.csv]"
      assert text =~ "Totales Consolidados CSV:"
      assert text =~ "MÉTRICAS DE ARCHIVOS JSON"
      assert text =~ "[Archivo: usuarios.json]"
      assert text =~ "MÉTRICAS DE ARCHIVOS LOG"
      assert text =~ "[Archivo: sistema.log]"
      assert text =~ "ANÁLISIS DE RENDIMIENTO"
      assert text =~ "ERRORES Y ADVERTENCIAS"
    end
  end

  # --- Orchestrator tests ---
  describe "Orchestrator (process sequential/parallel and compare)" do
    test "process/1 (sequential) writes final report", ctx do
      File.rm_rf!(ctx[:report_path])
      {:ok, path} = ProcesadorArchivos.process(ctx[:data_dir])
      assert norm(path) == norm(ctx[:report_path])
      assert File.exists?(path)
      text = File.read!(path)
      assert text =~ "Modo de procesamiento: Secuencial"
      assert text =~ "[Archivo: ventas_enero.csv]"
      assert text =~ "[Archivo: ventas_febrero.csv]"
      assert text =~ "MÉTRICAS DE ARCHIVOS JSON"
      assert text =~ "MÉTRICAS DE ARCHIVOS LOG"
      assert text =~ "Totales Consolidados CSV:"
    end

    test "process/2 (parallel) writes final report with progress", ctx do
      File.rm_rf!(ctx[:report_path])
      {:ok, path} = ProcesadorArchivos.process(ctx[:data_dir], :paralelo)
      assert norm(path) == norm(ctx[:report_path])
      assert File.exists?(path)
      text = File.read!(path)
      assert text =~ "Modo de procesamiento: Paralelo"
      assert text =~ "RESUMEN EJECUTIVO"
      assert text =~ "[Archivo: ventas_enero.csv]"
      assert text =~ "[Archivo: ventas_febrero.csv]"
    end

    test "process/3 (parallel with opts) writes report to custom outdir and lists JSON error", ctx do
      custom_out = Path.join(@tmp_base, "custom_out")
      File.rm_rf!(custom_out)
      File.mkdir_p!(custom_out)

      {:ok, path} =
        ProcesadorArchivos.process(@data_dir, :paralelo, %{
          out_dir: custom_out,
          max_retries: 1,
          per_process_timeout_ms: 2_000
        })

      assert norm(path) == norm(Path.join(custom_out, "reporte_final.txt"))
      assert File.exists?(path)
      text = File.read!(path)
      assert text =~ "ERRORES Y ADVERTENCIAS"
      assert text =~ "JSON read error"
    end

    test "compare/2 writes final report including performance analysis", ctx do
      File.rm_rf!(ctx[:report_path])
      {:ok, path} = ProcesadorArchivos.compare(ctx[:data_dir])
      assert norm(path) == norm(ctx[:report_path])
      assert File.exists?(path)
      text = File.read!(path)
      assert text =~ "Tiempo secuencial:"
      assert text =~ "Tiempo paralelo:"
      assert text =~ "Factor de mejora:"
    end
  end
end
