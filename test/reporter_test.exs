
defmodule ReporterTest do
  use ExUnit.Case, async: true

  alias PAI.Reporter

  # Minimal CSV/JSON/LOG metrics maps (only fields used by render_* functions)
  defp csv_metrics(file),
    do: %{
      file: file,
      total_sales: 1234.56,
      unique_products: 3,
      most_sold_product: {"Teclado", 2},
      top_category: {"Periféricos", 800.00},
      avg_discount_pct: 5.0,
      date_range: {~D[2024-01-01], ~D[2024-01-31]}
    }


  defp log_metrics(file),
    do: %{
      file: file,
      total_entries: 3,
      by_level: %{"INFO" => 1, "ERROR" => 1, "FATAL" => 1},
      by_level_pct: %{"INFO" => 33.3, "ERROR" => 33.3, "FATAL" => 33.3},
      top_error_component: {"DB", 1},
      hourly_distribution: [{9, 3}],
      avg_seconds_between_error: 60.0,
      top_messages: [{"DB Down", 1}]
    }

  defp build(%{
         results: results,
         errors: errors,
         dur_ms: dur_ms,
         opts: opts
       }) do
    Reporter.build_report(%{
      timestamp: DateTime.utc_now(),
      input_root: "(test)",
      mode: :parallel,
      results: results,
      errors: errors,
      duration_ms: dur_ms,
      runtime: %{process_count: 3, max_memory_mb: 10.0, duration_ms: dur_ms},
      options: opts
    })
  end

  test "conditional metric sections: only CSV and LOG appear when JSON is absent" do
    results = [
      {"a.csv", :csv, csv_metrics("a.csv"), %{metrics: :ok, row_errors: []}},
      {"b.log", :log, log_metrics("b.log"), %{metrics: :ok, line_errors: []}}
    ]

    report = build(%{results: results, errors: [], dur_ms: 100, opts: %{top_n_log_messages: 2}})

    assert report =~ "MÉTRICAS DE ARCHIVOS CSV"
    assert report =~ "MÉTRICAS DE ARCHIVOS LOG"
    refute report =~ "MÉTRICAS DE ARCHIVOS JSON"
  end

  test "grouped errors: JSON categories and CSV lines are grouped per file" do
    results = []
    errors = [
      # CSV aggregated error like pipeline produces:
      "Archivo ./data/ventas_corrupto.csv (csv): csv_has_corrupt_lines -> Línea 2: precio_unitario no es un número válido: 'ERR' | Línea 3: cantidad debe ser > 0, recibido 0",
      # JSON categories:
      "Archivo ./data/usuarios_mal.json (json): JSON mal formateado con: Comillas faltantes",
      "Archivo ./data/usuarios_mal.json (json): JSON mal formateado con: Llaves sin comillas",
      # Timeout sample:
      "./data/lento.log: Tiempo de espera excedido (timeout)"
    ]

    report = build(%{results: results, errors: errors, dur_ms: 10, opts: %{}})

    # JSON grouped once
    assert report =~ "✗ **./data/usuarios_mal.json** (json)"
    assert report =~ "  - JSON mal formateado con:"
    assert report =~ "    * Comillas faltantes"
    assert report =~ "    * Llaves sin comillas"

    # CSV grouped once with per-line bullets
    assert report =~ "✗ **./data/ventas_corrupto.csv** (csv)"
    assert report =~ "  - Líneas con error:"
    assert report =~ "    * Línea 2: precio_unitario"
    assert report =~ "    * Línea 3: cantidad debe ser > 0"

    # Timeout block
    assert report =~ "✗ **./data/lento.log**"
    assert report =~ "  - Tiempo de espera:"
  end

  test "success rate uses unique error files (not number of messages)" do
    results = [
      {"a.csv", :csv, csv_metrics("a.csv"), %{metrics: :ok, row_errors: []}}
    ]

    # two messages for the same JSON file => 1 error file
    errors = [
      "Archivo ./data/x.json (json): JSON mal formateado con: Comillas faltantes",
      "Archivo ./data/x.json (json): JSON mal formateado con: Comas faltantes"
    ]

    report = build(%{results: results, errors: errors, dur_ms: 10, opts: %{}})

    assert report =~ "Archivos con errores: 1"
    assert report =~ "Tasa de éxito:"
  end
end
