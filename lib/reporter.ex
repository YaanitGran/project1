
defmodule ProcesadorArchivos.Reporter do
  @moduledoc """
  Presentation layer for the final report (aligned to section 5.1).
  Expects a 'metrics' map with:
    - meta: %{generated_at, mode, root_dir, total_time_s, counts, errors_count, success_rate}
    - csv:  %{per_file: [...], consolidated: %{ventas_totales, productos_unicos_totales}}
    - json: %{per_file: [...]}
    - log:  %{per_file: [...]}
    - performance: nil or %{sequential_s, parallel_s, speedup_x, processes_used, max_memory_mb}
    - errors: [] or [%{file, detail}]
  """

  @doc """
  Generates the full report string based on 'metrics' and processing mode.
  """
  def generate(metrics, mode) when mode in [:secuencial, :paralelo] do
    meta = Map.get(metrics, :meta, %{})
    csv  = Map.get(metrics, :csv, %{})
    json = Map.get(metrics, :json, %{})
    log  = Map.get(metrics, :log, %{})
    perf = Map.get(metrics, :performance, nil)
    errs = Map.get(metrics, :errors, [])

    """
    ================================================================================
                        REPORTE DE PROCESAMIENTO DE ARCHIVOS
    ================================================================================

    Fecha de generación: #{format_dt(Map.get(meta, :generated_at, NaiveDateTime.local_now()))}
    Directorio procesado: #{Map.get(meta, :root_dir, "N/A")}
    Modo de procesamiento: #{mode_label(mode)}

    --------------------------------------------------------------------------------
    RESUMEN EJECUTIVO
    --------------------------------------------------------------------------------
    Total de archivos procesados: #{dig(meta, [:counts, :total], 0)}
      - Archivos CSV: #{dig(meta, [:counts, :csv], 0)}
      - Archivos JSON: #{dig(meta, [:counts, :json], 0)}
      - Archivos LOG: #{dig(meta, [:counts, :log], 0)}

    Tiempo total de procesamiento: #{fmt_float(Map.get(meta, :total_time_s, nil))} segundos
    Archivos con errores: #{Map.get(meta, :errors_count, 0)}
    Tasa de éxito: #{fmt_float(Map.get(meta, :success_rate, nil))}%

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS CSV
    --------------------------------------------------------------------------------
    #{format_csv_per_file(Map.get(csv, :per_file, []))}
    Totales Consolidados CSV:
      - Ventas totales: $#{fmt_money(dig(csv, [:consolidated, :ventas_totales], 0.0))}
      - Productos únicos totales: #{dig(csv, [:consolidated, :productos_unicos_totales], 0)}

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS JSON
    --------------------------------------------------------------------------------
    #{format_json_per_file(Map.get(json, :per_file, []))}

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS LOG
    --------------------------------------------------------------------------------
    #{format_log_per_file(Map.get(log, :per_file, []))}

    --------------------------------------------------------------------------------
    ANÁLISIS DE RENDIMIENTO
    --------------------------------------------------------------------------------
    #{format_performance(perf)}

    --------------------------------------------------------------------------------
    ERRORES Y ADVERTENCIAS
    --------------------------------------------------------------------------------
    #{format_errors(errs)}

    ================================================================================
                             FIN DEL REPORTE
    ================================================================================
    """
  end

  # ---------------- Helpers: labels & basic formatting ----------------

  # Mode label (Spanish output)
  defp mode_label(:secuencial), do: "Secuencial"
  defp mode_label(:paralelo),   do: "Paralelo"

  # Format NaiveDateTime to "YYYY-MM-DD HH:MM:SS"
  defp format_dt(%NaiveDateTime{} = dt) do
    dt
    |> NaiveDateTime.to_string()
    |> String.slice(0, 19)
  end

  # Float formatter (two decimals or "N/A")
  defp fmt_float(nil), do: "N/A"
  defp fmt_float(f) when is_float(f), do: :erlang.float_to_binary(f, decimals: 2)
  defp fmt_float(n) when is_integer(n), do: Integer.to_string(n)
  defp fmt_float(_), do: "N/A"

  # Money formatter (two decimals)
  defp fmt_money(f) when is_float(f), do: :erlang.float_to_binary(f, decimals: 2)
  defp fmt_money(n) when is_integer(n), do: Integer.to_string(n)
  defp fmt_money(_), do: "0.00"

  # Safe deep-get (map dig)
  defp dig(map, path, default \\ nil) do
    Enum.reduce(path, map, fn k, acc -> (is_map(acc) && Map.get(acc, k)) || default end)
  end

  # ---------------- CSV section ----------------

  # Print per-file CSV metrics block(s)
  defp format_csv_per_file(list) when is_list(list) do
    list
    |> Enum.map(fn m ->
      """
      [Archivo: #{Map.get(m, :file, "N/A")}]
        * Total de ventas: $#{fmt_money(Map.get(m, :total_ventas, 0.0))}
        * Productos únicos: #{Map.get(m, :productos_unicos, 0)}
        * Producto más vendido: #{format_prod(Map.get(m, :producto_mas_vendido, {"N/A", 0}))}
        * Categoría top: #{format_cat(Map.get(m, :categoria_top, {"N/A", 0.0}))}
        * Descuento promedio: #{fmt_float(Map.get(m, :descuento_promedio, nil))}%
        * Período: #{format_range(Map.get(m, :rango_fechas, {~D[1970-01-01], ~D[1970-01-01]}))}
      """
    end)
    |> Enum.join("\n")
  end

  # "Laptop X (12 unidades)"
  defp format_prod({name, units}), do: "#{name} (#{units} unidades)"

  # "Computadoras ($12345.67)"
  defp format_cat({name, income}), do: "#{name} ($#{fmt_money(income)})"

  # Date range formatting

  # Format "YYYY-MM-DD a YYYY-MM-DD" when both values are Date structs
  defp format_range({%Date{} = d1, %Date{} = d2}) do
    "#{Date.to_string(d1)} a #{Date.to_string(d2)}"
  end

  # Fallback when the input is not a pair of Date structs
  defp format_range(_), do: "N/A a N/A"


  # ---------------- JSON section ----------------

  # Print per-file JSON metrics block(s)
  defp format_json_per_file(list) when is_list(list) do
    list
    |> Enum.map(fn m ->
      """
      [Archivo: #{Map.get(m, :file, "N/A")}]
        * Usuarios registrados: #{Map.get(m, :usuarios_total, 0)}
        * Usuarios activos: #{Map.get(m, :usuarios_activos, 0)} (#{fmt_float(Map.get(m, :activos_pct, nil))}%)
        * Duración promedio de sesión: #{fmt_float(Map.get(m, :duracion_promedio_min, nil))} minutos
        * Páginas visitadas totales: #{Map.get(m, :paginas_totales, 0)}
        * Top acciones:
      #{format_top_actions(Map.get(m, :top_acciones, []))}
      """
    end)
    |> Enum.join("\n")
  end


  # In reporter.ex
    defp format_top_actions(list) do
      list
      |> Enum.with_index(1)
      |> Enum.map(fn {{action, n}, i} ->
        "    #{i}. #{action} (#{n} veces)"
      end)
      |> case do
        [] -> ["    (Sin acciones)"]
        lines -> lines
      end
      |> Enum.join("\n")
    end


  # ---------------- LOG section ----------------

  # Print per-file LOG metrics block(s)
  defp format_log_per_file(list) when is_list(list) do
    list
    |> Enum.map(fn m ->
      dist = Map.get(m, :distribucion_por_nivel, %{})
      """
      [Archivo: #{Map.get(m, :file, "N/A")}]
        * Total de entradas: #{Map.get(m, :total_entradas, 0)}
        * Distribución por nivel:
          - DEBUG: #{format_level(dist, :debug)}
          - INFO:  #{format_level(dist, :info)}
          - WARN:  #{format_level(dist, :warn)}
          - ERROR: #{format_level(dist, :error)}
          - FATAL: #{format_level(dist, :fatal)}
        * Componente más problemático: #{format_component(Map.get(m, :componente_mas_problemas, {"N/A", 0}))}
        * Patrón de error frecuente: #{format_pattern(Map.get(m, :patron_error_frecuente, {"N/A", 0}))}
      """
    end)
    |> Enum.join("\n")
  end

  # Format "count (xx.xx%)" or fallback
  defp format_level(dist, key) do
    case Map.get(dist, key, {0, 0.0}) do
      {count, pct} -> "#{count} (#{fmt_float(pct)}%)"
      count when is_integer(count) -> "#{count} (N/A)"
      _ -> "0 (N/A)"
    end
  end

  defp format_component({comp, n}), do: "#{comp} (#{n} errores)"
  defp format_pattern({msg, n}), do: ~s|"#{msg}" (#{n} ocurrencias)|

  # ---------------- Performance section ----------------

  defp format_performance(nil) do
    """
    Comparación Secuencial vs Paralelo:
      * Tiempo secuencial:
      * Tiempo paralelo:
      * Factor de mejora:
      * Procesos utilizados:
      * Memoria máxima:
    """
  end

  defp format_performance(%{
         sequential_s: seq,
         parallel_s: par,
         speedup_x: speed,
         processes_used: procs,
         max_memory_mb: mem
       }) do
    """
    Comparación Secuencial vs Paralelo:
      * Tiempo secuencial: #{fmt_float(seq)} segundos
      * Tiempo paralelo: #{fmt_float(par)} segundos
      * Factor de mejora: #{fmt_float(speed)} veces más rápido
      * Procesos utilizados: #{procs || "N/A"}
      * Memoria máxima: #{mem || "N/A"} MB
    """
  end

  # ---------------- Errors/Warnings section ----------------

  defp format_errors(list) do
    if Enum.empty?(list) do
      "  No errors found"
    else
      list
      |> Enum.map(fn %{file: file, detail: detail} ->
        # Use exact symbols requested
        prefix = if String.contains?(String.downcase(detail), "timeout"), do: "⚠", else: "✗"
        "#{prefix} #{file}: #{detail}"
      end)
      |> Enum.join("\n")
    end
  end


end
