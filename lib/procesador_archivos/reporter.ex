
defmodule ProcesadorArchivos.Reporter do
  @moduledoc """
  Builds and writes the final Spanish text report according to the template.
  Includes per-file metrics for CSV/JSON/LOG and consolidated section only for CSV,
  plus performance analysis and the final errors/warnings section.
  """

  alias ProcesadorArchivos.CSVMetrics

  @doc """
  Builds the full report string in Spanish.
  """

  def build_report(%{
        timestamp: ts,
        input_root: input_root,
        mode: mode,
        results: results,
        errors: errors,
        duration_ms: dur_ms,
        runtime: _runtime_info,
        options: opts
      }) do
    {csv_ms, json_ms, log_ms} = split_by_type(results)

    csv_consolidated = if csv_ms == [], do: nil, else: CSVMetrics.consolidate(csv_ms)

    total_files = length(results)
    counts = %{
      csv: length(csv_ms),
      json: length(json_ms),
      log: length(log_ms)
    }

    # --- NEW: group errors by file/type and compute unique error file count ---
    errors_group = group_errors_by_file(errors)
    error_files_count = map_size(errors_group)

    # Use unique error files count (NOT number of error messages)
    success_rate =
      if total_files + error_files_count > 0 do
        Float.round(100.0 * total_files / (total_files + error_files_count), 1)
      else
        0.0
      end

    mode_str =
      case mode do
        :parallel -> "Paralelo"
        :sequential -> "Secuencial"
      end

    ts_str = ts |> DateTime.shift_zone!("Etc/UTC") |> to_string()

    perf_section = build_performance_section(opts)

    """
    ================================================================================
                        REPORTE DE PROCESAMIENTO DE ARCHIVOS
    ================================================================================

    Fecha de generación: #{ts_str}
    Directorio procesado: #{input_root || "(lista de archivos)"}
    Modo de procesamiento: [#{mode_str}]

    --------------------------------------------------------------------------------
    RESUMEN EJECUTIVO
    --------------------------------------------------------------------------------
    Total de archivos procesados: #{total_files}
      - Archivos CSV: #{counts.csv}
      - Archivos JSON: #{counts.json}
      - Archivos LOG: #{counts.log}

    Tiempo total de procesamiento: #{Float.round(dur_ms / 1000.0, 2)} segundos
    Archivos con errores: #{error_files_count}
    Tasa de éxito: #{success_rate}%

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS CSV
    --------------------------------------------------------------------------------
    #{render_csv_files(csv_ms)}

    #{render_csv_consolidated(csv_consolidated)}

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS JSON
    --------------------------------------------------------------------------------
    #{render_json_files(json_ms)}

    --------------------------------------------------------------------------------
    MÉTRICAS DE ARCHIVOS LOG
    --------------------------------------------------------------------------------
    #{render_log_files(log_ms, opts)}

    --------------------------------------------------------------------------------
    ANÁLISIS DE RENDIMIENTO
    --------------------------------------------------------------------------------
    #{perf_section}

    --------------------------------------------------------------------------------
    ERRORES Y ADVERTENCIAS
    --------------------------------------------------------------------------------
    #{render_grouped_errors(errors_group)}

    ================================================================================
                               FIN DEL REPORTE
    ================================================================================
    """
  end


  @doc """
  Writes the report either to file (out path) or stdout if nil.
  """
  def write(report_string, out_path) when is_binary(out_path) do
    out_dir = Path.dirname(out_path)
    File.mkdir_p!(out_dir)
    File.write!(out_path, report_string)
    IO.puts("Reporte guardado en #{out_path}")
    :ok
  end
  def write(report_string, nil) do
    IO.puts(report_string)
    :ok
  end


  defp split_by_type(results) do
    Enum.reduce(results, {[], [], []}, fn
      {_path, :csv, metrics, _payload}, {c, j, l} -> {[metrics | c], j, l}
      {_path, :json, metrics, _payload}, {c, j, l} -> {c, [metrics | j], l}
      {_path, :log, metrics, _payload}, {c, j, l} -> {c, j, [metrics | l]}
    end)
    |> then(fn {c, j, l} -> {Enum.reverse(c), Enum.reverse(j), Enum.reverse(l)} end)
  end


  defp render_csv_files([]), do: "(No se procesaron archivos CSV)\n"
  defp render_csv_files(list) do
    list
    |> Enum.map(fn m ->
      most_sold =
        case m.most_sold_product do
          nil -> "N/A"
          {name, qty} -> "#{name} (#{qty} unidades)"
        end

      top_cat =
        case m.top_category do
          nil -> "N/A"
          {cat, amt} -> "#{cat} ($#{fmt_money(amt)})"
        end

      range =
        case m.date_range do
          nil -> "N/A"
          {d1, d2} -> "#{Date.to_string(d1)} a #{Date.to_string(d2)}"
        end

      """
      [Archivo: #{m.file}]
        * Total de ventas: $#{fmt_money(m.total_sales)}
        * Productos únicos: #{m.unique_products}
        * Producto más vendido: #{most_sold}
        * Categoría top: #{top_cat}
        * Descuento promedio: #{Float.round(m.avg_discount_pct, 1)}%
        * Período: #{range}
      """
    end)
    |> Enum.join("\n")
  end

  defp render_csv_consolidated(nil), do: ""
  defp render_csv_consolidated(m) do
    """
    Totales Consolidados CSV:
      - Ventas totales: $#{fmt_money(m.total_sales)}
      - Productos únicos totales: #{m.unique_products_total}
    """
  end

  defp render_json_files([]), do: "(No se procesaron archivos JSON)\n"
  defp render_json_files(list) do
    list
    |> Enum.map(fn m ->
      tops =
        m.top_actions
        |> Enum.with_index(1)
        |> Enum.map(fn {{act, cnt}, idx} -> "    #{idx}. #{act} (#{cnt} veces)" end)
        |> Enum.join("\n")

      peak =
        case m.peak_hour do
          nil -> "N/A"
          h -> "#{h}:00"
        end

      """
      [Archivo: #{m.file}]
        * Usuarios registrados: #{m.total_users}
        * Usuarios activos: #{m.active_users} (#{pct(m.active_users, m.total_users)}%)
        * Duración promedio de sesión: #{Float.round(m.avg_session_minutes, 2)} minutos
        * Páginas visitadas totales: #{m.total_pages}
        * Top acciones:
        #{tops}
        * Hora pico de actividad: #{peak}
      """
    end)
    |> Enum.join("\n")
  end

  defp render_log_files([],_opts), do: "(No se procesaron archivos LOG)\n"
  defp render_log_files(list, opts) do
    n = Map.get(opts, :top_n_log_messages, 3)

    list
    |> Enum.map(fn m ->
      dist =
        ["DEBUG","INFO","WARN","ERROR","FATAL"]
        |> Enum.map(fn lvl ->
          cnt = Map.get(m.by_level, lvl, 0)
          pc = Map.get(m.by_level_pct, lvl, 0.0)
          "      - #{lvl}: #{cnt} (#{pc}%)"
        end)
        |> Enum.join("\n")

      top_comp =
        case m.top_error_component do
          nil -> "N/A"
          {c, k} -> "#{c} (#{k} errores)"
        end

      hourly =
        m.hourly_distribution
        |> Enum.map(fn {h, cnt} -> "      #{String.pad_leading(Integer.to_string(h), 2, "0")}: #{cnt}" end)
        |> Enum.join("\n")

      top_msgs =
        m.top_messages
        |> Enum.with_index(1)
        |> Enum.map(fn {{msg, cnt}, idx} -> "    #{idx}. \"#{msg}\" (#{cnt} ocurrencias)" end)
        |> Enum.join("\n")

      avgf = m.avg_seconds_between_fatal || "N/A"

      """
      [Archivo: #{m.file}]
        * Total de entradas: #{m.total_entries}
        * Distribución por nivel:
      #{dist}
        * Componente más problemático: #{top_comp}
        * Distribución por hora:
      #{hourly}
        * Tiempo promedio entre errores FATAL: #{avgf}
        * Patrones de error recurrentes (Top #{n}):
      #{top_msgs}
      """
    end)
    |> Enum.join("\n")
  end

  # ------------------------------------------------------------
  # Groups raw error strings by (path, type) to render one block per file
  # ------------------------------------------------------------
  defp group_errors_by_file(errs) do
    Enum.reduce(errs, %{}, fn err, acc ->
      cond do
        # CSV aggregated message the pipeline emits:
        # "Archivo <path> (csv): csv_has_corrupt_lines -> Línea 2: ... | Línea 3: ..."
        Regex.match?(~r/^Archivo\s+(.+?)\s+\(csv\):\s+csv_has_corrupt_lines\s+->\s+(.+)$/u, err) ->
          [_, path, joined] =
            Regex.run(~r/^Archivo\s+(.+?)\s+\(csv\):\s+csv_has_corrupt_lines\s+->\s+(.+)$/u, err)

          items = String.split(joined, " | ")
          Map.update(acc, {path, :csv}, items, &(&1 ++ items))

        # JSON category line from pipeline:
        # "Archivo <path> (json): JSON mal (formateado|malformateado) con: <category>"
        Regex.match?(~r/^Archivo\s+(.+?)\s+\(json\):\s+JSON mal(?: |)formateado con:\s+(.+)$/u, err) ->
          [_, path, cat] =
            Regex.run(~r/^Archivo\s+(.+?)\s+\(json\):\s+JSON mal(?: |)formateado con:\s+(.+)$/u, err)

          Map.update(acc, {path, :json}, [cat], &[cat | &1])

        # Timeout lines like "<path>: Tiempo de espera excedido (timeout)"
        Regex.match?(~r/^(.+?):\s+Tiempo de espera excedido.*$/u, err) ->
          [_, path] = Regex.run(~r/^(.+?):\s+Tiempo de espera excedido.*$/u, err)
          Map.update(acc, {path, :timeout}, [err], &[err | &1])

        # Generic "<path>: <message>" fallback
        Regex.match?(~r/^(.+?):\s+(.+)$/u, err) ->
          [_, path, msg] = Regex.run(~r/^(.+?):\s+(.+)$/u, err)
          Map.update(acc, {path, :unknown}, [msg], &[msg | &1])

        # Fallback (no path detected)
        true ->
          Map.update(acc, {"(desconocido)", :unknown}, [err], &[err | &1])
      end
    end)
    # Ensure stable order and uniqueness inside each group
    |> Enum.into(%{}, fn {k, v} -> {k, Enum.reverse(Enum.uniq(v))} end)
  end

  # ------------------------------------------------------------
  # Renders grouped errors: one block per file with nested bullets
  # ------------------------------------------------------------
  defp render_grouped_errors(group) when map_size(group) == 0 do
    "(Sin errores)\n"
  end

  defp render_grouped_errors(group) do
    group
    |> Enum.map(fn
      {{path, :csv}, items} ->
        bullets =
          items
          |> order_csv_line_errors()
          |> Enum.map(&("    * " <> &1))
          |> Enum.join("\n")

        """
          ✗ **#{path}** (csv)
          - Líneas con error:
          #{bullets}
        """

      {{path, :json}, cats} ->
        bullets =
          cats
          |> Enum.map(&("    * " <> &1))
          |> Enum.join("\n")

        """
          ✗ **#{path}** (json)
          - JSON mal formateado con:
          #{bullets}
        """

      {{path, :timeout}, msgs} ->
        bullets =
          msgs
          |> Enum.map(&("    * " <> &1))
          |> Enum.join("\n")

        """
          ✗ **#{path}**
          - Tiempo de espera:
          #{bullets}
        """

      {{path, _other}, msgs} ->
        bullets =
          msgs
          |> Enum.map(&("    * " <> &1))
          |> Enum.join("\n")

        """
          ✗ **#{path}**
          - Errores:
          #{bullets}
        """
    end)
    |> Enum.join("\n")
  end

  # Orders CSV line errors by line number if they match "Línea <n>: ..."
  defp order_csv_line_errors(items) do
    items
    |> Enum.map(fn s ->
      case Regex.run(~r/^Línea\s+(\d+):\s+(.+)$/u, s) do
        [_, n, _msg] -> {String.to_integer(n), s}
        _ -> {1_000_000_000, s} # fallback at the end
      end
    end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end


  defp build_performance_section(opts) do
    # This section is filled when the user runs `ProcesadorArchivos.benchmark/2`.
    # Here we just provide placeholders indicating how to obtain real values.
    """
    Para obtener esta sección con datos reales, ejecute:

      iex> ProcesadorArchivos.benchmark("#{Map.get(opts, :input_root, "./data")}", %{max_workers: #{Map.get(opts, :max_workers)}})
      # => imprime comparación secuencial vs paralelo y factor de mejora.

    """
  end

  defp fmt_money(v) do
    # format with thousands separator ',' and decimal '.'
    :erlang.float_to_binary(v, decimals: 2)
    |> String.replace(~r/(?<=\d)(?=(\d{3})+\.)/, ",")
  end

  defp pct(part, total) when total > 0, do: Float.round(part * 100.0 / total, 1)
  defp pct(_part, _total), do: 0.0
end
