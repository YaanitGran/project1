defmodule ProcesadorArchivos.Metrics do
  @moduledoc """
  Contiene la lógica de negocio para calcular métricas de CSV, JSON y LOGs.
  """

  # Cambiado de defp a def para que sean accesibles desde ProcesadorArchivos
  def csv_metrics(rows,file_name) do

    typed =
        rows
        |> Enum.flat_map(fn
          [fecha, producto, categoria, precio_str, cantidad_str, desc_str] ->
            try do
              [%{
                fecha: Date.from_iso8601!(fecha),
                producto: producto,
                categoria: categoria,
                precio_unitario: String.to_float(precio_str),
                cantidad: String.to_integer(cantidad_str),
                descuento: String.to_float(desc_str)
              }]
            rescue
              _ -> []
            end

          _ -> []
        end)
        |> Enum.filter(fn r ->
          r.precio_unitario >= 0.0 and r.cantidad >= 0 and r.descuento >= 0.0 and r.descuento <= 100.0
        end)

      total_ventas =
        typed
        |> Enum.map(fn r -> r.precio_unitario * r.cantidad * (1.0 - r.descuento / 100.0) end)
        |> Enum.sum()

      _productos_unicos =
        typed
        |> Enum.map(& &1.producto)
        |> MapSet.new()
        |> MapSet.size()

      {prod_mas, unidades_mas} =
        typed
        |> Enum.group_by(& &1.producto)
        |> Enum.map(fn {prod, rs} -> {prod, Enum.map(rs, & &1.cantidad) |> Enum.sum()} end)
        |> Enum.max_by(fn {_p, qty} -> qty end, fn -> {"N/A", 0} end)

      {cat_top, ingreso_top} =
        typed
        |> Enum.group_by(& &1.categoria)
        |> Enum.map(fn {cat, rs} ->
          ingreso =
            rs
            |> Enum.map(fn r -> r.precio_unitario * r.cantidad * (1.0 - r.descuento / 100.0) end)
            |> Enum.sum()
          {cat, ingreso}
        end)
        |> Enum.max_by(fn {_c, ingreso} -> ingreso end, fn -> {"N/A", 0.0} end)

      descuento_promedio =
        case typed do
          [] -> 0.0
          _ ->
            typed
            |> Enum.map(& &1.descuento)
            |> Enum.sum()
            |> Kernel./(length(typed))
        end

      fechas = Enum.map(typed, & &1.fecha)
      rango_fechas =
        case fechas do
          [] -> {~D[1970-01-01], ~D[1970-01-01]}
          _  -> {Enum.min(fechas), Enum.max(fechas)}
        end


        productos =
            typed
            |> Enum.map(& &1.producto)
            |> MapSet.new()

          productos_unicos = MapSet.size(productos)

          %{
            file: file_name,
            total_ventas: total_ventas,
            productos_unicos: productos_unicos,
            productos: productos, # <- keep full set for exact consolidation
            producto_mas_vendido: {prod_mas, unidades_mas},
            categoria_top: {cat_top, ingreso_top},
            descuento_promedio: descuento_promedio,
            rango_fechas: rango_fechas
          }


  end



  def csv_consolidated(per_file_list) when is_list(per_file_list) do
    ventas_totales =
      per_file_list
      |> Enum.map(&Map.get(&1, :total_ventas, 0.0))
      |> Enum.sum()

    # Exact unique products across all CSV files (uses Set union)
    productos_unicos_totales =
      per_file_list
      |> Enum.map(&Map.get(&1, :productos, MapSet.new()))
      |> Enum.reduce(MapSet.new(), &MapSet.union/2)
      |> MapSet.size()

    %{
      ventas_totales: ventas_totales,
      productos_unicos_totales: productos_unicos_totales
    }
  end



  def json_metrics(%{usuarios: usuarios, sesiones: sesiones}, file_name) do
      usuarios_total   = length(usuarios)
      usuarios_activos = Enum.count(usuarios, fn u -> Map.get(u, "activo", false) == true end)
      activos_pct      = pct(usuarios_activos, usuarios_total)

      duracion_promedio_min =
        case sesiones do
          [] -> 0.0
          _  -> sesiones |> Enum.map(&Map.get(&1, "duracion_segundos", 0)) |> avg() |> Kernel./(60.0)
        end

      paginas_totales =
        sesiones |> Enum.map(&Map.get(&1, "paginas_visitadas", 0)) |> Enum.sum()

      top_acciones =
        sesiones
        |> Enum.flat_map(&Map.get(&1, "acciones", []))
        |> Enum.frequencies()
        |> Enum.sort_by(fn {_acc, n} -> -n end)
        |> Enum.take(3)

      %{
        file: file_name,
        usuarios_total: usuarios_total,
        usuarios_activos: usuarios_activos,
        activos_pct: activos_pct,
        duracion_promedio_min: duracion_promedio_min,
        paginas_totales: paginas_totales,
        top_acciones: top_acciones
      }
    end


    def log_metrics(entries, file_name) when is_list(entries) do
      total_entradas = length(entries)

      counts =
        entries
        |> Enum.map(& &1.level)
        |> Enum.frequencies()

      dist_pct = fn key ->
        c = Map.get(counts, key, 0)
        p = pct(c, total_entradas)       # porcentaje por nivel
        {c, p}
      end

      # Componente más problemático (solo errores/fatales)
      componente_mas_problemas =
        entries
        |> Enum.filter(fn e -> e.level in [:error, :fatal] end)
        |> Enum.map(& &1.component)
        |> Enum.frequencies()
        |> (fn freq ->
          if map_size(freq) == 0, do: {"N/A", 0}, else: Enum.max_by(freq, fn {_k, v} -> v end)
        end).()

      # Patrón de error más frecuente (mensaje más repetido en error/fatal)
      patron_error_frecuente =
        entries
        |> Enum.filter(&(&1.level in [:error, :fatal]))
        |> Enum.map(& &1.msg)
        |> Enum.frequencies()
        |> Enum.sort_by(fn {_m, n} -> -n end)
        |> List.first()
        |> case do
          nil -> {"N/A", 0}
          {m, n} -> {m, n}
        end

      %{
        file: file_name,
        total_entradas: total_entradas,
        distribucion_por_nivel: %{
          debug: dist_pct.(:debug),
          info:  dist_pct.(:info),
          warn:  dist_pct.(:warn),
          error: dist_pct.(:error),
          fatal: dist_pct.(:fatal)
        },
        componente_mas_problemas: componente_mas_problemas,
        patron_error_frecuente: patron_error_frecuente
      }
    end

    # Helpers (asegúrate de tenerlos en el módulo):
    defp pct(_part, total) when total <= 0, do: 0.0
    defp pct(part, total), do: (part / total) * 100.0

  # Helpers privados (estos sí pueden ser defp)
  defp avg(list), do: Enum.sum(list) / max(length(list), 1)

end
