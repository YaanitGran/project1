
defmodule ProcesadorArchivos.JSONReader do
  @moduledoc """
  JSON reader for 'usuarios' and 'sesiones' blocks with strict type checking.
  Whole-file JSON parsing errors are reported as a single parse error.
  Element-level errors are accumulated with specific messages.
  """

  @doc """
  Returns {:ok, %{usuarios: valid_users, sesiones: valid_sessions}, errors}

  * On JSON parse failure, returns {:error, ["<message>"]}.
  * Validates required fields and types per spec.
  """
  def read(path) do
    case File.read(path) do
      {:ok, bin} ->
        with {:ok, data} <- decode_json(bin, path) do
          {users, user_errs} = validate_users(Map.get(data, "usuarios", []))
          {sessions, sess_errs} = validate_sessions(Map.get(data, "sesiones", []))
          {:ok, %{usuarios: users, sesiones: sessions}, user_errs ++ sess_errs}
        else
          {:error, msg} -> {:error, [msg]}
        end

      {:error, reason} ->
        {:error, ["No se pudo leer #{path}: #{inspect(reason)}"]}
    end
  end

  defp decode_json(bin, path) do
    case Jason.decode(bin) do
      {:ok, data} when is_map(data) ->
        {:ok, data}

      {:ok, _other} ->
        {:error, "JSON raíz inválida en #{path}: se esperaba objeto"}

      {:error, %Jason.DecodeError{} = e} ->
        {:error, "JSON malformado en #{path}: #{Exception.message(e)}"}
    end
  end

  defp validate_users(list) when is_list(list) do
    Enum.with_index(list, 0)
    |> Enum.reduce({[], []}, fn {u, idx}, {acc, errs} ->
      case valid_user(u) do
        {:ok, u2} -> {[u2 | acc], errs}
        {:error, msg} -> {acc, [msg <> " (usuarios[#{idx}])" | errs]}
      end
    end)
    |> then(fn {a, e} -> {Enum.reverse(a), Enum.reverse(e)} end)
  end

  defp valid_user(%{
         "id" => id,
         "nombre" => nombre,
         "email" => email,
         "activo" => activo,
         "ultimo_acceso" => ts
       }) do
    with {:ok, _} <- int(id, "id"),
         :ok <- non_empty(nombre, "nombre"),
         :ok <- non_empty(email, "email"),
         {:ok, _} <- boolean(activo, "activo"),
         {:ok, _} <- iso8601(ts, "ultimo_acceso") do
      {:ok, %{id: id, nombre: nombre, email: email, activo: activo, ultimo_acceso: ts}}
    else
      {:error, _} = e -> e
    end
  end

  defp valid_user(_other), do: {:error, "Objeto usuario inválido o campos faltantes"}

  defp validate_sessions(list) when is_list(list) do
    Enum.with_index(list, 0)
    |> Enum.reduce({[], []}, fn {s, idx}, {acc, errs} ->
      case valid_session(s) do
        {:ok, s2} -> {[s2 | acc], errs}
        {:error, msg} -> {acc, [msg <> " (sesiones[#{idx}])" | errs]}
      end
    end)
    |> then(fn {a, e} -> {Enum.reverse(a), Enum.reverse(e)} end)
  end

  defp valid_session(%{
         "usuario_id" => uid,
         "inicio" => inicio,
         "duracion_segundos" => dur,
         "paginas_visitadas" => pages
       } = s) do
    acciones = Map.get(s, "acciones", [])
    with {:ok, _} <- int(uid, "usuario_id"),
         {:ok, _} <- iso8601(inicio, "inicio"),
         {:ok, _} <- non_neg_int(dur, "duracion_segundos"),
         {:ok, _} <- non_neg_int(pages, "paginas_visitadas"),
         {:ok, _} <- string_list(acciones, "acciones") do
      {:ok, %{usuario_id: uid, inicio: inicio, duracion_segundos: dur, paginas_visitadas: pages, acciones: acciones}}
    else
      {:error, _} = e -> e
    end
  end

  defp valid_session(_other), do: {:error, "Objeto sesión inválido o campos faltantes"}

  # Validators (type checks)

  defp int(v, field) when is_integer(v), do: {:ok, v}
  defp int(v, field), do: {:error, "Campo '#{field}' debe ser entero, recibido: #{inspect(v)}"}

  defp non_neg_int(v, field) when is_integer(v) and v >= 0, do: {:ok, v}
  defp non_neg_int(v, field), do: {:error, "Campo '#{field}' debe ser entero no negativo, recibido: #{inspect(v)}"}

  defp boolean(v, field) when is_boolean(v), do: {:ok, v}
  defp boolean(v, field), do: {:error, "Campo '#{field}' debe ser booleano, recibido: #{inspect(v)}"}

  defp non_empty(v, field) when is_binary(v) and byte_size(String.trim(v)) > 0, do: :ok
  defp non_empty(v, field), do: {:error, "Campo '#{field}' no puede estar vacío"}

  defp iso8601(v, field) when is_binary(v) do
    case DateTime.from_iso8601(v) do
      {:ok, _dt, _offset} -> {:ok, v}
      _ -> {:error, "Campo '#{field}' no es ISO-8601 válido: #{inspect(v)}"}
    end
  end
  defp iso8601(v, field), do: {:error, "Campo '#{field}' debe ser string ISO-8601, recibido: #{inspect(v)}"}

  defp string_list(v, field) when is_list(v) and Enum.all?(v, &is_binary/1), do: {:ok, v}
  defp string_list(v, field), do: {:error, "Campo '#{field}' debe ser lista de strings, recibido: #{inspect(v)}"}
end
