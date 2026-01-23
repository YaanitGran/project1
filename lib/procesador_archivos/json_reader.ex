
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
        case decode_json(bin, path) do
          {:ok, data} when is_map(data) ->
            {users, user_errs}   = validate_users(Map.get(data, "usuarios", []))
            {sessions, sess_errs}= validate_sessions(Map.get(data, "sesiones", []))
            {:ok, %{usuarios: users, sesiones: sessions}, user_errs ++ sess_errs}

          {:ok, _other} ->
            {:error, [:estructura_raiz_invalida]}

          {:error, categories} ->
            # malformed JSON → categories already normalized
            {:error, categories}
        end

      {:error, reason} ->
        {:error, [{:lectura_fallida, inspect(reason)}]}
    end
  end



    def categorize_errors(element_errors) when is_list(element_errors) do
       element_errors
       |> Enum.flat_map(&categorize_element_error/1)
       |> Enum.uniq()
       |> default_other()
    end

    # Infer categories for malformed JSON (DecodeError), inspecting both the error message and the raw content.

    defp default_other([]), do: [:otro]
    defp default_other(list), do: Enum.reverse(list)

    # -------------------------
    # Private: categorize element-level messages
    # -------------------------

    # Map validation error strings to categories (duración negativa, tipos, etc.)
    defp categorize_element_error(err) when is_binary(err) do
      cond do
        # Typical negative duration hint
        String.contains?(err, "duracion_segundos") and String.contains?(err, "no negativo") ->
          [{:valor_invalido, "duracion_segundos negativa"}]

        # Type mismatches: boolean/integer/etc.
        String.contains?(err, "booleano") or String.contains?(err, "entero") ->
          [:tipos_incorrectos]

        true ->
          [:otro]
      end
    end



    # Parses JSON and returns either {:ok, map} or {:error, categories}

    # Parses JSON and returns either {:ok, data} or {:error, categories}
    defp decode_json(bin, _path) do
      case Jason.decode(bin) do
        {:ok, data} ->
          {:ok, data}

        {:error, %Jason.DecodeError{} = e} ->
          {:error, detect_syntax_categories(bin, Exception.message(e))}
      end
    end

    # ---- Heuristic syntax categorizer (inspects the whole binary) ----
    defp detect_syntax_categories(bin, msg) do
      cats =
        []
        |> maybe(:comillas_faltantes,
             unclosed_quotes?(bin)
             or String.contains?(msg, "unexpected byte")
             or String.contains?(msg, "invalid string"))
        |> maybe(:comas_faltantes,
             missing_commas_between_pairs?(bin)
             or String.contains?(msg, "expected comma"))
        |> maybe(:llaves_sin_comillas,
             keys_without_quotes?(bin)
             or String.contains?(msg, "expected string for key")
             or String.contains?(msg, "object keys must be strings"))
        |> maybe(:comentarios_no_validos,
             String.contains?(bin, "//") or String.contains?(bin, "/*"))
        |> maybe(:corchetes_sin_cerrar, count?("[", bin) > count?("]", bin))
        |> maybe(:llaves_sin_cerrar,   count?("{", bin) > count?("}", bin))

      if cats == [], do: [:otro], else: Enum.uniq(cats)
    end

    # Count unescaped quotes (odd count → likely unclosed)
    defp unclosed_quotes?(bin) do
      Regex.scan(~r/(?<!\\)"/, bin) |> length() |> rem(2) == 1
    end

    # Key-value followed by a new key in next line without a comma (coarse heuristic)
    defp missing_commas_between_pairs?(bin) do
      Regex.match?(~r/"[^"]+"\s*:\s*[^,\}\]\n]+?\n\s*"[^"]+"\s*:/, bin)
    end

    # Unquoted JSON object keys like: usuario_id: 3002
    defp keys_without_quotes?(bin) do
      Regex.match?(~r/(?m)^\s*[A-Za-z_][A-Za-z0-9_]*\s*:/, bin)
    end

    defp count?(needle, bin), do: :binary.matches(bin, needle) |> length()

    defp maybe(list, tag, true),  do: [tag | list]
    defp maybe(list, _tag, false), do: list


    def humanize_category({:valor_invalido, msg}), do: "Valores inválidos (#{msg})"
    def humanize_category({:lectura_fallida, r}),  do: "No se pudo leer el archivo (#{r})"
    def humanize_category(:estructura_raiz_invalida), do: "Estructura raíz inválida (se esperaba objeto JSON)"
    def humanize_category(:comillas_faltantes),       do: "Comillas faltantes"
    def humanize_category(:comas_faltantes),          do: "Comas faltantes"
    def humanize_category(:llaves_sin_comillas),      do: "Llaves sin comillas"
    def humanize_category(:corchetes_sin_cerrar),     do: "Corchetes sin cerrar"
    def humanize_category(:llaves_sin_cerrar),        do: "Llaves sin cerrar"
    def humanize_category(:comentarios_no_validos),   do: "Comentarios no válidos en JSON"
    def humanize_category(:tipos_incorrectos),        do: "Tipos de datos incorrectos"
    def humanize_category(:otro),                     do: "Error de JSON no clasificado"



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

  defp int(v, _field) when is_integer(v), do: {:ok, v}
  defp int(v, field), do: {:error, "Campo '#{field}' debe ser entero, recibido: #{inspect(v)}"}

  defp non_neg_int(v, _field) when is_integer(v) and v >= 0, do: {:ok, v}
  defp non_neg_int(v, field), do: {:error, "Campo '#{field}' debe ser entero no negativo, recibido: #{inspect(v)}"}

  defp boolean(v, _field) when is_boolean(v), do: {:ok, v}
  defp boolean(v, field), do: {:error, "Campo '#{field}' debe ser booleano, recibido: #{inspect(v)}"}

  defp non_empty(v, field) do
    if is_binary(v) and String.trim(v) != "" do
      :ok
    else
      {:error, "Campo '#{field}' no puede estar vacío"}
    end
  end

  defp iso8601(v, field) when is_binary(v) do
    case DateTime.from_iso8601(v) do
      {:ok, _dt, _offset} -> {:ok, v}
      _ -> {:error, "Campo '#{field}' no es ISO-8601 válido: #{inspect(v)}"}
    end
  end
  defp iso8601(v, field), do: {:error, "Campo '#{field}' debe ser string ISO-8601, recibido: #{inspect(v)}"}

  defp string_list(v, field) do
    cond do
      not is_list(v) ->
        {:error, "Campo '#{field}' debe ser una lista"}

      Enum.all?(v, &is_binary/1) ->
        {:ok, v}

      true ->
        {:error, "Campo '#{field}' debe ser una lista de strings, recibido: #{inspect(v)}"}
    end
  end
end
