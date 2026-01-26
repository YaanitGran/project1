
defmodule JSONReaderTest do
  use ExUnit.Case, async: true

  alias PAI.JSONReader

  defp write!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "json_reader_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  test "valid JSON returns data and empty element_errors", %{dir: dir} do
    path =
      write!(Path.join(dir, "ok.json"), """
      {
        "usuarios": [
          { "id": 1, "nombre": "Ana", "email": "ana@test.com", "activo": true, "ultimo_acceso": "2024-01-01T10:00:00Z" }
        ],
        "sesiones": [
          { "usuario_id": 1, "inicio": "2024-01-01T09:00:00Z", "duracion_segundos": 300, "paginas_visitadas": 2, "acciones": ["login","logout"] }
        ]
      }
      """)

    assert {:ok, %{usuarios: u, sesiones: s}, errs} = JSONReader.read(path)
    assert is_list(u) and is_list(s)
    assert errs == []
  end

  test "malformed JSON returns syntax categories", %{dir: dir} do
    path =
      write!(Path.join(dir, "mal.json"), """
      {
        "usuarios": [
          { "id": 1, "nombre": "Ana, "email": "ana@test.com", "activo": true }
        ],
        "sesiones": [
          { "usuario_id": 1, "inicio": "2024-01-01T09:00:00Z", "duracion_segundos": "abc", "paginas_visitadas": 2 "acciones": ["login","logout"] }
        ]
        // invalid comment
      }
      """)

    assert {:error, cats} = JSONReader.read(path)
    assert is_list(cats) and cats != []
    # humanized labels should be non-empty strings
    assert Enum.all?(cats, fn c ->
             is_binary(JSONReader.humanize_category(c)) and JSONReader.humanize_category(c) != ""
           end)
  end

  test "structurally valid JSON but element-level errors can be categorized", %{dir: dir} do
    path =
      write!(Path.join(dir, "semantico.json"), """
      {
        "usuarios": [
          { "id": 2, "nombre": "Luis", "email": "luis@test.com", "activo": "si", "ultimo_acceso": "2024-01-01T10:00:00Z" }
        ],
        "sesiones": [
          { "usuario_id": 2, "inicio": "2024-01-01T09:00:00Z", "duracion_segundos": -5, "paginas_visitadas": 2, "acciones": ["login","logout"] }
        ]
      }
      """)

    assert {:ok, _data, element_errors} = JSONReader.read(path)
    cats = JSONReader.categorize_errors(element_errors)
    # at least one category like :tipos_incorrectos or {:valor_invalido, ...}
    assert Enum.any?(cats, fn
             :tipos_incorrectos -> true
             {:valor_invalido, _} -> true
             _ -> false
           end)
  end
end
