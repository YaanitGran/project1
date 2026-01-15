
defmodule ProcesadorArchivos.CLI do
  @moduledoc """
  Command-line interface for the File Processor (CSV/JSON/LOG).

  Usage:
    procesador_archivos [options] <path | list_of_paths>

  Options:
    --mode sequential | parallel | compare
    --max <int>            (optional; parallel/compare) max concurrency
    --timeout <ms>         (optional; parallel/compare) coordinator receive timeout
    --per-timeout <ms>     (optional; parallel/compare) per-process timeout
    --retries <int>        (optional; parallel/compare) automatic retries per file
    --outdir <path>        (optional) output directory override
    --help                 shows this help

  Notes:
    - 'sequential' and 'parallel' use module defaults unless you pass options via CLI.
    - 'compare' runs both sequential and parallel on the same input and includes performance analysis.
    - You can provide either a single path (directory or file) or multiple file paths.
  """

  def main(argv) do
    # Parse CLI options
    {opts, args, invalid} =
      OptionParser.parse(
        argv,
        switches: [
          mode: :string,
          max: :integer,
          timeout: :integer,
          per_timeout: :integer,   # maps --per-timeout to :per_timeout
          retries: :integer,
          outdir: :string,
          help: :boolean
        ],
        aliases: [h: :help]
      )

    # Build runtime options for orchestrator
    # NOTE (EN): 'process/3' and 'compare/2' will merge these with module-level defaults.
    rt_opts = %{
      max_concurrency: (opts[:max] || System.schedulers_online()),
      timeout_ms: (opts[:timeout] || 15_000),
      per_process_timeout_ms: (opts[:per_timeout] || 10_000),
      max_retries: (opts[:retries] || 1),
      out_dir: opts[:outdir]
    }


    cond do
      opts[:help] ->
        print_help()

      invalid != [] ->
        IO.puts(:stderr, "Invalid options: #{inspect(invalid)}")
        print_help()
        System.halt(1)

      args == [] ->
        IO.puts(:stderr, "Missing path or list of paths.")
        print_help()
        System.halt(1)

      true ->
        # Support a single path or multiple paths
        paths_or_dir =
          case args do
            [single] -> single
            many     -> many
          end

        # Normalize mode string (default: sequential)
        mode =
          case String.downcase(to_string(opts[:mode] || "sequential")) do
            "sequential" -> :secuencial
            "seq"        -> :secuencial
            "parallel"   -> :paralelo
            "par"        -> :paralelo
            "compare"    -> :compare
            "cmp"        -> :compare
            _            -> :secuencial
          end

        # Dispatch to orchestrator
        case mode do
          :compare ->
            # Compare accepts runtime opts (timeouts, retries, max concurrency, outdir)
            case ProcesadorArchivos.compare(paths_or_dir, rt_opts) do
              {:ok, out_path} ->
                IO.puts("Report generated (compare): #{out_path}")

              {:error, reason} ->
                IO.puts(:stderr, "Error: #{reason}")
                System.halt(1)
            end

          _ ->
            # process/3 accepts runtime opts (timeouts, retries, max concurrency, outdir)
            case ProcesadorArchivos.process(paths_or_dir, mode, rt_opts) do
              {:ok, out_path} ->
                IO.puts("Report generated: #{out_path}")

              {:error, reason} ->
                IO.puts(:stderr, "Error: #{reason}")
                System.halt(1)
            end
        end
    end
  end

  # Print CLI help text
  defp print_help do
    IO.puts("""
    Usage:
      procesador_archivos [options] <path | list_of_paths>

    Examples:
      procesador_archivos --mode sequential ./data
      procesador_archivos --mode parallel   ./data
      procesador_archivos --mode compare    --max 8 --timeout 20000 --per-timeout 12000 --retries 2 --outdir ./reportes ./data
      procesador_archivos ./data/ventas_enero.csv ./data/usuarios.json ./data/sistema.log

    Options:
      --mode sequential | parallel | compare
      --max <int>            (optional; parallel/compare) max concurrency
      --timeout <ms>         (optional; parallel/compare) coordinator receive timeout
      --per-timeout <ms>     (optional; parallel/compare) per-process timeout
      --retries <int>        (optional; parallel/compare) automatic retries per file
      --outdir <path>        (optional) output directory override
      --help

    Notes:
      - In parallel mode, module defaults are used unless you pass options via CLI.
      - 'compare' runs both sequential and parallel modes on the same input and includes performance analysis.
    """)
  end
end
