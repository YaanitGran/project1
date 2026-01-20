import Config

config :file_processor, FileProcessor,
  max_workers: System.schedulers_online(),
  timeout_ms: 5_000,
  retries: 1,
  retry_delay_ms: 200,
  error_strategy: :mark_as_corrupt, # :skip | :mark_as_corrupt | :fail_fast
  progress: true,
  out: "output/reporte_final.txt",
  top_n_log_messages: 3 # confirmado por ti

config :logger, level: :info
