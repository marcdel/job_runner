import Config

config :o11y, :attribute_namespace, "app"

key = System.fetch_env!("HONEYCOMB_KEY")

config :opentelemetry_exporter,
  otlp_endpoint: "https://api.honeycomb.io:443",
  otlp_headers: [{"x-honeycomb-team", key}]

# config :opentelemetry,
#  sweeper: %{span_ttl: :infinity, interval: :infinity},
#  processors: [{:otel_simple_processor, %{}}]

config :opentelemetry, :processors, [{:otel_simple_processor, %{}}]
