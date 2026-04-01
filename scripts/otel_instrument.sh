edot-bootstrap --action=install
$env:OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
$env:OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:4318/_otlp/v1/traces"
opentelemetry-instrument python rao/worker.py