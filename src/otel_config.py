import logging, warnings, os


def configure_otel(service_name, APP):
    # open telemetry
    if os.environ.get('JAEGER_ENABLED') == "True":
        logging.info("starting up otel telemetry")

        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        # httpx connections need to be open a little longer by the otel decorators
        # but some libs display warnings of resource being unclosed.
        # these supresses such warnings.
        logging.captureWarnings(capture=True)
        warnings.filterwarnings("ignore",category=ResourceWarning)

        provider = TracerProvider(
            resource=Resource.create({telemetery_service_name_key: service_name})
        )

        # Export via OTLP. The Jaeger exporter has been deprecated and removed from
        # OpenTelemetry (Jaeger ingests OTLP natively now). Prefer the endpoint provided
        # by the environment -- e.g. injected by the OpenTelemetry Operator
        # auto-instrumentation -- otherwise fall back to JAEGER_HOST/JAEGER_PORT for
        # standalone runs, defaulting to the standard OTLP gRPC port.
        if os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT'):
            otlp_exporter = OTLPSpanExporter()
        else:
            otlp_host = os.environ.get('JAEGER_HOST', 'jaeger-otel-agent')
            otlp_port = os.environ.get('JAEGER_PORT', '4317')
            otlp_exporter = OTLPSpanExporter(endpoint=f"{otlp_host}:{otlp_port}", insecure=True)

        provider.add_span_processor(
            BatchSpanProcessor(otlp_exporter)
        )
        trace.set_tracer_provider(provider)

        FastAPIInstrumentor.instrument_app(APP, tracer_provider=provider, excluded_urls="docs,openapi.json")
        HTTPXClientInstrumentor().instrument(tracer_provider=provider)
