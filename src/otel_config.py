import logging, warnings, os


def configure_otel(service_name, APP):
    # open telemetry
    if os.environ.get('JAEGER_ENABLED') == "True":
        logging.info("starting up jaeger telemetry")

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
        jaeger_host = os.environ.get('JAEGER_HOST', 'jaeger-otel-collector')
        jaeger_port = int(os.environ.get('JAEGER_PORT', '4317'))
        jaeger_endpoint = f'{jaeger_host}:{jaeger_port}'
        otlp_exporter = OTLPSpanExporter(endpoint=jaeger_endpoint)
        tracer_provider = TracerProvider(
            resource=Resource.create({telemetery_service_name_key: service_name})
        )
        tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        trace.set_tracer_provider(tracer_provider)
        FastAPIInstrumentor.instrument_app(APP, tracer_provider=tracer_provider, excluded_urls="docs,openapi.json")
        HTTPXClientInstrumentor().instrument()
