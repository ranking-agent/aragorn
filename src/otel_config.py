import logging, warnings, os


def configure_otel(service_name, APP):
    # open telemetry
    if os.environ.get('JAEGER_ENALBED') == "True":
        logging.info("starting up jaeger telemetry")

        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry import trace
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        # httpx connections need to be open a little longer by the otel decorators
        # but some libs display warnings of resource being unclosed.
        # these supresses such warnings.
        logging.captureWarnings(capture=True)
        warnings.filterwarnings("ignore",category=ResourceWarning)
        jaeger_host = os.environ.get('JAEGER_HOST', 'jaeger-otel-agent')
        jaeger_port = int(os.environ.get('JAEGER_PORT', '6831'))
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource.create({telemetery_service_name_key: service_name})
            )
        )
        jaeger_exporter = JaegerExporter(
            agent_host_name=jaeger_host,
            agent_port=jaeger_port,
        )
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter)
        )
        tracer = trace.get_tracer(__name__)
        FastAPIInstrumentor.instrument_app(APP, tracer_provider=trace, excluded_urls="docs,openapi.json")
        HTTPXClientInstrumentor().instrument()
