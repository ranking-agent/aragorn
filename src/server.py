"""wrapper for aragorn and robokop."""

from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from src.aragorn_app import ARAGORN_APP
from src.robokop_app import ROBOKOP_APP
from src.openapi_constructor import construct_open_api_schema
import os

#The app version is now going to be set in ../openapi-config.yaml
#if you want to bump it, do it there.
#Note that it's set once and applies to both robokop and aragorn (b/c they share much of their implementation)
#APP_VERSION = '2.0.24'

APP = FastAPI(title="ARAGORN/ROBOKOP")

# Mount aragorn app at /aragorn
APP.mount('/aragorn',  ARAGORN_APP, 'ARAGORN')
# Mount robokop app at /robokop
APP.mount('/robokop', ROBOKOP_APP, 'ROBOKOP')
# Add all routes of each app for open api generation at /openapi.json
# This will create an aggregate openapi spec.
APP.include_router(ARAGORN_APP.router, prefix='/aragorn')
APP.include_router(ROBOKOP_APP.router, prefix='/robokop')
APP.openapi_schema = construct_open_api_schema(APP, description="ARAGORN/ROBOKOP workflow engine")

# declare app access details
APP.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.environ.get("OTEL_ENABLED"):
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry import trace
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    service_name = os.environ.get('OTEL_SERVICE_NAME', 'Aragorn')
    assert service_name and isinstance(service_name, str)
    trace.set_tracer_provider(
        TracerProvider(
            resource=Resource.create({telemetery_service_name_key: service_name})
        )
    )
    jaeger_exporter = JaegerExporter(
        agent_host_name=os.environ.get("JAEGER_HOST", "localhost"),
        agent_port=int(os.environ.get("JAEGER_PORT", "6831")),
    )
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(jaeger_exporter)
    )
    tracer = trace.get_tracer(__name__)
    FastAPIInstrumentor.instrument_app(APP, tracer_provider=trace, excluded_urls=
                                       "docs,openapi.json") #,*cypher,*1.3/sri_testing_data")
    HTTPXClientInstrumentor().instrument()