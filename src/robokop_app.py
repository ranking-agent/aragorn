"""ROBOKOP"""
import os
import logging.config
import pkg_resources
import yaml

from enum import Enum
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse, AsyncQueryStatusResponse, AsyncQueryResponse
from pydantic import BaseModel

from fastapi import Body, FastAPI, BackgroundTasks

from src.openapi_constructor import construct_open_api_schema
from src.common import sync_query, async_query, status_query
from src.default_queries import default_input_sync, default_input_async
# import open telemetery configuration
from src.otel_config import configure_otel

# declare the FastAPI details
title = "ROBOKOP"
ROBOKOP_APP = FastAPI(title=title)
service_name = os.environ.get('OTEL_SERVICE_NAME', 'ARAGORN') + '-' + title
configure_otel(service_name=service_name, APP=ROBOKOP_APP)
# Set up default logger.
with pkg_resources.resource_stream("src", "logging.yml") as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = "./logs"

## Gets made by aragorn_app
## make the directory if it does not exist
# if not os.path.exists(log_dir):
#    os.makedirs(log_dir)

# create a configuration for the log file
config["handlers"]["file"]["filename"] = os.path.join(log_dir, "robokop.log")

# load the log config
logging.config.dictConfig(config)

# create a logger
logger = logging.getLogger(__name__)

## ROBOKOP should not need to make async calls
## declare the directory where the async data files will exist
# queue_file_dir = './queue-files'
#
## make the directory if it does not exist
# if not os.path.exists(queue_file_dir):
#    os.makedirs(queue_file_dir)

# declare the types of answer coalesce methods
class MethodName(str, Enum):
    all = "all"
    none = "none"
    graph = "graph"
    ontology = "ontology"
    property = "property"


# define the default request bodies
default_request_sync: Body = Body(default=default_input_sync)
default_request_async: Body = Body(default=default_input_async, example=default_input_async)


# async entry point
@ROBOKOP_APP.post("/asyncquery", tags=["ROBOKOP"], response_model=AsyncQueryResponse)
async def async_query_handler(
    background_tasks: BackgroundTasks, request: PDAsyncQuery = default_request_async, answer_coalesce_type: MethodName = MethodName.all
):
    """
    Performs an asynchronous query operation which compiles data from numerous ARAGORN ranking agent services.
    The services are called in the following order, each passing their output to the next service as an input:

    Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    return await async_query(background_tasks, request, answer_coalesce_type, logger, "ROBOKOP")


# synchronous entry point
@ROBOKOP_APP.post("/query", tags=["ROBOKOP"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request_sync, answer_coalesce_type: MethodName = MethodName.all):
    """Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
    The services are called in the following order, each passing their output to the next service as an input:

    Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    return await sync_query(request, answer_coalesce_type, logger, "ROBOKOP")

@ROBOKOP_APP.get("/asyncquery_status", tags=["ROBOKOP"], response_model=AsyncQueryStatusResponse, status_code=200)
async def status_query_handler(job_id: str):
    """Checks the status of an asynchronous query operation."""
    return await status_query(job_id)

ROBOKOP_APP.openapi_schema = construct_open_api_schema(ROBOKOP_APP, prefix="robokop", description="ROBOKOP: A non-federated ARA", infores="infores:robokop")
