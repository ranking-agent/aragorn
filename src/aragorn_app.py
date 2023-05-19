"""aragorn server."""
import os
import logging.config
import pkg_resources
import yaml
import aio_pika
import random
import string

# from pamqp import specification as spec
from enum import Enum
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse, AsyncQueryResponse, AsyncQueryStatusResponse
from pydantic import BaseModel
from fastapi import Body, FastAPI, BackgroundTasks
from src.openapi_constructor import construct_open_api_schema
from src.common import async_query, sync_query, status_query
from src.default_queries import default_input_sync, default_input_async
from src.util import get_channel_pool
from src.otel_config import configure_otel

# declare the FastAPI details
title = "ARAGORN"
ARAGORN_APP = FastAPI(title=title)

# configures open telemetry iff enabled.
service_name = os.environ.get('OTEL_SERVICE_NAME', 'ARAGORN') + '-' + title
configure_otel(service_name=service_name, APP=ARAGORN_APP)

# Set up default logger.
with pkg_resources.resource_stream("src", "logging.yml") as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = "./logs"

# make the directory if it does not exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# create a configuration for the log file
config["handlers"]["file"]["filename"] = os.path.join(log_dir, "aragorn.log")
log_level = os.getenv("LOG_LEVEL", "DEBUG")
config["handlers"]["console"]["level"] = log_level
config["handlers"]["file"]["level"] = log_level
config["loggers"]["src"]["level"] = log_level
#config["loggers"]["aio_pika"]["level"] = log_level

# load the log config
logging.config.dictConfig(config)

# create a logger
logger = logging.getLogger(__name__)

# Get rabbitmq channel pool
channel_pool = get_channel_pool()

# declare the directory where the async data files will exist
queue_file_dir = "./queue-files"

# make the directory if it does not exist
if not os.path.exists(queue_file_dir):
    os.makedirs(queue_file_dir)

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
@ARAGORN_APP.post("/asyncquery", tags=["ARAGORN"], response_model=AsyncQueryResponse)
async def async_query_handler(
    background_tasks: BackgroundTasks, request: PDAsyncQuery = default_request_async, answer_coalesce_type: MethodName = MethodName.all
):
    """
    Performs an asynchronous query operation which compiles data from numerous ARAGORN ranking agent services.
    The services are called in the following order, each passing their output to the next service as an input:

    Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """
    return await async_query(background_tasks, request, answer_coalesce_type, logger, "ARAGORN")


# synchronous entry point
@ARAGORN_APP.post("/query", tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request_sync, answer_coalesce_type: MethodName = MethodName.all):
    """Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
    The services are called in the following order, each passing their output to the next service as an input:

    Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """
    return await sync_query(request, answer_coalesce_type, logger, "ARAGORN")


@ARAGORN_APP.post("/callback/{guid}", tags=["ARAGORN"], include_in_schema=False)
async def subservice_callback(response: PDResponse, guid: str) -> int:
    """
    Receives asynchronous message requests from an ARAGORN subservice callback

    :param response:
    :param guid:
    :return:
    """
    # init the return html status code
    ret_val: int = 200

    logger.debug(f"{guid}: Receiving sub-service callback")
    # logger.debug(f'{guid}: The sub-service response: {response.json()}')

    try:
        async with channel_pool.acquire() as channel:
            await channel.get_queue(guid, ensure=True)

            # create a file path/name
            fname = "".join(random.choices(string.ascii_lowercase, k=12))
            file_name = f"{queue_file_dir}/{guid}-{fname}-async-data.json"

            # save the response data to a file
            with open(file_name, "w") as data_file:
                data_file.write(response.json())

            # publish what was received for the sub-service. post the file name for the queue handler
            publish_val = await channel.default_exchange.publish(aio_pika.Message(body=file_name.encode()), routing_key=guid)

            if publish_val:
                logger.debug(f"{guid}: Callback message published to queue.")
            else:
                logger.error(f"{guid}: Callback message publishing to queue failed, type: {type(publish_val)}")
            # if isinstance(publish_val, spec.Basic.Ack):
            #    logger.info(f'{guid}: Callback message published to queue.')
            # else:
            #    # set the html error code
            #    ret_val = 422
            #    logger.error(f'{guid}: Callback message publishing to queue failed, type: {type(publish_val)}')

    except Exception as e:
        logger.exception(f"Exception detected while handling sub-service callback using guid {guid}", e)
        # set the html status code
        ret_val = 500

    # return the response code
    return ret_val


@ARAGORN_APP.post("/aragorn_callback", tags=["ARAGORN"], include_in_schema=False)
async def receive_aragorn_async_response(response: PDResponse) -> int:
    """
    An endpoint for receiving the aragorn callback results. normally used in
    debug mode to verify the round trip insuring that the data is viable to a real client.
    """
    if hasattr(response, "pid"):
        pid = response.pid
    else:
        pid = "no PID"

    # save it to the log
    logger.debug(f"{pid}: ARAGORN async callback received.")

    # get the response in a dict
    # result = response.json()
    # logger.debug(f'{pid}: ARAGORN callback received. message {result}')

    # return the response code
    return 200

@ARAGORN_APP.get("/asyncquery_status/{job_id}", response_model=AsyncQueryStatusResponse, tags=["ARAGORN"], status_code=200)
async def status_query_handler(job_id: str):
    """Checks the status of an asynchronous query operation."""
    return await status_query(job_id)


ARAGORN_APP.openapi_schema = construct_open_api_schema(ARAGORN_APP, prefix="aragorn", description="ARAGORN: A fully-federated Translator ARA.")
