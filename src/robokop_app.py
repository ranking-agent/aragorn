"""ROBOKOP"""
import os
import logging.config
import pkg_resources
import yaml

from pamqp import specification as spec
from uuid import uuid4
from enum import Enum
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse
from pydantic import BaseModel

from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from src.openapi_constructor import construct_open_api_schema
from src.common import asyncexecute, execute_with_callback

# declare the FastAPI details
ROBOKOP_APP = FastAPI(
    title='ROBOKOP'
)

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = './logs'

## Gets made by aragorn_app
## make the directory if it does not exist
#if not os.path.exists(log_dir):
#    os.makedirs(log_dir)

# create a configuration for the log file
config['handlers']['file']['filename'] = os.path.join(log_dir, 'robokop.log')

# load the log config
logging.config.dictConfig(config)

# create a logger
logger = logging.getLogger(__name__)

## ROBOKOP should not need to make async calls
## declare the directory where the async data files will exist
#queue_file_dir = './queue-files'
#
## make the directory if it does not exist
#if not os.path.exists(queue_file_dir):
#    os.makedirs(queue_file_dir)

# declare the types of answer coalesce methods
class MethodName(str, Enum):
    all = 'all'
    none = 'none'
    graph = "graph"
    ontology = "ontology"
    property = "property"


default_input_sync: dict = {
    "message": {
        "query_graph": {
            "edges": {
                "e01": {
                    "object": "n0",
                    "subject": "n1",
                    "predicates": [
                        "biolink:entity_negatively_regulates_entity"
                    ]
                }
            },
            "nodes": {
                "n0": {
                    "ids": [
                        "NCBIGene:23221"
                    ],
                    "categories": [
                        "biolink:Gene"
                    ]
                },
                "n1": {
                    "categories": [
                        "biolink:Gene"
                    ]
                }
            }
        }
    }
}

default_input_async: dict = {
    "callback": "https://aragorn.renci.org/1.2/aragorn_callback",
    "message": {
        "query_graph": {
            "edges": {
                "e01": {
                    "object": "n0",
                    "subject": "n1",
                    "predicates": [
                        "biolink:entity_negatively_regulates_entity"
                    ]
                }
            },
            "nodes": {
                "n0": {
                    "ids": [
                        "NCBIGene:23221"
                    ],
                    "categories": [
                        "biolink:Gene"
                    ]
                },
                "n1": {
                    "categories": [
                        "biolink:Gene"
                    ]
                }
            }
        }
    }
}

# define the default request bodies
default_request_sync: Body = Body(default=default_input_sync)
default_request_async: Body = Body(default=default_input_async, example=default_input_async)

# get the queue connection params
q_username = os.environ.get('QUEUE_USER', 'guest')
q_password = os.environ.get('QUEUE_PW', 'guest')
q_host = os.environ.get('QUEUE_HOST', '127.0.0.1')


# Create a async class
class AsyncReturn(BaseModel):
    description: str


# async entry point
@ROBOKOP_APP.post('/asyncquery', tags=["ROBOKOP"], response_model=AsyncReturn)
async def async_query_handler(background_tasks: BackgroundTasks, request: PDAsyncQuery = default_request_async, answer_coalesce_type: MethodName = MethodName.all):
    """
        Performs an asynchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """
    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    try:
        # convert the incoming message into a dict
        if isinstance(request, dict):
            message = request
        else:
            message = request.dict()

        callback_url = message['callback']

        # remove this as it will be used again when calling sub-processes (like strider)
        del message['callback']

        if len(callback_url) == 0:
            raise ValueError('callback URL empty')

        logger.info(f'{guid}: Async query requested. ARAGORN callback URL is: {callback_url}')

    except KeyError as e:
        logger.error(f'{guid}: Async message call key error {e}, callback URL was not specified')
        return JSONResponse(content={"description": "callback URL missing"}, status_code=422)
    except ValueError as e:
        logger.error(f'{guid}: Async message call value error {e}, callback URL was empty')
        return JSONResponse(content={"description": "callback URL empty"}, status_code=422)

    # launch the process
    background_tasks.add_task(execute_with_callback, message, answer_coalesce_type, callback_url, guid, logger)

    # package up the response and return it
    return JSONResponse(content={"description": f"Query commenced. Will send result to {callback_url}"}, status_code=200)


# synchronous entry point
@ROBOKOP_APP.post('/query', tags=["ROBOKOP"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request_sync, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    logger.info(f'{guid}: Sync query requested.')

    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid, logger)

    logger.info(f'{guid}: Sync query returning.')

    return JSONResponse(content=final_msg, status_code=status_code)


ROBOKOP_APP.openapi_schema = construct_open_api_schema(ROBOKOP_APP, prefix="robokop",description="ROBOKOP: A non-federated ARA",infores="infores:robokop")
