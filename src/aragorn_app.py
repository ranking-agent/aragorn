"""aragorn server."""
import os
import logging.config
import pkg_resources
import yaml
import aio_pika

from pamqp import specification as spec
from uuid import uuid4
from enum import Enum
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse
from pydantic import BaseModel
from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from src.openapi_constructor import construct_open_api_schema
from src.common import execute_with_callback, asyncexecute


# declare the FastAPI details
ARAGORN_APP = FastAPI(
    title='ARAGORN'
)

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = './logs'

# make the directory if it does not exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# create a configuration for the log file
config['handlers']['file']['filename'] = os.path.join(log_dir, 'aragorn.log')

# load the log config
logging.config.dictConfig(config)

# create a logger
logger = logging.getLogger(__name__)

# declare the directory where the async data files will exist
queue_file_dir = './queue-files'

# make the directory if it does not exist
if not os.path.exists(queue_file_dir):
    os.makedirs(queue_file_dir)

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
@ARAGORN_APP.post('/asyncquery', tags=["ARAGORN"], response_model=AsyncReturn)
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
@ARAGORN_APP.post('/query', tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request_sync, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    logger.info(f'{guid}: Sync query requested.')

    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid,logger)

    logger.info(f'{guid}: Sync query returning.')

    return JSONResponse(content=final_msg, status_code=status_code)


@ARAGORN_APP.post("/callback/{guid}", tags=["ARAGORN"], include_in_schema=False)
async def subservice_callback(response: PDResponse,  guid: str) -> int:
    """
    Receives asynchronous message requests from an ARAGORN subservice callback

    :param response:
    :param guid:
    :return:
    """
    # init the return html status code
    ret_val: int = 200

    logger.info(f'{guid}: Receiving sub-service callback')
    # logger.debug(f'{guid}: The sub-service response: {response.json()}')

    # init the connection
    connection = None

    try:
        # create a connection to the queue
        connection = await aio_pika.connect_robust(f"amqp://{q_username}:{q_password}@{q_host}/")

        # with the connection post to the queue
        async with connection:
            # get a channel to the queue
            channel = await connection.channel()

            # create a file path/name
            file_name = f'{queue_file_dir}/{guid}-async-data.json'

            # save the response data to a file
            with open(file_name, 'w') as data_file:
                data_file.write(response.json())

            # publish what was received for the sub-service. post the file name for the queue handler
            publish_val = await channel.default_exchange.publish(aio_pika.Message(body=file_name.encode()), routing_key=guid)

            if isinstance(publish_val, spec.Basic.Ack):
                logger.info(f'{guid}: Callback message published to queue.')
            else:
                # set the html error code
                ret_val = 422

                logger.error(f'{guid}: Callback message publishing to queue failed, type: {type(publish_val)}')

    except Exception as e:
        logger.exception(f'Exception detected while handling sub-service callback using guid {guid}', e)

        # set the html status code
        ret_val = 500
    finally:
        # close the connection to the queue if it exists
        if connection:
            await connection.close()

    # return the response code
    return ret_val


@ARAGORN_APP.post("/aragorn_callback", tags=["ARAGORN"], include_in_schema=False)
async def receive_aragorn_async_response(response: PDResponse) -> int:
    """
    An endpoint for receiving the aragorn callback results. normally used in
    debug mode to verify the round trip insuring that the data is viable to a real client.
    """
    if hasattr(response, 'pid'):
        pid = response.pid
    else:
        pid = 'no PID'

    # save it to the log
    logger.debug(f'{pid}: ARAGORN async callback received.')

    # get the response in a dict
    # result = response.json()
    # logger.debug(f'{pid}: ARAGORN callback received. message {result}')

    # return the response code
    return 200

ARAGORN_APP.openapi_schema = construct_open_api_schema(ARAGORN_APP, description="ARAGORN: A fully-federated Translator ARA.")
