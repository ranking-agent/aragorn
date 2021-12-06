"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
import httpx
import requests
import aio_pika

from pamqp import specification as spec
from uuid import uuid4
from enum import Enum
from functools import wraps
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse
from pydantic import BaseModel
from src.util import create_log_entry
from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from src.service_aggregator import entry

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = './logs'

# set the app version
APP_VERSION = '2.0.21'

# make the directory if it does not exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# create a configuration for the log file
config['handlers']['file']['filename'] = os.path.join(log_dir, 'aragorn.log')

# load the log config
logging.config.dictConfig(config)

# create a logger
logger = logging.getLogger(__name__)

# declare the FastAPI details
APP = FastAPI(
    title='ARAGORN',
    version=APP_VERSION
)


# declare app access details
APP.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
@APP.post('/asyncquery', tags=["ARAGORN"], response_model=AsyncReturn)
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
    background_tasks.add_task(execute_with_callback, message, answer_coalesce_type, callback_url, guid)

    # package up the response and return it
    return JSONResponse(content={"description": f"Query commenced. Will send result to {callback_url}"}, status_code=200)


# synchronous entry point
@APP.post('/query', tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request_sync, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    logger.info(f'{guid}: Sync query requested.')

    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid)

    logger.info(f'{guid}: Sync query returning.')

    return JSONResponse(content=final_msg, status_code=status_code)


@APP.post("/callback/{guid}", tags=["ARAGORN"], include_in_schema=False)
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

            # publish what was received for the sub-service
            publish_val = await channel.default_exchange.publish(aio_pika.Message(body=response.json().encode()), routing_key=guid)

            if isinstance(publish_val, spec.Basic.Ack):
                logger.info(f'{guid}: Callback message published to queue.')
            else:
                logger.error(f'{guid}: Callback message publishing to queue failed, type: {type(publish_val)}')

                # set the html error code
                ret_val = 422

    except Exception as e:
        logger.exception(f'Exception detected while handling sub-service callback using guid {guid}', e)

        # set the html status code
        ret_val = 500
    finally:
        # close the connection to the queue if it exists
        if connection:
            await connection.close()

    return ret_val


@APP.post("/aragorn_callback", tags=["ARAGORN"], include_in_schema=False)
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


async def execute_with_callback(request, answer_coalesce_type, callback_url, guid):
    """
    Executes an asynchronous ARAGORN request

    :param request:
    :param answer_coalesce_type:
    :param callback_url:
    :param guid:
    :return:
    """
    # capture if this is a test request
    if 'test' in request:
        test_mode = True
    else:
        test_mode = False

    logger.info(f'{guid}: Awaiting async execute with callback URL: {callback_url}')

    # make the asynchronous request
    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid)

    # for some reason the "mock" test endpoint doesnt like the async client post
    if test_mode:
        callback(callback_url, final_msg, guid)
    else:
        try:
            # send back the result to the specified aragorn callback end point
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=600.0)) as client:
                response = await client.post(callback_url, json=final_msg)
                logger.info(f'{guid}: Executed POST to callback URL {callback_url}, response: {response.status_code}')
        except Exception as e:
            logger.exception(f'{guid}: Exception detected: POSTing to callback {callback_url}', e)


async def asyncexecute(request, answer_coalesce_type, guid):
    """
    Launches an asynchronous ARAGORN run

    :param request:
    :param answer_coalesce_type:
    :param guid:
    :return:
    """
    # convert the incoming message into a dict
    if type(request) is dict:
        message = request
    else:
        message = request.dict(exclude_unset=True)

    if 'logs' not in message or message['logs'] is None:
        message['logs'] = []

    # add in a log entry for the pid
    message['logs'].append(create_log_entry(f'pid: {guid}', "INFO"))

    query_result = message

    try:
        # call to process the input
        query_result, status_code = await entry(message, guid, answer_coalesce_type)

        # return the bad result if necessary
        if status_code != 200:
            return query_result, status_code

        query_result['status'] = 'Success'

        # Clean up: this should be cleaned up as the components move to 1.2, but for now, let's clean it up
        for edge_id, edge_data in query_result['message']['knowledge_graph']['edges'].items():
            if 'relation' in edge_data:
                del edge_data['relation']

        # This is also bogus, not sure why it's not validating
        del query_result['workflow']

        # validate the result
        final_msg = query_result
    except Exception as e:
        # put the error in the response
        status_code = 500
        logger.exception(f'{guid}: Exception {e} in execute()')

        query_result['logs'].append(create_log_entry(f'Exception {str(e)}', "ERROR"))

        # set the response
        final_msg = query_result

    # save the guid
    final_msg['pid'] = guid

    return final_msg, status_code


def callback(callback_url, final_msg, guid):
    """
    This is pulled out for the tester.

    :param callback_url:
    :param final_msg:
    :param guid:
    :return:
    """

    logger.info(f'{guid}: Handling async with callback with URL: {callback_url}')

    requests.post(callback_url, json=final_msg)


def log_exception(method):
    """
    Wrap method.
    :param method:
    :return:
    """
    @wraps(method)
    async def wrapper(*args, **kwargs):
        """Log exception encountered in method, then pass."""
        try:
            return await method(*args, **kwargs)

        except Exception as err:
            logger.exception(err)
            raise Exception(err)

    return wrapper


def construct_open_api_schema():
    """
    This creates the Open api schema object

    :return:
    """

    if APP.openapi_schema:
        return APP.openapi_schema

    open_api_schema = get_openapi(
        title='ARAGORN',
        version=APP_VERSION,
        routes=APP.routes
    )

    open_api_extended_file_path = os.path.join(os.path.dirname(__file__), '../openapi-config.yaml')

    with open(open_api_extended_file_path) as open_api_file:
        open_api_extended_spec = yaml.load(open_api_file, Loader=yaml.SafeLoader)

    x_translator_extension = open_api_extended_spec.get("x-translator")
    x_trapi_extension = open_api_extended_spec.get("x-trapi")
    contact_config = open_api_extended_spec.get("contact")
    terms_of_service = open_api_extended_spec.get("termsOfService")
    servers_conf = open_api_extended_spec.get("servers")
    tags = open_api_extended_spec.get("tags")
    title_override = open_api_extended_spec.get("title") or 'ARAGORN'
    description = open_api_extended_spec.get("description")

    if tags:
        open_api_schema['tags'] = tags

    if x_translator_extension:
        # if x_translator_team is defined amends schema with x_translator extension
        open_api_schema["info"]["x-translator"] = x_translator_extension

    if x_trapi_extension:
        # if x_translator_team is defined amends schema with x_translator extension
        open_api_schema["info"]["x-trapi"] = x_trapi_extension

    if contact_config:
        open_api_schema["info"]["contact"] = contact_config

    if terms_of_service:
        open_api_schema["info"]["termsOfService"] = terms_of_service

    if description:
        open_api_schema["info"]["description"] = description

    if title_override:
        open_api_schema["info"]["title"] = title_override

    # adds support to override server root path
    server_root = os.environ.get('SERVER_ROOT', '/')

    # make sure not to add double slash at the end.
    server_root = server_root.rstrip('/') + '/'

    if servers_conf:
        for s in servers_conf:
            if s['description'].startswith('Default'):
                s['url'] = server_root + '1.2' if server_root != '/' else s['url']
                s['x-maturity'] = os.environ.get("MATURITY_VALUE", "maturity")
                s['x-location'] = os.environ.get("LOCATION_VALUE", "location")

        open_api_schema["servers"] = servers_conf

    return open_api_schema


APP.openapi_schema = construct_open_api_schema()
