"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
import httpx
import requests

from uuid import uuid4
from enum import Enum
from functools import wraps
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse
from pydantic import BaseModel
from src.service_aggregator import queues, entry
from src.util import create_log_entry
from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

# declare the log directory
log_dir = './logs'

# set the app version
APP_VERSION = '2.0.13'

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


default_input: dict = {
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

# define the default request body
default_request: Body = Body(default=default_input)


# Why isn't there a pydantic model for this?
class AsyncReturn(BaseModel):
    description: str


# async entry point
@APP.post('/asyncquery', tags=["ARAGORN"], response_model=AsyncReturn)
async def async_query_handler(background_tasks: BackgroundTasks, request: PDAsyncQuery = default_request, answer_coalesce_type: MethodName = MethodName.all):
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

        logger.info(f'{guid}: async message call. ARAGORN callback URL is: {callback_url}')

    except KeyError as e:
        logger.error(f'{guid}: async message call Error {e}. callback URL was not specified')
        return JSONResponse(content={"description": "callback URL missing"}, status_code=422)
    except ValueError as e:
        logger.error(f'{guid}: async message call. Error {e} callback URL was empty')
        return JSONResponse(content={"description": "callback URL empty"}, status_code=422)

    background_tasks.add_task(execute_with_callback, message, answer_coalesce_type, callback_url, guid)

    return JSONResponse(content={"description": f"Query commenced. Will send result to {callback_url}"}, status_code=200)


# synchronous entry point
@APP.post('/query', tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def sync_query_handler(request: PDQuery = default_request, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid)

    return JSONResponse(content=final_msg, status_code=status_code)


@APP.post("/callback/{pid}", tags=["ARAGORN"])
async def subservice_callback(response: PDResponse,  pid: str) -> int:
    """
    Receives asynchronous message requests made by ARAGORN.
    """
    # pid indicates the query that we sent, put the response somewhere that the caller can find it
    if pid not in queues:
        logger.error(f'{pid} not found in queues')
        logger.debug(f'{len(queues)} valid pids are:')

        for x in queues:
            logger.debug(x)

        return 404

    await queues[pid].put(response)

    return 200


@APP.post("/aragorn_callback", tags=["ARAGORN"], include_in_schema=False)
async def receive_aragorn_async_response(response: PDResponse) -> int:
    """
    An endpoint for receiving the aragorn callback results normally used in
    debug mode to verify the round trip insuring that the data is viable to a real client.
    """
    logger.info('ARAGORN callback received.')

    # get the response in a dict
    result = response.json()

    # save it to the log
    logger.debug(result)

    # return the response
    return 200


async def execute_with_callback(request, answer_coalesce_type, callback_url, guid):
    """
    Executes an asynchronous ARAGORN rewuset

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

    # make the asynchronous request
    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid)

    logger.info(f'{guid}: handling callback({callback_url})')

    # for some reason the "mock" test endpoint doesnt like the async client post
    if test_mode:
        callback(callback_url, final_msg, guid)
    else:
        # send back the result to the specified aragorn callback end point
        async with httpx.AsyncClient() as client:
            await client.post(callback_url, json=final_msg)


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
    message['logs'].append(create_log_entry(f'PID: {guid}', "INFO"))

    query_result = message

    try:
        # call to process the input
        query_result, status_code = await entry(message, guid, answer_coalesce_type)

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

        final_msg = query_result

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

    logger.info(f'{guid}: handling callback({callback_url})')

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
        open_api_schema["servers"] = servers_conf

    return open_api_schema


APP.openapi_schema = construct_open_api_schema()
