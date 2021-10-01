"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
import requests
import uuid

from src.util import create_log_entry
from enum import Enum
from functools import wraps
from reasoner_pydantic import Query as PDQuery, AsyncQuery as PDAsyncQuery, Response as PDResponse
from src.service_aggregator import entry
from pydantic import BaseModel

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
APP_VERSION = '2.0.12'

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
            "nodes": {
                "n0": {
                    "categories": [
                        "biolink: PhenotypicFeature"
                    ]
                },
                "n1": {
                    "ids": [
                        "HGNC:6284"
                    ],
                "categories": ["biolink:Gene"]
                }
            },
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1"
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

#async entry point
@APP.post('/asyncquery', tags=["ARAGORN"], response_model=AsyncReturn)
async def asquery_handler(request: PDAsyncQuery,  background_tasks: BackgroundTasks, answer_coalesce_type: MethodName = MethodName.all):
    # create a guid that will be used for tagging the log entries
    guid = str(uuid.uuid4()).split('-')[-1]

    try:
        # convert the incoming message into a dict
        if isinstance(request, dict):
            message = request
        else:
            message = request.dict()

        callback_url = message['callback']

        if len(callback_url) == 0:
            raise ValueError('callback URL empty')

    except KeyError as e:
        logger.error(f'{guid}: async message call. callback URL was not specified')
        return JSONResponse(content={"description": "callback URL missing"}, status_code=422)
    except ValueError as e:
        logger.error(f'{guid}: async message call. callback URL was empty')
        return JSONResponse(content={"description": "callback URL empty"}, status_code=422)

    background_tasks.add_task(execute_with_callback, message, answer_coalesce_type, callback_url, guid)

    return JSONResponse(content={"description": f"Query commenced. Will send result to {callback_url}"}, status_code=200)

def execute_with_callback(request, answer_coalesece_type, callback_url, guid):
    #Go off and run the query, and when done, post it back to the callback
    del request['callback']

    final_msg, status_code = execute(request, answer_coalesece_type, guid)

    callback(callback_url, final_msg, guid)

#This is pulled out to make it easy to mock without interfering with other posts.
def callback(callback_url, final_msg, guid):

    logger.info(f'{guid}: handling callback({callback_url})')

    requests.post(callback_url,json=final_msg)

# synchronous entry point
@APP.post('/query', tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def query_handler(request: PDQuery = default_request, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score"""

    # create a guid that will be used for tagging the log entries
    guid = str(uuid.uuid4()).split('-')[-1]

    final_msg, status_code = await asyncexecute(request,answer_coalesce_type, guid)
    return JSONResponse(content=final_msg, status_code=status_code)

async def asyncexecute(request,answer_coalesce_type, guid):
    return execute(request,answer_coalesce_type, guid)

def execute(request, answer_coalesce_type, guid):
    # convert the incoming message into a dict
    if type(request) is dict:
        message = request
    else:
        message = request.dict(exclude_unset=True)

    if 'logs' not in message or message['logs'] is None:
        message['logs'] = []

    query_result = message

    try:
        # call to process the input
        query_result, status_code = entry(message, guid, answer_coalesce_type)

        if status_code != 200:
            return query_result,status_code

        query_result['status'] = 'Success'

        #Clean up: this should be cleaned up as the components move to 1.2, but for now, let's clean it up
        for edge_id,edge_data in query_result['message']['knowledge_graph']['edges'].items():
            if 'relation' in edge_data:
                del edge_data['relation']

        #This is also bogus, not sure why it's not validating
        del query_result['workflow']

        # validate the result
        final_msg = query_result
    except Exception as e:
        # put the error in the response
        status_code = 500
        logger.exception(f'{guid}: Exception {e} in execute()')
        query_result['logs'].append(create_log_entry(f'Exception {str(e)}', "ERROR"))
        final_msg = query_result

    return final_msg, status_code


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
