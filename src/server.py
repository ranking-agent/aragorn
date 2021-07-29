"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml

from datetime import datetime
from enum import Enum
from functools import wraps
from reasoner_pydantic import Response as PDResponse
from src.service_aggregator import entry
from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

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

# declare the FastAPI details
APP = FastAPI(
    title='ARAGORN',
    version='1.1.1'
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


# default_input: dict = {
#   "message": {
#     "query_graph": {
#       "nodes": {
#         "n0": {
#           "id": "MONDO:0004979",
#           "categories": "biolink:Disease"
#         },
#         "n1": {
#           "categories": "biolink:ChemicalSubstance"
#         }
#       },
#       "edges": {
#         "e01": {
#           "subject": "n0",
#           "object": "n1",
#           "predicates": "biolink:correlated_with"
#         }
#       }
#     }
#   }
# }

default_input: dict = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": [
                        "biolink:PhenotypicFeature"
                    ]
                },
                "n1": {
                    "ids": [
                        "HGNC:6284"
                    ],
		    "categories":["biolink:Gene"]
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


# declare the one and only entry point
@APP.post('/query', tags=["ARAGORN"], response_model=PDResponse, response_model_exclude_none=True, status_code=200)
async def query_handler(request: PDResponse = default_request, answer_coalesce_type: MethodName = MethodName.all):
    """ Performs a query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score"""

    # convert the incoming message into a dict
    if type(request) is dict:
        message = request
    else:
        message = request.dict()

    if 'logs' not in message or message['logs'] is None:
        message['logs'] = []

    query_result = message

    try:
        # call to process the input
        query_result, status_code = entry(message, answer_coalesce_type)

        # validate the result
        final_msg = jsonable_encoder(PDResponse(**query_result))
    except Exception as e:
        # put the error in the response
        status_code = 500
        query_result['logs'].append(create_log_entry(f'Exception {str(e)}', "ERROR"))
        final_msg = query_result

    # return the result
    return JSONResponse(content=final_msg, status_code=status_code)


def create_log_entry(msg: str, err_level, code=None) -> dict:
    """
    Creates a trapi log message

    :param msg:
    :param err_level:
    :param code:
    :return: dict of the data passed
    """
    # load the data
    ret_val = {
        'timestamp': str(datetime.now()),
        'level': err_level,
        'message': msg,
        'code': code
    }

    # return to the caller
    return ret_val


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
        version='1.1.1',
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

    if servers_conf:
        for s in servers_conf:
            s['url'] = s['url'] + '/1.1'

        open_api_schema["servers"] = servers_conf

    return open_api_schema


APP.openapi_schema = construct_open_api_schema()
