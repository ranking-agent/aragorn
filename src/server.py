"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
from fastapi.openapi.utils import get_openapi
from enum import Enum
from functools import wraps
from fastapi import Body, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Response
from src.service_aggregator import entry

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
    version='0.0.1',
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
    none = 'none'
    graph = "graph"
    ontology = "ontology"
    property = "property"


default_input: dict = {
  "message": {
    "query_graph": {
      "nodes": {
        "n0": {
          "id": "MONDO:0004979",
          "category": "biolink:Disease"
        },
        "n1": {
          "category": "biolink:ChemicalSubstance"
        }
      },
      "edges": {
        "e01": {
          "subject": "n0",
          "object": "n1",
          "predicate": "biolink:correlated_with"
        }
      }
    }
  }
}

# define the default request body
default_request: Body = Body(default=default_input)


# declare the one and only entry point
@APP.post('/query', tags=["ARAGORN"], response_model=Response, response_model_exclude_none=True, status_code=200)
async def query_handler(response: Response = default_request, answer_coalesce_type: MethodName = MethodName.graph) -> Response:
    """ Performs a query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score"""

    # convert the incoming message into a dict
    if type(response) is dict:
        message = response
    else:
        message = response.dict()

    # call to process the input
    query_result: dict = entry(message, answer_coalesce_type)

    # if there was an error detected make sure the response status shows it
    if query_result is None:
        message['error'] = 'Error. Nothing returned from call.'
        if type(response) is not dict:
            response.status_code = 500
    elif query_result.get('error') is not None:
        final_msg = query_result
        if type(response) is not dict:
            response.status_code = 500
    else:
        final_msg = query_result

    # return the answer
    return Response(**final_msg)


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
        version='0.0.2',
        routes=APP.routes
    )

    open_api_extended_file_path = os.path.join(os.path.dirname(__file__), '../openapi-config.yaml')

    with open(open_api_extended_file_path) as open_api_file:
        open_api_extended_spec = yaml.load(open_api_file, Loader=yaml.SafeLoader)

    x_translator_extension = open_api_extended_spec.get("x-translator")
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

    if contact_config:
        open_api_schema["info"]["contact"] = contact_config

    if terms_of_service:
        open_api_schema["info"]["termsOfService"] = terms_of_service

    if description:
        open_api_schema["info"]["description"] = description

    if title_override:
        open_api_schema["info"]["title"] = title_override

    if servers_conf:
        open_api_schema["servers"] = servers_conf

    return open_api_schema

# note: this must be commented out for local debugging
APP.openapi_schema = construct_open_api_schema()
