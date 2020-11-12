"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
from enum import Enum
from functools import wraps
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Request, Message
from src.one_hops import one_hop

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

# delacre the log directory
log_dir = 'logs'

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
    title='ARAGORN One Hop',
    version='0.0.1',
    description='Performs a one-hop operation which spans numerous ARAGORN services.'
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
    graph = "graph"
    none = "none"
    ontology = "ontology"
    property = "property"

# declare the one and only entry point
@APP.post('/aragorn', response_model=Message, response_model_exclude_none=True)
async def one_hop_handler(request: Request, answer_coalesce: MethodName) -> Message:
    """ Aragorn one-hop operations. """

    # convert the incoming message into a dict
    message = request.dict()

    # call to process the input
    one_hopped: dict = one_hop(message, answer_coalesce)

    # return the answer
    return Message(**one_hopped)


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
