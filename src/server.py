"""aragorn one hop server."""
import os
import logging.config
import pkg_resources
import yaml
from functools import wraps
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Request, Message
from src.one_hops import one_hop

# Set up default logger.
with pkg_resources.resource_stream('src', 'logging.yml') as f:
    config = yaml.safe_load(f.read())

log_dir = 'logs'

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

config['handlers']['file']['filename'] = os.path.join(log_dir, 'aragorn.log')

logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

APP = FastAPI(
    title='ARAGORN One Hop',
    version='0.0.1',
    description='Performs a one-hop operation which spans the usage of numerous ARAGORN services.'
)

APP.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@APP.post('/aragorn')
async def one_hop_handler(request: Request) -> Message:
    """ aragorn one-hop operations. """

    # convert the incoming message into a dict
    message = request.dict()

    # call to process the input
    one_hopped = one_hop(message)

    # return the answer
    return Message(**one_hopped)


def log_exception(method):
    """Wrap method."""
    @wraps(method)
    async def wrapper(*args, **kwargs):
        """Log exception encountered in method, then pass."""
        try:
            return await method(*args, **kwargs)
        except Exception as err:
            logger.exception(err)
            raise Exception(err)

    return wrapper
