import httpx
import requests
from functools import wraps
from uuid import uuid4
from fastapi.responses import JSONResponse
from src.service_aggregator import entry
from src.util import create_log_entry

async def async_query(background_tasks, request, answer_coalesce_type, logger, caller='ARAGORN'):
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

        logger.info(f'{guid}: Async query requested. {caller} callback URL is: {callback_url}')

    except KeyError as e:
        logger.error(f'{guid}: Async message call key error {e}, callback URL was not specified')
        return JSONResponse(content={"description": "callback URL missing"}, status_code=422)
    except ValueError as e:
        logger.error(f'{guid}: Async message call value error {e}, callback URL was empty')
        return JSONResponse(content={"description": "callback URL empty"}, status_code=422)

    # launch the process
    background_tasks.add_task(execute_with_callback, message, answer_coalesce_type, callback_url, guid, logger, caller)

    # package up the response and return it
    return JSONResponse(content={"description": f"Query commenced. Will send result to {callback_url}"}, status_code=200)

async def sync_query(request, answer_coalesce_type, logger, caller = "ARAGORN"):
    """ Performs a synchronous query operation which compiles data from numerous ARAGORN ranking agent services.
        The services are called in the following order, each passing their output to the next service as an input:

        Strider -> (optional) Answer Coalesce -> ARAGORN-Ranker:omnicorp overlay -> ARAGORN-Ranker:weight correctness -> ARAGORN-Ranker:score
    """

    # create a guid that will be used for tagging the log entries
    guid = str(uuid4()).split('-')[-1]

    logger.info(f'{guid}: Sync query requested.')

    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid,logger, caller)

    logger.info(f'{guid}: Sync query returning.')

    return JSONResponse(content=final_msg, status_code=status_code)

async def execute_with_callback(request, answer_coalesce_type, callback_url, guid,logger, caller):
    """
    Executes an asynchronous ARAGORN/ROBOKOP request

    :param request:
    :param answer_coalesce_type:
    :param callback_url:
    :param guid:
    :param logger:
    :param caller:
    :return:
    """
    # capture if this is a test request
    if 'test' in request:
        test_mode = True
    else:
        test_mode = False

    logger.info(f'{guid}: Awaiting async execute with callback URL: {callback_url}')

    # make the asynchronous request
    final_msg, status_code = await asyncexecute(request, answer_coalesce_type, guid, logger, caller)

    # for some reason the "mock" test endpoint doesnt like the async client post
    if test_mode:
        callback(callback_url, final_msg, guid, caller)
    else:
        try:
            # send back the result to the specified aragorn callback end point
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=600.0)) as client:
                response = await client.post(callback_url, json=final_msg)
                logger.info(f'{guid}: Executed POST to callback URL {callback_url}, response: {response.status_code}')
        except Exception as e:
            logger.exception(f'{guid}: Exception detected: POSTing to callback {callback_url}', e)


async def asyncexecute(request, answer_coalesce_type, guid, logger, caller):
    """
    Launches an asynchronous ARAGORN run

    :param request:
    :param answer_coalesce_type:
    :param guid:
    :param logger:
    :param caller:
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
        query_result, status_code = await entry(message, guid, answer_coalesce_type, caller)

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


def callback(callback_url, final_msg, guid, logger):
    """
    This is pulled out for the tester.

    :param callback_url:
    :param final_msg:
    :param guid:
    :param logger:
    :return:
    """

    logger.info(f'{guid}: Handling async with callback with URL: {callback_url}')

    requests.post(callback_url, json=final_msg)


def log_exception(method,logger):
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
