import httpx
import requests
from functools import wraps
from src.service_aggregator import entry
from src.util import create_log_entry

async def execute_with_callback(request, answer_coalesce_type, callback_url, guid,logger):
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


async def asyncexecute(request, answer_coalesce_type, guid, logger):
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
