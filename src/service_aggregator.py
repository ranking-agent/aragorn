"""Literature co-occurrence support."""
import logging
import requests
import os
import asyncio
from requests.exceptions import ConnectionError
from functools import partial
from src.util import create_log_entry
from datetime import datetime
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# A place to transmit results from async queries
# it's a dict from ids to async result queues
queues = {}


async def entry(message, guid, coalesce_type='all') -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of a TRAPI Message
    :param guid:
    :param coalesce_type: what kind of answer coalesce type should be performed
    :return: the result of the request and the status code
    """

    # A map from operations advertised in our x-trapi to functions
    # This is to functions rather than e.g. service urls because we may combine multiple calls into one op.
    #  e.g. our score operation will include both weighting and scoring for now.
    # Also gives us a place to handle function specific logic
    known_operations = {'lookup': strider,
                        'enrich_results': partial(answercoalesce, coalesce_type=coalesce_type),
                        'overlay_connect_knodes': omnicorp,
                        'score': score}

    # if the workflow is defined in the message use it, otherwise use the default aragorn workflow
    if 'workflow' in message and not (message['workflow'] is None):
        workflow_def = message['workflow']

        # The underlying tools (strider) don't want the workflow element and will 400
        del message['workflow']
    else:
        workflow_def = [{'id': 'lookup'}, {'id': 'enrich_results'}, {'id': 'overlay_connect_knodes'}, {'id': 'score'}]

    # convert the workflow def into function calls.
    # Raise a 422 if we find one we don't actually know how to do.
    # We told the world what we can do!
    # Workflow will be a list of the functions, and the parameters if there are any
    workflow = []

    for op in workflow_def:
        try:
            workflow.append((known_operations[op['id']], op.get('parameters', {})))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = await run_workflow(message, workflow, guid)

    # return the workflow def so that the caller can see what we did
    final_answer['workflow'] = workflow_def

    # return the answer
    return final_answer, status_code


async def post_async(host_url, query, guid, params=None):
    """
    Post an asynchronous message.

    Note: this method can return either a "request.models.Response " or a "reasoner-pydantic.message.response"

    :param host_url:
    :param query:
    :param guid:
    :param params:
    :return:
    """
    queues[guid] = asyncio.Queue()

    # get the server root path
    callback_host = os.environ.get('SERVER_ROOT', '/')

    # set the callback host in the query
    query['callback'] = f'{callback_host}/callback/{guid}'

    # set the debug level
    # TODO: make sure other aragorn friends do this too
    query['log_level'] = 'DEBUG'

    # make sure there is a place for the trapi log messages
    if 'logs' not in query:
        query['logs'] = []

    # Send the query, using the pid for the callback
    if params is None:
        post_response = requests.post(host_url, json=query)
    else:
        post_response = requests.post(host_url, json=query, params=params)

    # we could get an error posting the query. if there is
    # this will return a <requests.models.Response> type
    if post_response.status_code != 200:
        return post_response

    try:
        # wait for the callback. if this was successful it will
        # return a 'reasoner-pydantic' response type
        response = await queues[guid].get()

    except Exception as e:
        error_string = f'Queue error exception {e} for callback {query["callback"]}'
        logger.exception(error_string)
        raise HTTPException(500, error_string)

    # if we got this far make it a good callback
    response.status_code = 200

    # remove the item from the queue
    del queues[guid]

    # return with the message
    return response


async def post(name, url, message, guid, asyncquery=False, params=None) -> (dict, int):
    """
    launches a post request, returns the response.

    :param name: name of service
    :param url: the url of the service
    :param message: the message to post to the service
    :param guid: run identifier
    :param asyncquery:
    :param params: the parameters passed to the service
    :return: dict, status code
    """
    # init return values
    ret_val = message

    # are we goign to include the timings
    debug = os.environ.get('DEBUG_TIMING', 'False')

    # if we are capturing the timings
    if debug == 'True':
        dt_start = datetime.now()
    else:
        dt_start = None

    # remove the workflow element
    if 'workflow' in message and message['workflow'] is None:
        del message['workflow']

    logger.debug(f"{guid}: Calling {url}")

    try:
        # launch the post depending on the query type and get the response
        if asyncquery:
            # note there are two possible "Response" types that post_async() can return.
            # if it is of type "request.models.Response" it is most likely an HTML error.
            # else, it is a "reasoner-pydantic.message.Response" that contains a trapi message.
            response = await post_async(url, message, guid, params)

            # save the response code
            status_code = response.status_code

            # if this is a trapi message get the dict of it. it doesnt have a .json() method
            if str(response.__class__).find("reasoner") > -1:
                response = response.dict()
        else:
            if params is None:
                response = requests.post(url, json=message)
            else:
                response = requests.post(url, json=message, params=params)

            # save the response code
            status_code = response.status_code

        logger.debug(f'{guid}: {name} returned with {status_code}')

        if status_code == 200:
            try:
                # this could be a dict if it was an async call
                if isinstance(response, dict) and len(response) > 0:
                    ret_val = response
                # if there is a response return it as a dict
                elif len(response.json()):
                    ret_val = response.json()

            except Exception as e:
                status_code = 500
                logger.exception(f"{guid}: ARAGORN Exception {e} translating json from post to {name}")

    except ConnectionError as ce:
        status_code = 404
        logger.exception(f'{guid}: ARAGORN ConnectionError {ce} posting to {name}')
    except Exception as e:
        status_code = 500
        logger.exception(f"{guid}: ARAGORN Exception {e} posting to {name}")

    # make sure there is a place for the trapi log messages
    if 'logs' not in ret_val:
        ret_val['logs'] = []

    # html error code returned
    if status_code != 200:
        error_string = f'{guid}: {name} HTML error status code {status_code} returned.'

        logger.error(error_string)

        ret_val['logs'].append(create_log_entry(error_string, "ERROR"))
    # good html status code
    elif len(ret_val['message']['results']) == 0:
        logger.error(f'{guid}: {name} No results returned.')

        ret_val['logs'].append(create_log_entry(f'warning: empty returned', "WARNING"))
    else:
        logger.info(f'{guid}: {name} returned {len(ret_val["message"]["results"])} results.')

    if debug == 'True':
        diff = datetime.now() - dt_start

        ret_val['logs'].append(create_log_entry(f'{guid}: End of {name} processing. Time elapsed: {diff.seconds} seconds', 'DEBUG'))

    return ret_val, status_code


async def strider(message, params, guid) -> (dict, int):
    """
    Calls strider

    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = 'https://strider.renci.org/1.2/'

    # select the type of query post. "test" will come from the tester
    if 'test' in message:
        url += 'query'
        asyncquery = False
    else:
        url += 'asyncquery'
        asyncquery = True

    response = await post('strider', url, message, guid, asyncquery=asyncquery)

    return response


async def answercoalesce(message, params, guid, coalesce_type='all') -> (dict, int):
    """
    Calls answercoalesce
    :param message:
    :param params:
    :param guid:
    :param coalesce_type:
    :return:
    """
    url = f'https://answercoalesce.renci.org/1.2/coalesce/{coalesce_type}'

    return await post('answer_coalesce', url, message, guid)


async def omnicorp(message, params, guid) -> (dict, int):
    """
    Calls omnicorp
    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = 'https://aragorn-ranker.renci.org/1.2/omnicorp_overlay'

    return await post('omnicorp', url, message, guid)


async def score(message, params, guid) -> (dict, int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :param params:
    :param guid:
    :return:
    """
    weight_url = 'https://aragorn-ranker.renci.org/1.2/weight_correctness'

    score_url = 'https://aragorn-ranker.renci.org/1.2/score'

    message, status_code = await post('weight', weight_url, message, guid)

    return await post('score', score_url, message, guid)


async def run_workflow(message, workflow, guid) -> (dict, int):
    """

    :param message:
    :param workflow:
    :param guid:
    :return:
    """
    logger.debug(f'{guid}: {message}')

    status_code = None

    for operator_function, params in workflow:
        message, status_code = await operator_function(message, params, guid)

        if status_code != 200 or 'results' not in message['message']:
            break
        elif len(message['message']['results']) == 0:
            break

        # loop through all the log entries and fix the timestamps
        if 'logs' in message:
            for item in message['logs']:
                item['timestamp'] = str(item['timestamp'])

    # return the requested data
    return message, status_code


def one_hop_message(curie_a, type_a, type_b, edge_type, reverse=False) -> dict:
    """
    Creates a tests message.
    :param curie_a:
    :param type_a:
    :param type_b:
    :param edge_type:
    :param reverse:
    :return:
    """
    query_graph = {
                    "nodes": [
                        {
                            "id": "a",
                            "type": type_a,
                            "curie": curie_a
                        },
                        {
                            "id": "b",
                            "type": type_b
                        }
                    ],
                    "edges": [
                        {
                            "id": "ab",
                            "source_id": "a",
                            "target_id": "b"
                        }
                    ]
                }

    if edge_type is not None:
        query_graph['edges'][0]['type'] = edge_type

        if reverse:
            query_graph['edges'][0]['source_id'] = 'b'
            query_graph['edges'][0]['target_id'] = 'a'

    message = {
                "message":
                {
                    "query_graph": query_graph,
                    'knowledge_graph': {"nodes": [], "edges": []},
                    'results': []
                }
            }
    return message
