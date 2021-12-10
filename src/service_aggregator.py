"""Literature co-occurrence support."""
import asyncio
import logging
import requests
import os
import aio_pika

from functools import partial
from src.util import create_log_entry
from src.operations import sort_results_score, filter_results_top_n, filter_kgraph_orphans, filter_message_top_n
from datetime import datetime
from fastapi import HTTPException
from requests.models import Response
from requests.exceptions import ConnectionError
from asyncio.exceptions import TimeoutError
from src.file_message_queue import FileMessageQueue
import time
import json
logger = logging.getLogger(__name__)


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
                        'score': score,
                        'sort_results_score': sort_results_score,
                        'filter_results_top_n': filter_results_top_n,
                        'filter_kgraph_orphans': filter_kgraph_orphans,
                        'filter_message_top_n': filter_message_top_n}

    # if the workflow is defined in the message use it, otherwise use the default aragorn workflow
    if 'workflow' in message and not (message['workflow'] is None):
        workflow_def = message['workflow']

        # The underlying tools (strider) don't want the workflow element and will 400
        del message['workflow']
    else:
        workflow_def = [{'id': 'lookup'},
                        {'id': 'enrich_results', 'parameters': {'max_input_size': 5000}},
                        {'id': 'overlay_connect_knodes'},
                        {'id': 'score'},
                        {'id': 'filter_message_top_n', 'parameters': {'max_results': 5000}}]

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
    # get the server root path
    callback_host = os.environ.get('CALLBACK_HOST', '/')

    # set the callback host in the query
    # TODO this should have the trapi endpoint in production
    query['callback'] = f'{callback_host}/callback/{guid}'

    # set the debug level
    # TODO: make sure other aragorn friends do this too
    # query['log_level'] = 'DEBUG'

    # make sure there is a place for the trapi log messages
    if 'logs' not in query:
        query['logs'] = []

    # Send the query, using the pid for the callback
    if params is None:
        post_response = requests.post(host_url, json=query)
    else:
        post_response = requests.post(host_url, json=query, params=params)

    # check the response status.
    if post_response.status_code != 200:
        # if there is an error this will return a <requests.models.Response> type
        return post_response

    file_queue = FileMessageQueue()
    try:
        time_out = 60*60*4
        try:
            data = await file_queue.subscribe(guid=guid, time_out=time_out)
        except asyncio.exceptions.TimeoutError as error:
            error_string = f'{guid}: Async query to {host_url} timed out'
            logging.error(error_string)
            response = Response()
            response.status_code = 598
            return response

        data = data.encode()
        response = Response()
        response.status_code = 200
        response._content = data
    #     # get the rabbitmq connection params
    #     q_username = os.environ.get('QUEUE_USER', 'guest')
    #     q_password = os.environ.get('QUEUE_PW', 'guest')
    #     q_host = os.environ.get('QUEUE_HOST', '127.0.0.1')
    #
    #     # get a connection to the rabbit mq server
    #     connection = await aio_pika.connect_robust(host=q_host, login=q_username, password=q_password)
    #
    #     # use the connection to create a queue using the guid
    #     async with connection:
    #         # create a channel to the rabbit mq
    #         channel = await connection.channel()
    #
    #         # declare the queue using the guid as the key
    #         queue = await channel.declare_queue(guid, auto_delete=True)
    #
    #         # wait for the response.  Timeout after 4 hours
    #         async with queue.iterator(timeout=60*60*4) as queue_iter:
    #             # wait the for the message
    #             async for message in queue_iter:
    #                 async with message.process():
    #                     response = Response()
    #                     response.status_code = 200
    #                     response._content = message.body
    #
    #                     break
    #
    #     await connection.close()
    #
    # except TimeoutError as e:
    #     error_string = f'{guid}: Async query to {host_url} timed out'
    #     response = Response()
    #     response.status_code = 598
    #     return response
    except Exception as e:
        error_string = f'{guid}: Queue error exception {e} for callback {query["callback"]}'
        logger.exception(error_string, e)
        raise HTTPException(500, error_string)
    finally:
        file_queue.clean_up_files(guid)
    # if we got this far make it a good callback
    response.status_code = 200

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

    # are we going to include the timings
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
            # handle the response
            response = await post_async(url, message, guid, params)
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
                # if there is a response return it as a dict
                if len(response.json()):
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
        msg = f'{name} HTML error status code {status_code} returned.'

        logger.error(f'{guid}: {msg}')

        ret_val['logs'].append(create_log_entry(msg, "ERROR"))
    # good html status code
    elif len(ret_val['message']['results']) == 0:
        msg = f'{name} No results returned.'

        logger.warning(f'{guid}: {msg}')

        ret_val['logs'].append(create_log_entry(msg, "WARNING"))
    else:
        logger.info(f'{guid}: {name} returned {len(ret_val["message"]["results"])} results.')

    if debug == 'True':
        diff = datetime.now() - dt_start

        ret_val['logs'].append(create_log_entry(f'End of {name} processing. Time elapsed: {diff.seconds} seconds', 'DEBUG'))

    return ret_val, status_code


async def strider(message, params, guid) -> (dict, int):
    """
    Calls strider

    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = os.environ.get("STRIDER_URL", "https://strider.renci.org/1.2/")

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
    url = f'{os.environ.get("ANSWER_COALESCE_URL", "https://answercoalesce.renci.org/1.2/coalesce/")}{coalesce_type}'

    # With the current answercoalesce, we make the result list longer, and frequently much longer.  If
    # we've already got 10s of thousands of results, let's skip this step...
    if 'max_input_size' in params:
        if len(message['message']['results']) > params['max_input_size']:
            # This is already too big, don't do anything else
            return message, 200
    return await post('answer_coalesce', url, message, guid)


async def omnicorp(message, params, guid) -> (dict, int):
    """
    Calls omnicorp
    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = f'{os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.2/")}omnicorp_overlay'

    return await post('omnicorp', url, message, guid)


async def score(message, params, guid) -> (dict, int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :param params:
    :param guid:
    :return:
    """
    ranker_url = os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.2/")

    weight_url = f'{ranker_url}weight_correctness'

    score_url = f'{ranker_url}score'

    message, status_code = await post('weight', weight_url, message, guid)

    return await post('score', score_url, message, guid)


async def run_workflow(message, workflow, guid) -> (dict, int):
    """

    :param message:
    :param workflow:
    :param guid:
    :return:
    """
    logger.debug(f'{guid}: incoming message: {message}')

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
