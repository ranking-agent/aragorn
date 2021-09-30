"""Literature co-occurrence support."""
import logging
import requests
from requests.exceptions import ConnectionError
from functools import partial
from src.util import create_log_entry
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def entry(message, guid, coalesce_type='all') -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of a TRAPI Message
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

    # convert the workflow def into function calls.   Raise a 422 if we find one we don't actually know how to do.
    # We told the world what we can do!
    # Workflow will be a list of the functions, and the parameters if there are any
    workflow = []
    for op in workflow_def:
        try:
            workflow.append((known_operations[op['id']],op.get('parameters',{})))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = run_workflow(message, workflow, guid)

    # return the workflow def so that the caller can see what we did
    final_answer['workflow'] = workflow_def

    # return the answer
    return final_answer, status_code


def post(name, url, message, guid, params=None) -> (dict, int):
    """
    launches a post request, returns the response.

    :param name: name of service
    :param url: the url of the service
    :param message: the message to post to the service
    :param guid: run identifier
    :param params: the parameters passed to the service
    :return: dict, status code
    """
    # init return values
    ret_val = message

    debug = os.environ.get('DEBUG_TIMING', 'False')

    if debug == 'True':
        dt_start = datetime.now()

    if 'workflow' in message and message['workflow'] is None:
        del message['workflow']

    logger.debug(f"{guid}: Calling {url}")

    try:
        if params is None:
            response = requests.post(url, json=message)
        else:
            response = requests.post(url, json=message, params=params)

        # save the response code
        status_code = response.status_code

        logger.info(f'{guid}: {name} returned with {status_code}')

        if status_code == 200:
            try:
                # regardless of the error code if there is a response return it
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

    if 'logs' not in ret_val:
        ret_val['logs'] = []

    # html error code returned
    if status_code != 200:
        error_string=f'{guid}: {name} HTML error status code {status_code} returned.'
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


def strider(message, params, guid) -> (dict, int):
    """
    Calls strider
    :param message:
    :return:
    """
    url = 'https://strider.renci.org/1.2/query'
    response = post('strider', url, message, guid)
    return response

def answercoalesce(message, params, guid, coalesce_type='all') -> (dict,int):
    """
    Calls answercoalesce
    :param message:
    :param coalesce_type:
    :return:
    """
    url = f'https://answercoalesce.renci.org/1.2/coalesce/{coalesce_type}'
    return post('answer_coalesce', url, message, guid)

def omnicorp(message, params, guid) -> (dict,int):
    """
    Calls omnicorp
    :param message:
    :param coalesce_type:
    :return:
    """
    url='https://aragorn-ranker.renci.org/1.2/omnicorp_overlay'
    return post('omnicorp', url, message, guid)

def score(message, params, guid) -> (dict,int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :return:
    """
    weight_url='https://aragorn-ranker.renci.org/1.2/weight_correctness'
    score_url='https://aragorn-ranker.renci.org/1.2/score'
    message, status_code = post('weight', weight_url, message, guid)
    return  post('score', score_url, message, guid)

def run_workflow(message, workflow, guid) -> (dict, int):
    # create a guid
    # do we still want this?  What's the purpose?
    #uid: str = str(uuid.uuid4())

    logger.debug(f'{guid}: {message}')

    for operator_function, params in workflow:
        message, status_code = operator_function(message, params, guid)
        if len(message['message']['results']) == 0:
            break

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
