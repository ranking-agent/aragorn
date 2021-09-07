"""Literature co-occurrence support."""
import logging
import requests
import json
import uuid

from datetime import datetime
from requests.exceptions import ConnectionError

from functools import partial

logger = logging.getLogger(__name__)


def entry(message, coalesce_type='all') -> (dict, int):
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
                        'enrich_results': partial(answercoalesce,coalesce_type=coalesce_type),
                        'connect_knodes': omnicorp,
                        'score': score}

    # if the workflow is defined in the message use it, otherwise use the default aragorn workflow
    if 'workflow' in message and not (message['workflow'] is None):
        workflow_def = message['workflow']
        #The underlying tools (strider) don't want the workflow element and will 400
        del message['workflow']
    else:
        workflow_def = [{'id':'lookup'},{'id':'enrich_results'},{'id':'connect_knodes'},{'id':'score'}]

    #convert the workflow def into function calls.   Raise a 422 if we find one we don't actually know how to do.
    # We told the world what we can do!
    #Workflow will be a list of the functions, and the parameters if there are any
    workflow = []
    for op in workflow_def:
        try:
            workflow.append((known_operations[op['id']],op.get('parameters',{})))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = run_workflow(message, workflow)

    #return the workflow def so that the caller can see what we did
    final_answer['workflow'] = workflow_def

    # return the answer
    return final_answer, status_code


def post(name, url, message, params=None) -> (dict, int):
    """
    launches a post request, returns the response.

    :param name: name of service
    :param url: the url of the service
    :param message: the message to post to the service
    :param params: the parameters passed to the service
    :return: dict, status code
    """
    # init return values
    ret_val = message
    if 'workflow' in message and message['workflow'] is None:
        del message['workflow']

    logger.debug(f"Calling {url}")

    try:
        if params is None:
            response = requests.post(url, json=message)
        else:
            response = requests.post(url, json=message, params=params)

        # save the response code
        status_code = response.status_code

        # regardless of the error code if there is a response return it
        if len(response.json()):
            ret_val = response.json()

    except ConnectionError as ce:
        status_code = 404
        logger.error(ce)
    except Exception as e:
        status_code = 500
        logger.error(e)

    # html error code returned
    if status_code != 200:
        error_string=f'{name} error: HTML error status code {status_code} returned.'
        logger.error(error_string)
        ret_val['logs'].append(create_log_entry(error_string, "ERROR"))
    # good html status code
    elif len(ret_val['message']['results']) == 0:
        ret_val['logs'].append(create_log_entry(f'warning: empty returned', "WARNING"))
    else:
        logger.debug(f'Returned. {len(ret_val["message"]["results"])} results.')

    return ret_val, status_code


def create_log_entry(msg: str, err_level, code=None) -> dict:
    # load the data
    ret_val = {
        'timestamp': str(datetime.now()),
        'level': err_level,
        'message': msg,
        'code': code
    }

    # return to the caller
    return ret_val


def strider(message,params) -> (dict, int):
    """
    Calls strider
    :param message:
    :return:
    """
    url = 'https://strider.renci.org/1.1/query'
    response = post('strider', url, message)
    return response

def answercoalesce(message,params,coalesce_type='all') -> (dict,int):
    """
    Calls answercoalesce
    :param message:
    :param coalesce_type:
    :return:
    """
    url = f'https://answercoalesce.renci.org/1.1/coalesce/{coalesce_type}'
    return post('answer_coalesce',url, message)

def omnicorp(message,params) -> (dict,int):
    """
    Calls omnicorp
    :param message:
    :param coalesce_type:
    :return:
    """
    url='https://aragorn-ranker.renci.org/1.1/omnicorp_overlay'
    return post('omnicorp',url, message)

def score(message,params) -> (dict,int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :return:
    """
    weight_url='https://aragorn-ranker.renci.org/1.1/weight_correctness'
    score_url='https://aragorn-ranker.renci.org/1.1/score'
    message, status_code = post('weight', weight_url , message)
    return  post('score', score_url, message)

def run_workflow(message, workflow) -> (dict, int):
    # create a guid
    # do we still want this?  What's the purpose?
    #uid: str = str(uuid.uuid4())

    logger.debug(message)
    for operator_function, params in workflow:
        message, status_code = operator_function(message,params)

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
