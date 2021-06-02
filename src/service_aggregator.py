"""Literature co-occurrence support."""
import logging
import requests
import json
import uuid

from datetime import datetime
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)


def entry(message, coalesce_type='all') -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of a TRAPI Message
    :param coalesce_type: what kind of answer coalesce type should be performed
    :return: the result of the request and the status code
    """

    # make the call to traverse the various services to get the data
    final_answer, status_code = strider_and_friends(message, coalesce_type)

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

    try:
        if params is None:
            response = requests.post(url, json=message)
        else:
            response = requests.post(url, json=message, params=params)

        # save the response code
        status_code = response.status_code

        if status_code != 200:
            logger.error(f'Error response from {name}, status code: {response.status_code}')

        # regardless of the error code if there is a response return it
        if len(response.json()):
            ret_val = response.json()

    except ConnectionError as ce:
        status_code = 404
        logger.error(ce)
    except Exception as e:
        status_code = 500
        logger.error(e)

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


def strider(message) -> (dict, int):
    """
    Calls strider
    :param message:
    :return:
    """
    url = 'https://strider.renci.org/1.1/query'

    strider_answer, status_code = post('strider', url, message)

    return strider_answer, status_code


def strider_and_friends(message, coalesce_type) -> (dict, int):
    # create a guid
    uid: str = str(uuid.uuid4())

    # call the strider service
    running_answer, status_code = strider(message)

    # html error code returned
    if status_code != 200:
        logger.error(f'Strider error: HTML error status code {status_code} returned.')
        running_answer['logs'].append(create_log_entry(f'Strider error: HTML error status code {status_code} returned.', "ERROR"))
        return running_answer, status_code
    # good html status code
    else:
        # check the results. if none returned then abort the pipeline
        if len(running_answer['message']['results']) == 0:
            running_answer['logs'].append(create_log_entry(f'Strider warning: No results returned', "WARNING"))
            return running_answer, status_code
        else:
            logger.debug(f"strider in ({uid}): {json.dumps(message)}")
            logger.debug(f"strider out ({uid}): {json.dumps(running_answer)}")

    # are we doing answer coalesce
    if coalesce_type != 'none':
        # get the request coalesced answer
        running_answer, status_code = post('coalesce', f'https://answercoalesce.renci.org/1.1/coalesce/{coalesce_type}', running_answer) # https://answercoalesce.renci.org/1.1/coalesce/ http://127.0.0.1:5001/coalesce/

        # html error code returned
        if status_code != 200:
            logger.error(f'Answer coalesce error: HTML error status code {status_code} returned.')
            running_answer['logs'].append(create_log_entry(f'Answer coalesce error: HTML error status code {status_code} returned.', "WARNING"))

        # good html status code
        else:
            logger.debug(f'coalesce out ({uid}): {json.dumps(running_answer)}')

    # call the omnicorp overlay service
    running_answer, status_code = post('omnicorp', 'https://aragorn-ranker.renci.org/1.1/omnicorp_overlay', running_answer)  # https://aragorn-ranker.renci.org/1.1/ http://127.0.0.1:5002/

    # html error code returned
    if status_code != 200:
        logger.error(f'Ranker/Omnicorp overlay error: HTML error status code {status_code} returned.')
        running_answer['logs'].append(create_log_entry(f'Ranker/Omnicorp overlay error: HTML error status code {status_code} returned.', "WARNING"))
    # good html status code
    else:
        logger.debug(f'omnicorp out ({uid}): {json.dumps(running_answer)}')

    # call the weight correction service
    running_answer, status_code = post('weight', 'https://aragorn-ranker.renci.org/1.1/weight_correctness', running_answer)

    # html error code returned
    if status_code != 200:
        logger.error(f'Ranker/Weight correctness error: HTML error status code {status_code} returned.')
        running_answer['logs'].append(create_log_entry(f'Ranker/Weight correctness error: HTML error status code {status_code} returned.', "WARNING"))
    # good html status code
    else:
        logger.debug(f'weighted out ({uid}): {json.dumps(running_answer)}')

    # call the scoring service
    running_answer, status_code = post('score', 'https://aragorn-ranker.renci.org/1.1/score', running_answer)

    # html error code returned
    if status_code != 200:
        logger.error(f'Ranker/Score error: HTML error status code {status_code} returned.')
        running_answer['logs'].append(create_log_entry(f'Ranker/Score error: HTML error status code {status_code} returned.', "WARNING"))
    # good html status code, but still do some checking
    else:
        logger.debug(f'scored out ({uid}): {json.dumps(running_answer)}')

    # return the requested data
    return running_answer, status_code


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
