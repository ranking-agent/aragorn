"""Literature co-occurrence support."""
import logging
import requests
import json
import os
import uuid

logger = logging.getLogger(__name__)


def entry(message, coalesce_type='none') -> dict:
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of form Message
    :param coalesce_type: what kind of answer coalesce type should be performed
    :return: the result of the request
    """

    # make the call to traverse the various services to get the data
    final_answer: dict = strider_and_friends(message, coalesce_type)

    # return the answer
    return final_answer


def post(name, url, message, params=None):
    """
    launches a post request, returns the response.

    :param name: name of service
    :param url: the url of the service
    :param message: the message to post to the service
    :param params: the parameters passed to the service
    :return: dict, the result
    """
    try:
        if params is None:
            response = requests.post(url, json=message)
        else:
            response = requests.post(url, json=message, params=params)

        if not response.status_code == 200:
            logger.error(f'Error response from {name}, status code: {response.status_code}')
            return {}

        return response.json()
    except Exception as e:
        logger.error(e)
        return None

def strider(message) -> dict:
    """
    Calls strider
    :param message:
    :return:
    """
    url = 'http://robokop.renci.org:5781/query'

    strider_answer = post('strider', url, message)

    if strider_answer is None or len(strider_answer) == 0:
        return {}

    num_answers = len(strider_answer['message']['results'])

    if (num_answers == 0) or ((num_answers == 1) and (len(strider_answer['results'][0]['node_bindings']) == 0)):
        logger.error(f'Error response from Strider, no result data returned.')
        return {}

    # scan for missing attribute types. put one in if there isnt one already three ("type": "EDAM:data_0006")
    kg_nodes = strider_answer['message']['knowledge_graph']['nodes']

    for node in kg_nodes:
        attribs = kg_nodes[node]['attributes']

        for attrib in attribs:
            if "type" not in attrib:
                attrib['type'] = 'EDAM:data_0006'

    return strider_answer


def strider_and_friends(message, coalesce_type) -> dict:

    # create a guid
    uid: str = str(uuid.uuid4())

    message['error'] = None

    # call strider service
    strider_answer: dict = strider(message)

    # was there an error getting data
    if strider_answer is None:
        #logger.error("Error detected. Strider failed to return anything, aborting.")
        message['status'] = 'Error detected. Strider didnt return anything, aborting.'
        return message
    elif len(strider_answer) == 0:
        #logger.error("Error detected. Got an empty answer from strider, aborting.")
        message['status'] = 'Error detected. Got an empty result from strider, aborting.'
        return message
    else:
        logger.debug(f"aragorn post ({uid}): {json.dumps(strider_answer)}")

    # are we doing answer coalesce
    if coalesce_type != 'none':
        # get the request coalesced answer
        coalesce_answer: dict = post('coalesce', f'https://answercoalesce.renci.org/coalesce/{coalesce_type}', strider_answer)

        # was there an error getting data
        if coalesce_answer is None:
            logger.error("Error detected: Got no answer from Answer coalesce, aborting.")
            message['status'] = 'Error detected: Answer coalesce failed to return an answer, aborting.'
            return message
        # did we get a good response
        elif len(coalesce_answer) == 0:
            logger.error("Error detected: Got an empty answer from Answer coalesce, aborting.")
            message['status'] = 'Error detected: Got an empty answer from Answer coalesce, aborting.'
            return message
        else:
            logger.debug(f'coalesce answer ({uid}): {json.dumps(coalesce_answer)}')
    else:
        # just use the strider result in Message format
        coalesce_answer: dict = strider_answer

    # call the omnicorp overlay service
    omni_answer: dict = post('omnicorp', 'https://aragorn-ranker.renci.org/omnicorp_overlay', coalesce_answer)

    # # open the test file
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'test', 'omni_answer.json'), 'r') as tf:
    #     omni_answer = json.load(tf)

    # was there an error getting data
    if omni_answer is None:
        logger.error('Error detected: Aragorn-ranker/omnicorp_overlay failed to return an answer, aborting.')
        message['status'] = 'Error detected: Aragorn-ranker/omnicorp_overlay failed to return an answer, aborting.'
        return message
    # did we get a good response
    elif len(omni_answer) == 0:
        logger.error('Error detected: Got an empty answer from Aragorn-ranker/omnicorp_overlay, aborting.')
        message['status'] = 'Error detected: Got an empty answer from Aragorn-ranker/omnicorp_overlay, aborting.'
        return message
    else:
        logger.debug(f'omni answer ({uid}): {json.dumps(omni_answer)}')

    # call the weight correction service
    weighted_answer: dict = post('weight', 'https://aragorn-ranker.renci.org/weight_correctness', omni_answer)

    # open the test file
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'test', 'weighted_answer.json'), 'r') as tf:
    #     weighted_answer = json.load(tf)

    # was there an error getting data
    if weighted_answer is None:
        logger.error('Error detected: Aragorn-ranker/weight_correctness failed to return an answer, aborting.')
        message['status'] = 'Error detected: Aragorn-ranker/weight_correctness failed to return an answer, aborting.'
        return message
    # did we get a good response
    elif len(weighted_answer) == 0:
        logger.error('Error detected: Got an empty answer from Aragorn-ranker/weight_correctness, aborting.')
        message['status'] = 'Error detected: Got an empty answer from Aragorn-ranker/weight_correctness, aborting.'
    else:
        logger.debug(f'weighted answer ({uid}): {json.dumps(weighted_answer)}')

    # call the scoring service
    scored_answer: dict = post('score', 'https://aragorn-ranker.renci.org/score', weighted_answer)

    # # open the input and output files
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'test', 'scored_answer.json'), 'r') as tf:
    #     json.dump(scored_answer, out_file, indent=2)

    # was there an error getting data
    if scored_answer is None:
        logger.error('Error detected: Aragorn-ranker/score failed to return an answer, aborting.')
        message['status'] = 'Error detected: Aragorn-ranker/score failed to return an answer, aborting.'
        return message
    # did we get a good response
    elif len(scored_answer) == 0:
        logger.error('Error detected: Got an empty answer from Aragorn-ranker/score, aborting.')
        message['status'] = 'Error detected: Got an empty answer from Aragorn-ranker/score, aborting.'
        return message
    else:
        logger.debug(f'scored answer ({uid}): {json.dumps(scored_answer)}')

    # return the requested data
    return scored_answer


def one_hop_message(curie_a, type_a, type_b, edge_type, reverse=False) -> dict:
    """
    Creates a test message.
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
