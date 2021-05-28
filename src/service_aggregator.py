"""Literature co-occurrence support."""
import logging
import requests
import json
import uuid

logger = logging.getLogger(__name__)


def entry(message, coalesce_type='all') -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of form Message
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
    ret_val = {}
    status_code = 500

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

    except Exception as e:
        logger.error(e)

    return ret_val, status_code


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

    # call strider service
    strider_answer, status_code = strider(message)

    # was any data returned
    if len(strider_answer) == 0:
        message['error'] = 'Strider error: Got empty response from strider.'
        return message, status_code
    # html error code returned, but at least there was something returned
    elif status_code != 200:
        logger.error(f'Strider error: HTML error status code {status_code} returned.')
        strider_answer['error'] = f'Strider error: HTML error status code {status_code} returned.'
        return strider_answer, status_code
    # good html status code, but still do some checking
    elif status_code == 200:
        # are there some results
        num_results = len(strider_answer['message']['results'])

        # was there some actual results returned
        if (num_results == 0) or ((num_results == 1) and (len(strider_answer['results'][0]['node_bindings']) == 0)):
            logger.error(f'Strider error: No results data returned.')
            strider_answer['error'] = f'Strider error: No results data returned.'
            return strider_answer, status_code
        else:
            logger.debug(f"strider in ({uid}): {json.dumps(message)}")
            logger.debug(f"strider out ({uid}): {json.dumps(strider_answer)}")

    # are we doing answer coalesce
    if coalesce_type != 'none':
        # get the request coalesced answer
        coalesce_answer, status_code = post('coalesce', f'https://answercoalesce.renci.org/1.1/coalesce/{coalesce_type}', strider_answer)

        # was any data returned
        if len(coalesce_answer) == 0:
            strider_answer['error'] = 'Answer coalesce error: Got empty response from strider.'
            return strider_answer, status_code
        # html error code returned, but at least there was something returned
        elif status_code != 200:
            logger.error(f'Answer coalesce error: HTML error status code {status_code} returned.')
            coalesce_answer['error'] = f'Answer coalesce error: HTML error status code {status_code} returned.'
            return coalesce_answer, status_code
        # good html status code, but still do some checking
        elif status_code == 200:
            # are there some results
            num_results = len(strider_answer['message']['results'])

            # was there some actual results returned
            if (num_results == 0) or ((num_results == 1) and (len(coalesce_answer['results'][0]['node_bindings']) == 0)):
                logger.error(f'Answer coalesce error: No results data returned.')
                coalesce_answer['error'] = f'Answer coalesce error: No results data returned.'
                return coalesce_answer, status_code
            else:
                logger.debug(f'coalesce out ({uid}): {json.dumps(coalesce_answer)}')
    else:
        # just use the strider result in Message format
        coalesce_answer: dict = strider_answer

    # call the omnicorp overlay service
    omni_answer, status_code = post('omnicorp', 'https://aragorn-ranker.renci.org/1.1/omnicorp_overlay', coalesce_answer)

    # # open the tests file
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'tests', 'omni_answer.json'), 'r') as tf:
    #     omni_answer = json.load(tf)

    # was any data returned
    if len(omni_answer) == 0:
        coalesce_answer['error'] = 'Ranker/Omnicorp overlay error: Got empty response from strider.'
        return coalesce_answer, status_code
    # html error code returned, but at least there was something returned
    elif status_code != 200:
        logger.error(f'Ranker/Omnicorp overlay error: HTML error status code {status_code} returned.')
        omni_answer['error'] = f'Ranker/Omnicorp overlay error: HTML error status code {status_code} returned.'
        return omni_answer, status_code
    # good html status code, but still do some checking
    elif status_code == 200:
        # are there some results
        num_results = len(omni_answer['message']['results'])

        # was there some actual results returned
        if (num_results == 0) or ((num_results == 1) and (len(omni_answer['results'][0]['node_bindings']) == 0)):
            logger.error(f'Ranker/Omnicorp overlay error: No results data returned.')
            omni_answer['error'] = f'Ranker/Omnicorp overlay error: No results data returned.'
            return omni_answer, status_code
        else:
            logger.debug(f'coalesce out ({uid}): {json.dumps(omni_answer)}')

    # call the weight correction service
    weighted_answer, status_code = post('weight', 'https://aragorn-ranker.renci.org/1.1/weight_correctness', omni_answer)

    # open the tests file
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'tests', 'weighted_answer.json'), 'r') as tf:
    #     weighted_answer = json.load(tf)

    # was any data returned
    if len(weighted_answer) == 0:
        omni_answer['error'] = 'Ranker/Weight correctness error: Got empty response from strider.'
        return omni_answer, status_code
    # html error code returned, but at least there was something returned
    elif status_code != 200:
        logger.error(f'Ranker/Weight correctness error: HTML error status code {status_code} returned.')
        weighted_answer['error'] = f'Ranker/Weight correctness error: HTML error status code {status_code} returned.'
        return weighted_answer, status_code
    # good html status code, but still do some checking
    elif status_code == 200:
        # are there some results
        num_results = len(weighted_answer['message']['results'])

        # was there some actual results returned
        if (num_results == 0) or ((num_results == 1) and (len(weighted_answer['results'][0]['node_bindings']) == 0)):
            logger.error(f'Ranker/Weight correctness error: No results data returned.')
            message['error'] = f'Ranker/Weight correctness error: No results data returned.'
            return weighted_answer, status_code
        else:
            logger.debug(f'weighted out ({uid}): {json.dumps(weighted_answer)}')

    # call the scoring service
    scored_answer, status_code = post('score', 'https://aragorn-ranker.renci.org/1.1/score', weighted_answer)

    # # open the input and output files
    # with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'tests', 'scored_answer.json'), 'r') as tf:
    #     json.dump(scored_answer, out_file, indent=2)

    # was any data returned
    if len(scored_answer) == 0:
        weighted_answer['error'] = 'Ranker/Score error: Got empty response from strider.'
        return scored_answer, status_code
    # html error code returned, but at least there was something returned
    elif status_code != 200:
        logger.error(f'Ranker/Score error: HTML error status code {status_code} returned.')
        scored_answer['error'] = f'Ranker/Score error: HTML error status code {status_code} returned.'
        return scored_answer, status_code
    # good html status code, but still do some checking
    elif status_code == 200:
        # are there some results
        num_results = len(scored_answer['message']['results'])

        # was there some actual results returned
        if (num_results == 0) or ((num_results == 1) and (len(scored_answer['results'][0]['node_bindings']) == 0)):
            logger.error(f'Ranker/Score error: No results data returned.')
            scored_answer['error'] = f'Ranker/Score error: No results data returned.'
            return scored_answer, status_code
        else:
            logger.debug(f'scored out ({uid}): {json.dumps(scored_answer)}')

    # return the requested data
    return scored_answer, status_code


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
