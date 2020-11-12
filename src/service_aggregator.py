"""Literature co-occurrence support."""
import logging
import requests

logger = logging.getLogger(__name__)


def query(message, coalesce_type='none') -> dict:
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
    if params is None:
        response = requests.post(url, json=message)
    else:
        response = requests.post(url, json=message, params=params)

    if not response.status_code == 200:
        print(name, 'error:', response.status_code)
        return {}

    return response.json()


def strider(message) -> dict:
    """
    Calls strider
    :param message:
    :return:
    """
    url = 'http://robokop.renci.org:5781/query'

    strider_answer = post(strider, url, message)

    num_answers = len(strider_answer['results'])

    if (num_answers == 0) or ((num_answers == 1) and (len(strider_answer['results'][0]['node_bindings']) == 0)):
        print('no answers')
        return {}

    # Strider for some reason doesn't return the query graph
    strider_answer['query_graph'] = message['message']['query_graph']

    return strider_answer


def strider_and_friends(message, coalesce_type) -> dict:
    # call strider service
    strider_answer: dict = strider(message)

    # call the omnicorp overlay service
    omni_answer: dict = post('omnicorp', 'https://aragorn-ranker.renci.org/omnicorp_overlay', {'message': strider_answer})

    # call the weight correction service
    weighted_answer: dict = post('weight', 'https://aragorn-ranker.renci.org/weight_correctness', {'message': omni_answer})

    # call the scoring service
    scored_answer: dict = post('score', 'https://aragorn-ranker.renci.org/score', {'message': weighted_answer})

    if coalesce_type != 'none':
        # get the request coalesced answer
        final_answer: dict = post('coalesce', f'https://answercoalesce.renci.org/coalesce/{coalesce_type}', {'message': scored_answer})
    else:
        # just return the scored result in Message format
        final_answer: dict = scored_answer

    # return the requested data
    return final_answer


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
