"""Literature co-occurrence support."""
import logging
import requests
import json
from reasoner_pydantic import Request, Message

logger = logging.getLogger(__name__)


def one_hop(message) -> Message:
    message = one_hop_message('MONDO:0005090', 'disease', 'chemical_substance', 'treats', reverse=True)

    answer, scored, results = striderandfriends(message)

    return answer


def automat(db, message):
    automat_url = f'https://automat.renci.org/{db}/query'
    response = requests.post(automat_url, json=message['message'])
    print(response.status_code)
    return response.json()


def post(name, url, message, params=None):
    if params is None:
        response = requests.post(url, json=message)
    else:
        response = requests.post(url, json=message, params=params)
    if not response.status_code == 200:
        print(name, 'error:', response.status_code)
        return {}
    return response.json()


def strider(message):
    url = 'http://robokop.renci.org:5781/query'
    strider_answer = post(strider, url, message)
    numanswers = len(strider_answer['results'])
    if (numanswers == 0) or ((numanswers == 1) and (len(strider_answer['results'][0]['node_bindings']) == 0)):
        print('no answers')
        return {}
    # Strider for some reason doesn't return the query graph
    strider_answer['query_graph'] = message['message']['query_graph']
    return strider_answer


def striderandfriends(message):
    strider_answer = strider(message)
    omni_answer = post('omnicorp', 'https://aragorn-ranker.renci.org/omnicorp_overlay', {'message': strider_answer})
    weighted_answer = post('weight', 'https://aragorn-ranker.renci.org/weight_correctness', {'message': omni_answer})
    scored_answer = post('score', 'https://aragorn-ranker.renci.org/score', {'message': weighted_answer})
    new_answer = json.dumps(scored_answer)
    new_answer = new_answer.replace('"curie": null,', '')
    scored_answer = json.loads(new_answer)
    coalesced_answer = post('coalesce', 'https://answercoalesce.renci.org/coalesce', {'message': scored_answer}, params={'method': 'graph'})
    return strider_answer, scored_answer, coalesced_answer

def one_hop_message(curiea,typea,typeb,edgetype,reverse=False):
    query_graph = {
    "nodes": [
        {
            "id": "a",
            "type": typea,
            "curie": curiea
        },
        {
            "id": "b",
            "type": typeb
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
    if edgetype is not None:
        query_graph['edges'][0]['type'] = edgetype
        if reverse:
            query_graph['edges'][0]['source_id'] = 'b'
            query_graph['edges'][0]['target_id'] = 'a'
    message = {"message": {"query_graph": query_graph,
                          'knowledge_graph':{"nodes": [], "edges": [],},
                           'results':[]}}
    return message