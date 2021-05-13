from fastapi.testclient import TestClient
from src.server import APP
import os,json

client = TestClient(APP)

jsondir = 'InputJson_1.0'

def test_standup_1():
    # get the location of the Translator specification file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))

    test_filename = os.path.join(dir_path, jsondir, 'standup_1.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert(response.status_code == 200)

    # convert the response to a json object
    j_ret = json.loads(response.content)

    # check the data
    ret = j_ret['message']

    # make sure we got back the query_graph, knowledge_graph and results data
    assert(len(ret) == 3)

    # make sure we got the expected number of results
    assert(len(ret['results']) == 95)

    # strider should have created nodes and edges
    assert (len(ret['knowledge_graph']['nodes']) > 1)
    assert (len(ret['knowledge_graph']['edges']) > 1)

    # turn dicts into a list for easier indexing
    node_list = list(ret['knowledge_graph']['nodes'].items())
    edge_list = list(ret['knowledge_graph']['edges'].items())

    # insure that AC added node and edge attributes to the knowledge graph data
    assert('attributes' in node_list[0][1])
    assert('attributes' in edge_list[1][1])

    found = False

    # insure that ranker omnicorp overlay node have omni article counts
    for n in node_list[1][1]['attributes']:
        if n['name'] == 'omnicorp_article_count':
            found = True
            break

    assert found

    found = False

    # insure that ranker omnicorp overlay edges have omnicorp data
    for e in edge_list:
        if 'attributes' in e[1]:
            for a in e[1]['attributes']:
                if str(a['value']).startswith('omnicorp')  or str(a['value']).startswith('omnicorp.term_to_term'):
                    found = True
                    break

    assert found

    # insure AC node norm did something. node normalization should
    # have added some number of equivalent ids in the results
    assert(len(ret['results'][0]['node_bindings']['n1']) > 1)

    # insure that AC added p-values and coalescence method
    assert ('p_value' in ret['results'][0]['node_bindings']['n1'][0])
    assert ('coalescence_method' in ret['results'][0]['node_bindings']['n1'][0])

    # insure that ranker weight added the weight element
    assert ('weight' in ret['results'][0]['edge_bindings']['e01'][0])

    # insure ranker score added the score element
    assert ('score' in ret['results'][0])

def test_standup_2():
    # get the location of the Translator specification file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))

    test_filename = os.path.join(dir_path, jsondir, 'standup_2.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert(response.status_code == 200)

    # convert the response to a json object
    j_ret = json.loads(response.content)

    # check the data
    ret = j_ret['message']

    # make sure we got back the query_graph, knowledge_graph and results data
    assert(len(ret) == 3)

    # make sure we got the expected number of results
    assert(len(ret['results']) == 26)

    # strider should have created nodes and edges
    assert (len(ret['knowledge_graph']['nodes']) > 1)
    assert (len(ret['knowledge_graph']['edges']) > 1)

    # turn dicts into a list for easier indexing
    node_list = list(ret['knowledge_graph']['nodes'].items())
    edge_list = list(ret['knowledge_graph']['edges'].items())

    # insure that AC added node and edge attributes to the knowledge graph data
    assert('attributes' in node_list[0][1])
    assert('attributes' in edge_list[1][1])

    found = False

    # insure that ranker omnicorp overlay node have omni article counts
    for n in node_list[1][1]['attributes']:
        if n['name'] == 'omnicorp_article_count':
            found = True
            break

    assert found

    found = False

    # insure that ranker omnicorp overlay edges have omnicorp data
    for e in edge_list:
        if 'attributes' in e[1]:
            for a in e[1]['attributes']:
                if str(a['value']).startswith('omnicorp')  or str(a['value']).startswith('omnicorp.term_to_term'):
                    found = True
                    break

    assert found

    # insure AC node norm did something. node normalization should
    # have added some number of equivalent ids in the results
    assert(len(ret['results'][0]['node_bindings']['n01']) > 1)

    # insure that AC added p-values and coalescence method
    assert ('p_value' in ret['results'][0]['node_bindings']['n01'][0])
    assert ('coalescence_method' in ret['results'][0]['node_bindings']['n01'][0])

    # insure that ranker weight added the weight element
    assert ('weight' in ret['results'][0]['edge_bindings']['e00'][0])

    # insure ranker score added the score element
    assert ('score' in ret['results'][0])
