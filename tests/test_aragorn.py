from fastapi.testclient import TestClient
from src.server import APP
import os
import json

client = TestClient(APP)

jsondir = 'InputJson_1.1'


def test_standup_1():
    # get the location of the test file
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

    # make sure we got back the query_graph, knowledge_graph and results data from strider
    assert(len(ret) == 3)

    # make sure we got the expected number of results
    assert(len(ret['results']) == 61)

    # strider should have created knowledge graph nodes and edges
    assert (len(ret['knowledge_graph']['nodes']) > 1)
    assert (len(ret['knowledge_graph']['edges']) > 1)

    # turn dicts into a list for easier indexing
    kg_node_list = list(ret['knowledge_graph']['nodes'].items())
    kg_edge_list = list(ret['knowledge_graph']['edges'].items())

    # insure that strider added node and edge attributes to the knowledge graph data
    assert('attributes' in kg_node_list[1][1])
    assert('attributes' in kg_edge_list[1][1])

    # AC updates the query graph with extra nodes and edges
    assert(len(ret['query_graph']['nodes']) > len(query['message']['query_graph']['nodes']))
    assert(len(ret['query_graph']['edges']) > len(query['message']['query_graph']['edges']))

    target_kgnode = None

    # AC augments knowledge graph nodes categories and attributes. find a node with len()s > 1 to confirm
    for n in kg_node_list:
        if 'categories' in n[1] and len(n[1]['categories']) > 0 and 'attributes' in n[1] and len(n[1]['attributes']) > 0:
            target_kgnode = n
            break

    assert target_kgnode

    found = False

    # insure AC node norm did something in the knowledge graph.
    # node normalization should have added some number of equivalent ids
    for a in target_kgnode[1]['attributes']:
        if 'attribute_type_id' in a and a['attribute_type_id'].startswith('biolink:same_as'):
            assert(len(a['value']) >= 1)
            found = True
            break

    assert found

    found = False

    # insure AC added p-values and a coalesce method in the results.
    for r in ret['results']:
        if 'node_bindings' in r:
            for nb in r['node_bindings']:
                if len(r['node_bindings'][nb][0]) > 1 and 'p_value' in r['node_bindings'][nb][0] and 'coalescence_method' in r['node_bindings'][nb][0]:
                    found = True
                    break

    assert found

    found = False

    # insure that ranker/omnicorp overlay added the omni article count
    for n in kg_node_list:
        if 'attributes' in n[1] and len(n[1]['attributes']) > 0:
            for a in n[1]['attributes']:
                if a['original_attribute_name'].startswith('omnicorp_article_count'):
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure that ranker/omnicorp overlay added the omnicorp data
    for e in kg_edge_list:
        if 'attributes' in e[1]:
            for a in e[1]['attributes']:
                if str(a['value']).startswith('omnicorp') or str(a['value']).startswith('omnicorp.term_to_term'):
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure that ranker/weight added the weight element
    for r in ret['results']:
        if 'edge_bindings' in r:
            for nb in r['edge_bindings']:
                if len(r['edge_bindings'][nb][0]) > 1 and 'weight' in r['edge_bindings'][nb][0]:
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure ranker/score added the score element
    for r in ret['results']:
        if 'score' in r:
            found = True
            break

    assert found


def test_standup_2():
    # get the location of the test file
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

    # make sure we got back the query_graph, knowledge_graph and results data from strider
    assert(len(ret) == 3)

    # make sure we got the expected number of results
    assert(len(ret['results']) == 21)

    # strider should have created knowledge graph nodes and edges
    assert (len(ret['knowledge_graph']['nodes']) > 1)
    assert (len(ret['knowledge_graph']['edges']) > 1)

    # turn dicts into a list for easier indexing
    kg_node_list = list(ret['knowledge_graph']['nodes'].items())
    kg_edge_list = list(ret['knowledge_graph']['edges'].items())

    # insure that strider added node and edge attributes to the knowledge graph data
    assert('attributes' in kg_node_list[1][1])
    assert('attributes' in kg_edge_list[1][1])

    # AC updates the query graph with extra nodes and edges
    assert(len(ret['query_graph']['nodes']) > len(query['message']['query_graph']['nodes']))
    assert(len(ret['query_graph']['edges']) > len(query['message']['query_graph']['edges']))

    target_kgnode = None

    # AC augments knowledge graph nodes categories and attributes. find a node with len()s > 1 to confirm
    for n in kg_node_list:
        if 'categories' in n[1] and len(n[1]['categories']) > 1 and 'attributes' in n[1] and len(n[1]['attributes']) > 0:
            target_kgnode = n
            break

    assert target_kgnode

    found = False

    # insure AC node norm did something in the knowledge graph.
    # node normalization should have added some number of equivalent ids
    for a in target_kgnode[1]['attributes']:
        if 'attribute_type_id' in a and a['attribute_type_id'].startswith('biolink:same_as'):
            assert(len(a['value']) >= 1)
            found = True
            break

    assert found

    found = False

    # insure AC added p-values and a coalesce method in the results.
    for r in ret['results']:
        if 'node_bindings' in r:
            for nb in r['node_bindings']:
                if len(r['node_bindings'][nb][0]) > 1 and 'p_value' in r['node_bindings'][nb][0] and 'coalescence_method' in r['node_bindings'][nb][0]:
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure that ranker/omnicorp overlay the omni article count
    for n in kg_node_list:
        if 'attributes' in n[1] and len(n[1]['attributes']) > 0:
            for a in n[1]['attributes']:
                if a['original_attribute_name'].startswith('omnicorp_article_count'):
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure that ranker/omnicorp overlay added the omnicorp data
    for e in kg_edge_list:
        if 'attributes' in e[1]:
            for a in e[1]['attributes']:
                if str(a['value']).startswith('omnicorp') or str(a['value']).startswith('omnicorp.term_to_term'):
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure that ranker/weight added the weight element
    for r in ret['results']:
        if 'edge_bindings' in r:
            for nb in r['edge_bindings']:
                if len(r['edge_bindings'][nb][0]) > 1 and 'weight' in r['edge_bindings'][nb][0]:
                    found = True
                    break
        if found:
            break

    assert found

    found = False

    # insure ranker/score added the score element
    for r in ret['results']:
        if 'score' in r:
            found = True
            break

    assert found
