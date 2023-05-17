import pytest
from fastapi.testclient import TestClient
from src.server import APP as APP
from src import operations
import os
import json
from unittest.mock import patch
from random import shuffle
from src.process_db import init_db

client = TestClient(APP)
jsondir = 'InputJson_1.2'

def test_bad_ops():
    # get the location of the test file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'workflow_422.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/aragorn/query', json=query)

    # was the request successful
    assert(response.status_code == 422)

def test_lookup_only():
    """This has a workflow with a single op (lookup).  So the result should not have scores"""
    init_db()
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'workflow_200.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    query['test'] = ''

    # make a good request
    response = client.post('/aragorn/query', json=query)

    # was the request successful
    assert (response.status_code == 200)
    results = response.json()['message']['results']
    assert len(results) > 0

    if 'score' in results[0] and results[0]['score'] is not None:
        assert False

@pytest.mark.asyncio
async def test_sorting():
    message = {'message':{'query_graph':{},'knowledge_graph':{},'results':[]}}
    #A,B,C are not real trapi, but we're just checking that we sort by score correctly
    message['message']['results'].append({'id':'A'}) #no score, come in last
    message['message']['results'].append({'id':'B','score':1})
    message['message']['results'].append({'id':'C','score':2})
    outm,s= await operations.sort_results_score(message,params={},guid='xyz')
    assert s==200
    ids = [r['id'] for r in outm['message']['results']]
    assert ids == ['C','B','A']
    outm2,s = await operations.sort_results_score(outm, params={'ascending_or_descending':'ascending'},guid='zyx')
    assert s==200
    ids = [r['id'] for r in outm2['message']['results']]
    assert ids == ['A','B','C']
    outm3,s = await operations.sort_results_score(outm2, params={'ascending_or_descending':'descending'},guid='zyx')
    assert s == 200
    ids = [r['id'] for r in outm3['message']['results']]
    assert ids == ['C','B','A']


@pytest.mark.asyncio
async def test_filter():
    #Note that the filtering doesn't care about the score, it just takes the first X
    message = {'message': {'query_graph': {}, 'knowledge_graph': {}, 'results': []}}
    # A,B,C are not real trapi, but we're just checking that we sort by score correctly
    message['message']['results'].append({'id': 'A'})  # no score, come in last
    message['message']['results'].append({'id': 'B', 'score': 1})
    message['message']['results'].append({'id': 'C', 'score': 2})
    outm,s = await operations.filter_results_top_n(message, params={'max_results':2}, guid='xyz')
    assert s == 200
    ids = [r['id'] for r in outm['message']['results']]
    assert ids == ['A','B']
    #if you call without max_results, it's going to filter, but to a big number not noticable here.
    outm2,s = await operations.filter_results_top_n(outm, params={}, guid='xyz')
    assert s == 200
    ids = [r['id'] for r in outm2['message']['results']]
    assert ids == ['A','B']

@pytest.mark.asyncio
async def test_filter_kgraph():
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'filter_input.json')
    with open(test_filename, 'r') as tf:
        input = json.load(tf)
    n_qnodes = len(input['message']['query_graph']['nodes'])
    n_qedges = len(input['message']['query_graph']['edges'])
    n_knodes = len(input['message']['knowledge_graph']['nodes'])
    n_kedges = len(input['message']['knowledge_graph']['edges'])
    n_results = len(input['message']['results'])
    #Starting with more than one result and multiple knodes and kedges mapped to qnodes and qedges (in diff results)
    assert n_results > 1
    assert n_knodes > n_qnodes
    assert n_kedges > n_qedges
    message,s = await operations.filter_results_top_n(input, params={'max_results':1}, guid='xyz')
    assert s == 200
    #We should now have a single result, but still lots of knodes/edges
    n_qnodes = len(message['message']['query_graph']['nodes'])
    n_qedges = len(message['message']['query_graph']['edges'])
    n_knodes = len(message['message']['knowledge_graph']['nodes'])
    n_kedges = len(message['message']['knowledge_graph']['edges'])
    n_results = len(message['message']['results'])
    assert n_results == 1
    assert n_knodes > n_qnodes
    assert n_kedges > n_qedges
    #now filter kgraph, which should leave a single result, and therefore 1 knode / kedge for each qnode/qedge
    finalmessage,s = await operations.filter_kgraph_orphans(message,{},'xyz')
    assert s == 200
    n_qnodes = len(message['message']['query_graph']['nodes'])
    n_qedges = len(message['message']['query_graph']['edges'])
    n_knodes = len(message['message']['knowledge_graph']['nodes'])
    n_kedges = len(message['message']['knowledge_graph']['edges'])
    assert n_knodes == n_qnodes
    assert n_kedges == n_qedges

@pytest.mark.asyncio
async def test_filter_kgraph():
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'filter_input.json')
    with open(test_filename, 'r') as tf:
        input = json.load(tf)
    max_score = input['message']['results'][0]['score']
    shuffle(input['message']['results'])
    n_qnodes = len(input['message']['query_graph']['nodes'])
    n_qedges = len(input['message']['query_graph']['edges'])
    n_knodes = len(input['message']['knowledge_graph']['nodes'])
    n_kedges = len(input['message']['knowledge_graph']['edges'])
    n_results = len(input['message']['results'])
    #Starting with more than one result and multiple knodes and kedges mapped to qnodes and qedges (in diff results)
    assert n_results > 1
    assert n_knodes > n_qnodes
    assert n_kedges > n_qedges
    message,s = await operations.filter_message_top_n(input, params={'max_results':1}, guid='xyz')
    assert s == 200
    #We should now have a single result, and 1 knode / kedge for each qnode/qedge
    n_qnodes = len(message['message']['query_graph']['nodes'])
    n_qedges = len(message['message']['query_graph']['edges'])
    n_knodes = len(message['message']['knowledge_graph']['nodes'])
    n_kedges = len(message['message']['knowledge_graph']['edges'])
    n_results = len(message['message']['results'])
    assert n_results == 1
    assert n_knodes == n_qnodes
    assert n_kedges == n_qedges
    #And the result that's here should be the max score
    assert message['message']['results'][0]['score'] == max_score
