import pytest
from fastapi.testclient import TestClient
import redis.asyncio
from src.server import APP as APP
from src import operations
import os
import json
from unittest.mock import patch
from random import shuffle
from src.process_db import init_db
from tests.helpers.redisMock import redisMock

client = TestClient(APP)
jsondir = 'InputJson_1.2'

def test_bad_ops(monkeypatch):
    monkeypatch.setattr(redis.asyncio, "Redis", redisMock)
    # get the location of the test file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'workflow_422.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/aragorn/query', json=query)

    # was the request successful
    assert(response.status_code == 422)

def test_lookup_only(monkeypatch):
    """This has a workflow with a single op (lookup).  So the result should not have scores"""
    monkeypatch.setattr(redis.asyncio, "Redis", redisMock)
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

