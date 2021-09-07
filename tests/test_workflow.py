import pytest
from fastapi.testclient import TestClient
from src.server import APP
import os
import json
from unittest.mock import patch

client = TestClient(APP)
jsondir = 'InputJson_1.2'

def test_bad_ops():
    # get the location of the test file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'workflow_422.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert(response.status_code == 422)

def test_lookup_only():
    """This has a workflow with a single op (lookup).  So the result should not have scores"""
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    test_filename = os.path.join(dir_path, jsondir, 'workflow_200.json')

    with open(test_filename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert (response.status_code == 200)
    results = response.json()['message']['results']
    assert len(results) > 0
    assert results[0]['score'] is None