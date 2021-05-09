from fastapi.testclient import TestClient
from src.server import APP
import os,json

client = TestClient(APP)

jsondir= 'InputJson_1.0'

def test_standup_1():
    # get the location of the Translator specification file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))

    testfilename = os.path.join(dir_path,jsondir,'standup_1.json')

    with open(testfilename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert(response.status_code == 200)

    # convert the response to a json object
    jret = json.loads(response.content)

    # check the data
    ret = jret['message']
    assert(len(ret) == 3)
    assert( len(ret['results']) == 116 )

def test_standup_2():
    # get the location of the Translator specification file
    dir_path: str = os.path.dirname(os.path.realpath(__file__))

    testfilename = os.path.join(dir_path,jsondir,'standup_2.json')

    with open(testfilename, 'r') as tf:
        query = json.load(tf)

    # make a good request
    response = client.post('/query', json=query)

    # was the request successful
    assert(response.status_code == 200)

    # convert the response to a json object
    jret = json.loads(response.content)

    # check the data
    ret = jret['message']
    assert(len(ret) == 3)
    assert( len(ret['results']) == 116 )