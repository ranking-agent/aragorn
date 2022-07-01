import os

import requests
from rules import rules
import json
from reasoner_pydantic import Query,Message, KnowledgeGraph
from copy import deepcopy
from src.service_aggregator import merge_results_by_node
import asyncio

import redis

#1 is for treats to start out
# will be to find out which db to lookin for a given predicate?
def get_redis(db=1):
    r = redis.Redis(host='localhost', port=6379, db=db)
    return r

async def get_input_ids():
    automat_url = os.environ.get("ROBOKOP_CYPHER_ENDPOINT", 'https://automat.renci.org/robokopkg/cypher')
    query = {"query": "MATCH (n:`biolink:DiseaseOrPhenotypicFeature`) RETURN n.id"}
    results = requests.post(automat_url,json=query).json()
    dids = [ result['row'][0] for result in results['results'][0]['data'] ]
    vals = ['MONDO','HP','DOID','ORPHANET','OMIM','MESH','UMLS','NCIT','EFO']
    skeys = {p:i for i,p in enumerate(vals)}
    predids = [ (skeys[ x.split(':')[0] ], x) for x in dids]
    predids.sort()
    dids = [ x[1] for x in predids ]
    print(len(dids))
    return dids

async def collect_results(did,r):
    print(did)
    result_messages = []
    for nr,rule in enumerate(rules):
        #We're using the $ here so that we can update as needed to match the input query
        query = rule.substitute(disease="$disease$", chemical="$chemical$", disease_id=did)
        qg = json.loads(query)
        message = {'message':qg}
        automat_url ='https://automat.renci.org/robokopkg/1.2/query'
        results = requests.post(automat_url,json=message)
        if results.status_code == 200:
            message = results.json()
            print(nr, len(message['message']['results']) )
            if len(message['message']['results']) > 0:
                result_messages.append(message)
    if len(result_messages) > 0:
        print(f'got hits for {len(result_messages)} rules')
        # We have to stitch stuff together again
        pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
        for rm in result_messages:
            pydantic_kgraph.update(KnowledgeGraph.parse_obj(rm['message']['knowledge_graph']))
        result = result_messages[0]
        result['message']['knowledge_graph'] = pydantic_kgraph.dict()
        for rm in result_messages[1:]:
            result['message']['results'].extend(rm['message']['results'])
        mergedresults = await merge_results_by_node(result, "$chemical$")
        print(f'merged result has {len(result["message"]["results"])} results')
        jsonresult = json.dumps(mergedresults)
    else:
        jsonresult = '{}'
    r.set(did,jsonresult)

async def go():
    #await collect_results('MONDO:0005044')
    ids = await get_input_ids()
    r = get_redis(1)
    for disease_id in ids:
        if r.get(disease_id) is None:
            await collect_results(disease_id,r)

asyncio.run(go())

