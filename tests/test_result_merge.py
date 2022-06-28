import pytest
from src.service_aggregator import merge_results_by_node

@pytest.mark.asyncio
async def test_simple_merge():
    m = {'message':
    {"query_graph": {"nodes": { "drug":{"categories":["biolink:ChemicalEntity"]},
                                "disease": {"ids": ["MONDO:1234"]}},
                     "edges": { "e": {"subject": "drug", "object": "disease", "predicate": "biolink:treats"}}},
     "knowledge_graph": {}, #doesn't get used
     "results": [
         {"node_bindings": {"disease": [{"id":"MONDO:1234"}], "drug": [{"id":"drug1"}]},
          "edge_bindings": {"_1": [{"id": "e1"}]}},
         {"node_bindings": {"disease": [{"id": "MONDO:1234"}], "drug": [{"id": "drug1"}]},
          "edge_bindings": {"_1": [{"id": "e2"}]}},
     ]
     }}
    m = await merge_results_by_node(m,'drug')
    results = m['message']['results']
    assert len(results) == 1
    assert len(results[0]['node_bindings']) == 2
    assert len(results[0]['edge_bindings']) == 2
