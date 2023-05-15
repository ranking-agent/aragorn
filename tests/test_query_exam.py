import pytest
from src.service_aggregator import merge_answer, create_aux_graph, add_knowledge_edge
from reasoner_pydantic.results import Analysis, EdgeBinding, Result, NodeBinding
from reasoner_pydantic.auxgraphs import AuxiliaryGraph
from reasoner_pydantic.message import Message, Response
from reasoner_pydantic.qgraph import QueryGraph

def create_result_graph():
    """Create a "treats" result graph with a query graph."""
    query_graph = {"nodes": {"input": {"ids": ["MONDO:1234"]}, "output": {"categories": ["biolink:ChemicalEntity"]}},
        "edges": {"e": {"subject": "output", "object": "input", "predicates": ["biolink:treats"]}}}
    result_message = {"query_graph": query_graph, "knowledge_graph": {"nodes":{}, "edges":{}}, "auxiliary_graphs": set(), "results": []}
    pydantic_message = Response(**{"message":result_message})
    return pydantic_message

def create_result(node_bindings: dict[str,str], edge_bindings: dict[str,str]) -> Result:
    """Pretend that we've had a creative mode query, and then broken it into rules.  THose rules run as individual
    queries in strider or robokop, and this is a result."""
    analysis = Analysis(edge_bindings = {k:[EdgeBinding(id=v)] for k,v in edge_bindings.items()},resource_id="infores:KP")
    result = Result(node_bindings = {k:[NodeBinding(id=v)] for k,v in node_bindings.items()}, analyses = set([analysis]))
    return result

def test_merge_answer():
    """Test that merge_answer() puts all the aux graphs in the right places."""
    pydantic_result = create_result_graph()
    result_message = pydantic_result.to_dict()
    answer = "PUBCHEM.COMPOUND:789"
    qnode_ids = ["input", "output"]
    result1 = create_result({"input":"MONDO:1234", "output":answer, "node2": "curie:3"}, {"g":"KEDGE:1", "f":"KEDGE:2"}).to_dict()
    result2 = create_result({"input":"MONDO:1234", "output":answer, "nodeX": "curie:8"}, {"q":"KEDGE:4", "z":"KEDGE:8"}).to_dict()
    results = [result1, result2]
    merge_answer(result_message,answer,results,qnode_ids)
    assert len(result_message["message"]["results"]) == 1
    assert len(result_message["message"]["results"][0]["node_bindings"]) == 2
    assert len(result_message["message"]["results"][0]["analyses"]) == 1
    assert len(result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]) == 1
    # e is the name of the query edge defined in create_result_graph()
    assert "e" in result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]
    kedge_id = result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]["e"][0]
    kedge = result_message["message"]["knowledge_graph"]["edges"][kedge_id]
    assert kedge["subject"] == "PUBCHEM.COMPOUND:789"
    assert kedge["object"] == "MONDO:1234"
    assert kedge["predicate"] == "biolink:treats"
    found = False
    for attribute in kedge["attributes"]:
        if attribute["attribute_type_id"] == "biolink:support_graphs":
            aux_graphs = attribute["value"]
            found = True
    assert found
    assert len(aux_graphs) == 2
    edges = frozenset([ frozenset( result_message["message"]["auxiliary_graphs"][aux_graph_id]["edges"] ) for aux_graph_id in aux_graphs ])
    assert edges == frozenset([frozenset(["KEDGE:1", "KEDGE:2"]), frozenset(["KEDGE:4", "KEDGE:8"])])



def test_create_aux_graph():
    """Given an analysis object with multiple edge bindings, test that create_aux_graph() returns a valid aux graph."""
    eb1 = EdgeBinding(id = 'eb1')
    eb2 = EdgeBinding(id = 'eb2')
    analysis = Analysis(resource_id = "example.com", edge_bindings = {"qedge1":[eb1], "qedge2":[eb2]})
    agid, aux_graph = create_aux_graph(analysis.to_dict())
    assert len(agid) == 36
    assert aux_graph["edges"] == ["eb1", "eb2"]
    #Make sure that we can parse the aux graph
    axg = AuxiliaryGraph.parse_obj(aux_graph)

def test_add_knowledge_edge():
    """Test that add_knowledge_edge() runs without errors."""
    #First make a pydantic query so that we're sure it's valid
    pydantic_message=create_result_graph()
    #Convert it into a dict (as it will be in the real code) and add an edge
    result_message = pydantic_message.to_dict()
    aux_graph_ids = ["ag1", "ag2"]
    answer = "PUBCHEM.COMPOUND:789"
    add_knowledge_edge(result_message, aux_graph_ids, answer)
    #Make sure that the edge was added, that its properties are correct
    assert len(result_message["message"]["knowledge_graph"]["edges"]) == 1
    for edge_id, edge in result_message["message"]["knowledge_graph"]["edges"].items():
        assert edge["subject"] == "PUBCHEM.COMPOUND:789"
        assert edge["object"] == "MONDO:1234"
        assert edge["predicate"] == "biolink:treats"
        assert edge["attributes"] == [
            {
                "attribute_type_id": "biolink:support_graphs",
                "value": aux_graph_ids
            }
        ]
    #Does it validate?
    check_message = Response.parse_obj(result_message)
