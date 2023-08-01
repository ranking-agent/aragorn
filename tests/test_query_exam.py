import pytest
from src.service_aggregator import create_aux_graph, add_knowledge_edge, merge_results_by_node, filter_repeated_nodes
from reasoner_pydantic.results import Analysis, EdgeBinding, Result, NodeBinding
from reasoner_pydantic.auxgraphs import AuxiliaryGraph
from reasoner_pydantic.message import Response
import json

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

def create_pretend_knowledge_edge(subject, object, predicate, infores):
    """Create a pretend knowledge edge."""
    ke = {"subject":subject, "object":object, "predicate":predicate, "sources":[{"resource_id":infores, "resource_role": "primary_knowledge_source"}]}
    return ke

def test_merge_answer_creative_only():
    """Test that merge_answer() puts all the aux graphs in the right places."""
    pydantic_result = create_result_graph()
    result_message = pydantic_result.to_dict()
    answer = "PUBCHEM.COMPOUND:789"
    qnode_ids = ["input", "output"]
    result1 = create_result({"input":"MONDO:1234", "output":answer, "node2": "curie:3"}, {"g":"KEDGE:1", "f":"KEDGE:2"}).to_dict()
    result2 = create_result({"input":"MONDO:1234", "output":answer, "nodeX": "curie:8"}, {"q":"KEDGE:4", "z":"KEDGE:8"}).to_dict()
    results = [result1, result2]

    #In reality the results will be in the message and we want to be sure that they get cleared out.
    result_message["message"]["results"] = results
    merge_results_by_node(result_message,"output",[])
    assert len(result_message["message"]["results"]) == 1
    assert len(result_message["message"]["results"][0]["node_bindings"]) == 2
    assert len(result_message["message"]["results"][0]["analyses"]) == 1
    assert len(result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]) == 1
    # e is the name of the query edge defined in create_result_graph()
    assert "e" in result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]
    kedge_id = result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]["e"][0]["id"]
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
    Response.parse_obj(result_message)

def test_merge_answer_lookup_only():
    """Test that merge_answer() puts all the aux graphs in the right places."""
    pydantic_result = create_result_graph()
    result_message = pydantic_result.to_dict()
    answer = "PUBCHEM.COMPOUND:789"
    qnode_ids = ["input", "output"]
    result1 = create_result({"input":"MONDO:1234", "output":answer}, {"e":"lookup:1"}).dict(exclude_none=True)
    result2 = create_result({"input":"MONDO:1234", "output":answer}, {"e":"lookup:2"}).dict(exclude_none=True)
    for n, ke_id in enumerate(["lookup:1", "lookup:2"]):
        ke = create_pretend_knowledge_edge("MONDO:1234", answer, "biolink:treats", f"infores:i{n}")
        result_message["message"]["knowledge_graph"]["edges"][ke_id] = ke
    lookup_results = [result1, result2]
    result_message["message"]["results"] = []
    merge_results_by_node(result_message,"output",lookup_results)
    assert len(result_message["message"]["results"]) == 1
    assert len(result_message["message"]["results"][0]["node_bindings"]) == 2
    assert len(result_message["message"]["results"][0]["analyses"]) == 1
    assert len(result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]) == 1
    # e is the name of the query edge defined in create_result_graph()
    assert "e" in result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]
    assert result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]["e"] == [{"id":"lookup:1"}, {"id":"lookup:2"}]
    assert "auxiliary_graphs" not in result_message
    Response.parse_obj(result_message)

def test_merge_answer_creative_and_lookup():
    """Test that merge_answer() puts all the aux graphs in the right places."""
    pydantic_result = create_result_graph()
    result_message = pydantic_result.to_dict()
    answer = "PUBCHEM.COMPOUND:789"
    qnode_ids = ["input", "output"]
    result1 = create_result({"input":"MONDO:1234", "output":answer, "node2": "curie:3"}, {"g":"KEDGE:1", "f":"KEDGE:2"}).to_dict()
    result2 = create_result({"input":"MONDO:1234", "output":answer, "nodeX": "curie:8"}, {"q":"KEDGE:4", "z":"KEDGE:8"}).to_dict()
    results = [result1, result2]
    lookup = [create_result({"input":"MONDO:1234", "output":answer}, {"e":"lookup:1"}).dict(exclude_none=True)]
    for n, ke_id in enumerate(["lookup:1"]):
        ke = create_pretend_knowledge_edge("MONDO:1234", answer, "biolink:treats", f"infores:i{n}")
        result_message["message"]["knowledge_graph"]["edges"][ke_id] = ke
    #In reality the results will be in the message and we want to be sure that they get cleared out.
    result_message["message"]["results"] = results
    merge_results_by_node(result_message,"output",lookup)
    assert len(result_message["message"]["results"]) == 1
    assert len(result_message["message"]["results"][0]["node_bindings"]) == 2
    assert len(result_message["message"]["results"][0]["analyses"]) == 1
    assert len(result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]) == 1
    # e is the name of the query edge defined in create_result_graph()
    assert "e" in result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]
    #There will be a binding to 2 kedges; 1 is the lookup and the other is creative
    assert len(result_message["message"]["results"][0]["analyses"][0]["edge_bindings"]["e"]) == 2
    Response.parse_obj(result_message)

def create_3hop_query () -> Response:
    """Create a 3-hop query graph."""
    query_graph = { "nodes": {  "chemical": { "categories": [ "biolink:ChemicalEntity" ] },
                                "disease": { "ids": [ "MONDO:0015626" ], "categories": [ "biolink:DiseaseOrPhenotypicFeature" ] },
                                "i": { "categories": [ "biolink:BiologicalProcessOrActivity" ] },
                                "e": { "categories": [ "biolink:ChemicalEntity" ] } },
                    "edges": {  "edge_0": { "subject": "i", "object": "chemical", "predicates": [ "biolink:has_input" ] },
                                "edge_1": { "subject": "i", "object": "e", "predicates": [ "biolink:has_input" ] },
                                "edge_2": { "subject": "e", "object": "disease", "predicates": [ "biolink:treats" ] } } }
    result_message = {"query_graph": query_graph, "knowledge_graph": {"nodes": {}, "edges": {}}, "auxiliary_graphs": set(), "results": []}
    pydantic_message = Response(**{"message": result_message})
    return pydantic_message


@pytest.mark.asyncio
async def test_filter_repeats():
    """Create a 3 hop query with 2 results.  One of them is 4 separate nodes, the other has 2 nodes that are the same.
    Make sure that the result with the repeated node is filtered out, along with its nodes and edges."""
    message = create_3hop_query().dict(exclude_none=True)
    #results are (diseaes) edge_2 (e) edge_1 (i) edge_0 (chemical)
    #all the nodes are different and the edges are different
    good_result = create_result({"i":"GO:keep", "chemical":"PUBCHEM.COMPOUND:1", "e":"PUBCHEM.COMPOUND:2", "disease":"MONDO:1"},
                                {"edge_0":"keep:0", "edge_1":"keep:1", "edge_2":"keep:2"}).dict(exclude_none=True)
    # in this one the final chemical is the same as e from the good result, so edge 0 and edge 1 are the same and same as
    # the first edge 1.  Edge 2 is also the same as edge 2 in the good result
    bad_result = create_result({"i":"GO:keep", "chemical":"PUBCHEM.COMPOUND:1", "e":"PUBCHEM.COMPOUND:1", "disease":"MONDO:1"},
                               {"edge_0":"keep:1", "edge_1":"keep:1", "edge_2":"keep:2"}).dict(exclude_none = True)
    # this one has all different chemical & i than the first, but it does have the repeat, so everything should be removed
    other_bad_result = create_result({"i":"GO:remove", "chemical":"PUBCHEM.COMPOUND:3", "e":"PUBCHEM.COMPOUND:3", "disease":"MONDO:1"},
                                     {"edge_0":"remove:0", "edge_1":"remove:0", "edge_2":"remove:1"}).dict(exclude_none = True)
    message["message"]["results"] = [good_result, bad_result, other_bad_result]
    for node_id in ["PUBCHEM.COMPOUND:1", "PUBCHEM.COMPOUND:2", "PUBCHEM.COMPOUND:3", "MONDO:1", "GO:keep", "GO:remove"]:
        message["message"]["knowledge_graph"]["nodes"][node_id] = {}
    for edge_id in ["keep:0", "keep:1", "keep:2", "remove:0", "remove:1"]:
        message["message"]["knowledge_graph"]["edges"][edge_id] = {}
    await filter_repeated_nodes(message, "guid")
    assert len(message["message"]["results"]) == 1
    assert len(message["message"]["knowledge_graph"]["nodes"]) == 4
    assert len(message["message"]["knowledge_graph"]["edges"]) == 3

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
    add_knowledge_edge(result_message, aux_graph_ids, answer, True)
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
