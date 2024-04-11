import pytest
from src.operations import filter_kgraph_orphans, sort_results_score
from reasoner_pydantic import Response

def create_message():
    """Create a query graph for testing."""
    query_graph = {
        "nodes": {"input": {"ids": ["MONDO:1234"]}, "output": {"categories": ["biolink:ChemicalEntity"]}},
        "edges": {"e": {"subject": "output", "object": "input", "predicates": ["biolink:treats"]}}}
    result_message = {"query_graph": query_graph,
        "knowledge_graph": {"nodes": {"MONDO:1234":{"categories":["biolink:Disease"], "attributes": []}}, "edges": {}},
        "auxiliary_graphs": {}, "results": []}
    return result_message

def add_new_node(message, new_nodes):
    newnode_id = f"node_{len(message['knowledge_graph']['nodes'])}"
    node = {"categories": ["biolink:NamedThing"], "attributes": []}
    message["knowledge_graph"]["nodes"][newnode_id] = node
    new_nodes.add(newnode_id)
    return newnode_id

def add_new_edge(message, new_edges, subject, object):
    new_edge_id = f"edge_{len(message['knowledge_graph']['edges'])}"
    new_edge = {"subject": subject, "object": object, "predicate": "biolink:blahblah",
                "sources": [{"resource_id": "infores:madeup", "resource_role":"primary_knowledge_source"}],
                "attributes": []}
    message["knowledge_graph"]["edges"][new_edge_id] = new_edge
    new_edges.add(new_edge_id)
    return new_edge_id

def add_new_auxgraph(message, new_auxgraphs, edge1_id, edge2_id):
    new_auxgraph_id = f"auxgraph_{len(message['auxiliary_graphs'])}"
    new_auxgraph = {"edges": [edge1_id, edge2_id], "attributes": []}
    message["auxiliary_graphs"][new_auxgraph_id] = new_auxgraph
    return new_auxgraph_id

def add_two_hop(message, new_edges, new_nodes, new_auxgraphs, subject, object):
    """Create a node and two edges connecting it to the subject and object.
    Put the edges into a new auxiliary graph and return its id"""
    intermediate_node_id = add_new_node(message, new_nodes)
    edge1_id = add_new_edge(message, new_edges, subject, intermediate_node_id)
    edge2_id = add_new_edge(message, new_edges, intermediate_node_id, object)
    new_auxgraph_id = add_new_auxgraph(message, new_auxgraphs, edge1_id, edge2_id)
    return new_auxgraph_id

def add_support_graph_to_edge(message, new_auxgraph_id, new_edge_id):
    parent_edge = message["knowledge_graph"]["edges"][new_edge_id]
    parent_edge["attributes"] = [{"attribute_type_id": "biolink:support_graphs", "value": [new_auxgraph_id]}]

def add_result(message, scores=[0.5]):
    """Given a message, add a result.  The result should have analyses.  Each analysis will have a single
     edge binding. The KG edge will have an auxgraph, specifying a 2 hop, adding an extra node and 2 edges to
     KG.  Each analysis will have a score. Each analysis will also have an auxgraph, connecting nodes in the related edges."""
    DISEASE = "MONDO:1234"
    new_edges = set()
    new_nodes = set()
    new_auxgraphs = set()
    #Add the result with a new answer node
    newnode_id = add_new_node(message, new_nodes)
    result = {"node_bindings": {"input":[{"id":DISEASE, "attributes": []}], "output": [{"id": newnode_id, "attributes":[]} ]}, "analyses": []}
    message["results"].append(result)
    for score in scores:
        #Make an analysis, add an edge_binding to a new creative edge
        new_edge_id = add_new_edge(message, new_edges, newnode_id, DISEASE)
        analysis = {"edge_bindings": {"e": [{"id": new_edge_id, "attributes": []}]}, "score": score, "resource_id": "infores:madeup"}
        #Add a support graph to the creative edge
        new_auxgraph_id = add_two_hop(message, new_edges, new_nodes, new_auxgraphs,  DISEASE, newnode_id)
        add_support_graph_to_edge(message, new_auxgraph_id, new_edge_id)
        #Add a support graph to the analysis
        new_auxgraph_id = add_two_hop(message, new_edges, new_nodes, new_auxgraphs,  DISEASE, newnode_id)
        analysis["support_graphs"] = [new_auxgraph_id]
        #Put the anslysis in the result
        result["analyses"].append(analysis)
    return new_nodes, new_edges, new_auxgraphs



@pytest.mark.asyncio
async def test_deorphaning():
    """Given two original results, suppose one is filtered. Make sure that the correct edges and nodes are
    removed from the KG.  We also want to remove auxiliary graphs that are no longer referenced."""
    message = create_message()
    result_1_nodes, result_1_edges, result_1_auxgraphs = add_result(message)
    result_2_nodes, result_2_edges, result_2_auxgraphs = add_result(message)
    assert len(result_1_nodes) == 1 + 1 + 1 # The result node + 1 for the analysis aux graph + 1 for the edge aux graph
    assert len(result_2_nodes) == 1 + 1 + 1 # The result node + 1 for the analysis aux graph + 1 for the edge aux graph
    #Now remove the first result
    message["results"] = message["results"][1:]
    #remove orphans
    response = {"message": message, "logs": []}
    pdresponse = Response(**response)
    await filter_kgraph_orphans(response,{}, "")
    #Make sure that the nodes and edges from the first result are gone
    message = response["message"]
    for node_id in result_1_nodes:
        assert node_id not in message["knowledge_graph"]["nodes"]
    for edge_id in result_1_edges:
        assert edge_id not in message["knowledge_graph"]["edges"]
    for auxgraph_id in result_1_auxgraphs:
        assert auxgraph_id not in message["auxiliary_graphs"]
    #Make sure that the nodes and edges from the second result are still there
    for node_id in result_2_nodes:
        assert node_id in message["knowledge_graph"]["nodes"]
    for edge_id in result_2_edges:
        assert edge_id in message["knowledge_graph"]["edges"]
    for auxgraph_id in result_2_auxgraphs:
        assert auxgraph_id in message["auxiliary_graphs"]
    #The total number of nodes should also include the 1 input node
    assert len(message["knowledge_graph"]["nodes"]) == len(result_2_nodes) + 1


@pytest.mark.asyncio
async def test_sorting():
    """Given a set of results, make sure that they are sorted correctly by score"""
    message = create_message()
    result_1_nodes, result_1_edges, result_1_auxgraphs = add_result(message,scores = [0.5])
    result_2_nodes, result_2_edges, result_2_auxgraphs = add_result(message, scores = [0.9])
    response = {"message": message}
    #Sort should put result 2 first
    outm,s= await sort_results_score(response,params={},guid='xyz')
    assert s==200
    ids = [r['node_bindings']["output"][0]["id"] for r in outm['message']['results']]
    assert(ids[0] in result_2_nodes)
    assert(ids[1] in result_1_nodes)
    outm2,s = await sort_results_score(outm, params={'ascending_or_descending':'ascending'},guid='zyx')
    ids = [r['node_bindings']["output"][0]["id"] for r in outm2['message']['results']]
    assert(ids[0] in result_1_nodes)
    assert(ids[1] in result_2_nodes)
    outm3,s = await sort_results_score(outm2, params={'ascending_or_descending':'descending'},guid='zyx')
    ids = [r['node_bindings']["output"][0]["id"] for r in outm3['message']['results']]
    assert(ids[0] in result_2_nodes)
    assert(ids[1] in result_1_nodes)

@pytest.mark.asyncio
async def test_sorting_multiple_analyses():
    """Given a set of results, make sure that they are sorted correctly by maximum score across analyses"""
    message = create_message()
    result_1_nodes, _, __ = add_result(message,scores = [0.5])
    result_2_nodes, _, __ = add_result(message, scores = [0.1,0.9])
    result_3_nodes, _, __ = add_result(message, scores = [0.2,0.1])
    result_4_nodes, _, __ = add_result(message, scores = [0.8,0.6])
    response = {"message": message}
    outm,s= await sort_results_score(response,params={},guid='xyz')
    assert s==200
    ids = [r['node_bindings']["output"][0]["id"] for r in outm['message']['results']]
    assert(ids[0] in result_2_nodes)
    assert(ids[1] in result_4_nodes)
    assert(ids[2] in result_1_nodes)
    assert(ids[3] in result_3_nodes)
