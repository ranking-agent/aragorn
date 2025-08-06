"Handles pathfinder queries"
import asyncio
from collections import defaultdict
import copy
import hashlib
import os
import json

import httpx
import networkx
from reasoner_pydantic import Message

from src.pathfinder.get_cooccurrence import get_the_curies, get_the_pmids
from src.operations import recursive_get_edge_support_graphs

node_norm_url = os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/")
strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/")
NUM_TOTAL_HOPS = 4
TOTAL_PUBS = 27840000
CURIE_PRUNING_LIMIT = 50
LIT_CO_FACTOR = 10000
INFORMATION_CONTENT_THRESHOLD = 85

blocklist = []
with open("blocklist.json", "r") as f:
    blocklist = json.load(f)

async def generate_from_strider(message):
    """Generates knowledge graphs from strider."""
    try:
        async with httpx.AsyncClient(timeout=3600) as client:
            lookup_response = await client.post(
                url=strider_url + "query",
                json=message,
            )
            lookup_response.raise_for_status()
            lookup_response = lookup_response.json()
    except Exception:
        lookup_response = {}
    return lookup_response.get("message", {})


async def get_normalized_curies(curies, guid, logger):
    """Gives us normalized curies that we can look up in our database, assuming
    the database is also properly normalized."""
    async with httpx.AsyncClient(timeout=900) as client:
        try:
            normalizer_response = await client.post(
                url=node_norm_url + "get_normalized_nodes",
                json={"curies": list(curies), "conflate": True, "description": False, "drug_chemical_conflate": True},
            )
            normalizer_response.raise_for_status()
            return normalizer_response.json()
        except Exception:
            logger.info(f"{guid}: Failed to get a response from node norm")


async def shadowfax(message, guid, logger):
    """Processes pathfinder queries. This is done by using literature
    co-occurrence to find nodes that occur in publications with our input
    nodes, then finding paths that connect our input nodes through these
    intermediate nodes."""
    qgraph = message["message"]["query_graph"]
    pinned_node_ids_set = set()
    pinned_node_keys = []
    for node_key, node in qgraph["nodes"].items():
        pinned_node_keys.append(node_key)
        if node.get("ids", None) is not None:
            pinned_node_ids_set.add(node["ids"][0])
    if len(pinned_node_ids_set) != 2:
        logger.error(f"{guid}: Pathfinder queries require two pinned nodes.")
        return message, 500
    pinned_node_ids = list(pinned_node_ids_set)

    intermediate_categories = []
    path_key = next(iter(qgraph["paths"].keys()))
    qpath = qgraph["paths"][path_key]
    if qpath.get("constraints", None) is not None:
        constraints = qpath["constraints"]
        if len(constraints) > 1:
            logger.error(f"{guid}: Pathfinder queries do not support multiple constraints.")
            return message, 500
        if len(constraints) > 0:
            intermediate_categories = constraints[0].get("intermediate_categories", None) or []
        if len(intermediate_categories) > 1:
            logger.error(f"{guid}: Pathfinder queries do not support multiple intermediate categories")
            return message, 500
    else:
        intermediate_categories = ["biolink:NamedThing"]

    normalized_pinned_ids = await get_normalized_curies(pinned_node_ids, guid, logger)
    if normalized_pinned_ids is None:
        normalized_pinned_ids = {}

    source_node = normalized_pinned_ids.get(pinned_node_ids[0], {"id": {"identifier": pinned_node_ids[0]}})["id"]["identifier"]
    source_category = normalized_pinned_ids.get(pinned_node_ids[0], {"type": ["biolink:NamedThing"]})["type"][0]
    source_equivalent_ids = [i["identifier"] for i in normalized_pinned_ids.get(pinned_node_ids[0], {"equivalent_identifiers": []})["equivalent_identifiers"]]
    target_node = normalized_pinned_ids.get(pinned_node_ids[1], {"id": {"identifier": pinned_node_ids[1]}})["id"]["identifier"]
    target_equivalent_ids = [i["identifier"] for i in normalized_pinned_ids.get(pinned_node_ids[1], {"equivalent_identifiers": []})["equivalent_identifiers"]]
    target_category = normalized_pinned_ids.get(pinned_node_ids[1], {"type": ["biolink:NamedThing"]})["type"][0]

    # Find shared publications between input nodes
    source_pubs = len(get_the_pmids([source_node]))
    target_pubs = len(get_the_pmids([target_node]))
    pairwise_pubs = get_the_pmids([source_node, target_node])
    if source_pubs == 0 or target_pubs == 0 or len(pairwise_pubs) == 0:
        logger.info(f"{guid}: No publications found.")
        return message, 200

    # Find other nodes from those shared publications
    curies = set()
    for pub in pairwise_pubs:
        curie_list = get_the_curies(pub)
        for curie in curie_list:
            if curie not in [source_node, target_node] and curie not in source_equivalent_ids and curie not in target_equivalent_ids:
                curies.add(curie)

    if len(curies) == 0:
        logger.info(f"{guid}: No curies found.")
        return message, 200

    normalizer_response = await get_normalized_curies(list(curies), guid, logger)
    if normalizer_response is None:
        logger.error(f"{guid}: Failed to get a good response from Node Normalizer")
        return message, 500

    curie_info = defaultdict(dict)
    for curie, normalizer_info in normalizer_response.items():
        if normalizer_info:
            if (normalizer_info.get("information_content", 101) > INFORMATION_CONTENT_THRESHOLD) and curie not in blocklist:
                curie_info[curie]["categories"] = normalizer_info.get("type", ["biolink:NamedThing"])
                cooc = len(get_the_pmids([curie, source_node, target_node]))
                num_pubs = len(get_the_pmids([curie]))
                curie_info[curie]["pubs"] = num_pubs
                curie_info[curie]["score"] = max(
                    0,
                    ((cooc / TOTAL_PUBS) - (source_pubs / TOTAL_PUBS) * (target_pubs / TOTAL_PUBS) * (num_pubs / TOTAL_PUBS))
                )

    # Find the nodes with most significant co-occurrence
    pruned_curies = []
    while len(pruned_curies) < CURIE_PRUNING_LIMIT and len(pruned_curies) < len(curies):
        max_cov = 0
        for info in curie_info.values():
            max_cov = max(info["score"], max_cov)
        for curie, info in curie_info.items():
            if info["score"] == max_cov:
                pruned_curies.append(curie)
                info["score"] = -1

    node_category_mapping = defaultdict(list)
    node_category_mapping[source_category].append(source_node)
    node_category_mapping[target_category].append(target_node)
    for curie in pruned_curies:
        node_category_mapping[curie_info[curie]["categories"][0]].append(curie)

    lookup_nodes = {}
    for category, category_curies in node_category_mapping.items():
        lookup_nodes[category.removeprefix("biolink:")] = {"ids": category_curies, "categories": [category]}

    # Create queries matching each category to each other
    strider_multiquery = []
    for subject_index, subject_category in enumerate(lookup_nodes.keys()):
        for object_category in list(lookup_nodes.keys())[(subject_index + 1) :]:
            lookup_edge = {"subject": subject_category, "object": object_category, "predicates": ["biolink:related_to"]}
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {subject_category: lookup_nodes[subject_category], object_category: lookup_nodes[object_category]},
                        "edges": {"e0": lookup_edge},
                    }
                }
            }
            strider_multiquery.append(m)
    lookup_messages = []
    logger.debug(f"{guid}: Sending {len(strider_multiquery)} requests to strider.")
    # We will want to use multistrider here eventually
    for lookup_message in await asyncio.gather(*[generate_from_strider(lookup_query) for lookup_query in strider_multiquery]):
        if lookup_message:
            lookup_message["query_graph"] = {"nodes": {}, "edges": {}}
            lookup_messages.append(lookup_message)
    logger.debug(f"{guid}: Received {len(lookup_messages)} responses from strider.")

    merged_lookup_message = Message.parse_obj(lookup_messages[0])
    lookup_results = []
    for lookup_message in lookup_messages:
        # Build graph from results to avoid subclass loops
        # Results do not concatenate when they have different qnode ids
        lookup_message_obj = Message.parse_obj(lookup_message)
        merged_lookup_message.update(lookup_message_obj)
        lookup_results.extend(lookup_message_obj.dict().get("results") or [])
    merged_lookup_message_dict = merged_lookup_message.dict()
    lookup_knowledge_graph = merged_lookup_message_dict.get("knowledge_graph", {"nodes": {}, "edges": {}})
    lookup_aux_graphs = merged_lookup_message_dict.get("auxiliary_graphs", {})

    non_support_edges = []
    for edge_key, edge in lookup_knowledge_graph["edges"].items():
        add_edge = True
        for support_graph in lookup_aux_graphs.values():
            if edge_key in support_graph and edge["predicate"] == "biolink:subclass_of":
                # see if we can get paths from non subclass edges used in support graphs
                add_edge = False
        if add_edge:
            non_support_edges.append(edge_key)

    # Build a large kg and find paths
    path_graph = networkx.Graph()
    path_graph.add_nodes_from([source_node, target_node])
    for edge_key in non_support_edges:
        edge = lookup_knowledge_graph["edges"][edge_key]
        e_subject = edge["subject"]
        e_object = edge["object"]
        if path_graph.has_edge(e_subject, e_object):
            path_graph[e_subject][e_object]["keys"].append(edge_key)
        else:
            path_graph.add_edge(e_subject, e_object, keys=[edge_key])

    paths = networkx.all_simple_paths(path_graph, source_node, target_node, NUM_TOTAL_HOPS)
    num_paths = 0
    result_paths = []
    for path in paths:
        num_paths += 1
        fits_constraint = False
        for curie in path:
            if curie not in [source_node, target_node]:
                # Handles constraints, behavior may change depending decision
                # handling multiple constraints
                if intermediate_categories[0] in lookup_knowledge_graph["nodes"].get(curie, {}).get("categories", []):
                    fits_constraint = True
        if fits_constraint:
            result_paths.append(path)

    # Build knowledge graph from paths
    aux_graphs = {}

    result = {
        "node_bindings": {pinned_node_keys[0]: [{"id": source_node, "attributes": []}], pinned_node_keys[1]: [{"id": target_node, "attributes": []}]},
        "analyses": [],
    }

    for path in result_paths:
        aux_edges = []
        aux_edges_keys = []
        path_edges, path_support_graphs, path_nodes = set(), set(), set()
        support_node_graph_mapping = defaultdict(set)
        support_node_edge_mapping = defaultdict(set)
        missing_hop = False
        hop_count = 0
        hop_edge_map = defaultdict(set)
        for i, node in enumerate(path[:-1]):
            hop_count += 1
            next_node = path[i + 1]
            single_aux_edges = set()
            for kedge_key in path_graph[node][next_node]["keys"]:
                single_edges, single_support_graphs, single_nodes = set(), set(), set()
                # get nodes and edges from path
                try:
                    single_edges, single_support_graphs, single_nodes = recursive_get_edge_support_graphs(
                        kedge_key,
                        single_edges,
                        single_support_graphs,
                        lookup_knowledge_graph["edges"],
                        lookup_aux_graphs,
                        single_nodes,
                        guid
                    )
                except KeyError as e:
                    logger.warning(e)
                    continue
                for single_support_graph in single_support_graphs:
                    # map support graphs to component nodes
                    for edge_id in lookup_aux_graphs[single_support_graph]["edges"]:
                        support_node_graph_mapping[lookup_knowledge_graph["edges"][edge_id]["subject"]].add(single_support_graph)
                        support_node_graph_mapping[lookup_knowledge_graph["edges"][edge_id]["object"]].add(single_support_graph)
                for edge_id in single_edges:
                    support_node_edge_mapping[lookup_knowledge_graph["edges"][edge_id]["subject"]].add(edge_id)
                    support_node_edge_mapping[lookup_knowledge_graph["edges"][edge_id]["object"]].add(edge_id)
                    hop_edge_map[hop_count].add(edge_id)
                check = True
                for path_node in single_nodes:
                    if path_node != node and path_node in path_nodes:
                        # if a node is repeated, remove all associated edges from path
                        check = False
                        for single_support_graph in support_node_graph_mapping[path_node]:
                            if single_support_graph in path_support_graphs:
                                path_support_graphs.remove(single_support_graph)
                        for edge_id in support_node_edge_mapping[path_node]:
                            if edge_id in path_edges:
                                path_edges.remove(edge_id)
                            if edge_id in aux_edges:
                                aux_edges.remove(edge_id)
                            for hop_edges in hop_edge_map.values():
                                if edge_id in hop_edges:
                                    hop_edges.remove(edge_id)
                            
                if check:
                    path_edges.update(single_edges)
                    path_support_graphs.update(single_support_graphs)
                    # Don't add the current node in since we have more edges for this hop
                    path_nodes.update({single_node for single_node in single_nodes if (single_node != node and single_node != next_node)})
                    single_aux_edges.add(kedge_key)

            # Now add node in to check for repeats later
            path_nodes.add(node)

            # check if we have completely removed all edges from one of the hops in a path
            if len(single_aux_edges) == 0:
                missing_hop = True
            for hop_edges in hop_edge_map.values():
                if len(hop_edges) == 0:
                    missing_hop = True

            if missing_hop:
                break
            aux_edges.extend(single_aux_edges)
        
        # If a hop is missing, path is no longer valid, cannot be added
        if missing_hop:
            continue
        
        sha256 = hashlib.sha256()
        for x in set(aux_edges):
            sha256.update(bytes(x, encoding="utf-8"))
        aux_graph_key = sha256.hexdigest()
        if aux_graph_key not in aux_edges_keys:
            lookup_aux_graphs[aux_graph_key] = {"edges": list(aux_edges), "attributes": []}
            aux_edges_keys.append(aux_graph_key)

        analysis = {
            "resource_id": "infores:aragorn",
            "path_bindings": {
                path_key: [{"id": aux_graph_key, "attributes": []}],
            },
        }
        result["analyses"].append(analysis)

    result_message = {
        "message": {
            "query_graph":  message["message"]["query_graph"],
            "knowledge_graph": lookup_knowledge_graph,
            "results": [result],
            "auxiliary_graphs": lookup_aux_graphs
        }
    }

    return result_message, 200
