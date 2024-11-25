import asyncio
from collections import defaultdict
import copy
import uuid
import hashlib
import os
import networkx
import httpx

from reasoner_pydantic import Message

from src.pathfinder.get_cooccurrence import get_the_curies, get_the_pmids

node_norm_url = os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/")
strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/")
num_intermediate_hops = 3
TOTAL_PUBS = 27840000

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
    except:
        lookup_response = {}
    return lookup_response.get("message", {})


async def get_normalized_curies(curies, guid, logger):
    """Gives us normalized curies that we can look up in our database, assuming
    the database is also properly normalized."""
    async with httpx.AsyncClient(timeout=900) as client:
        try:
            normalizer_response = await client.post(
                url=node_norm_url + "get_normalized_nodes",
                json={
                    "curies": list(curies),
                    "conflate": True,
                    "description": False,
                    "drug_chemical_conflate": True
                }
            )
            normalizer_response.raise_for_status()
            return normalizer_response.json()
        except:
            logger.info(f"{guid}: Failed to get a response from node norm")


async def shadowfax(message, guid, logger):
    """Processes pathfinder queries. This is done by using literature co-occurrence
    to find nodes that occur in publications with our input nodes, then finding
    paths that connect our input nodes through these intermediate nodes."""
    qgraph = message["message"]["query_graph"]
    pinned_node_ids = []
    for node in qgraph["nodes"].values():
        if node.get("ids", None) is not None:
            pinned_node_ids.append(node["ids"][0])
        else:
            unpinned_node_category = node.get("categories", ["biolink:NamedThing"])[0]
    if len(pinned_node_ids) != 2:
        logger.info(f"{guid}: Pathfinder queries require two pinned nodes.")
        return message, 500
    
    normalized_pinned_ids = await get_normalized_curies(pinned_node_ids, guid, logger)

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

    curie_info = defaultdict(dict)
    for curie, normalizer_info in normalizer_response.items():
        if normalizer_info:
            curie_info[curie]["categories"] = normalizer_info.get("type", ["biolink:NamedThing"])
            cooc = len(get_the_pmids([curie, source_node, target_node]))
            num_pubs = len(get_the_pmids([curie]))
            curie_info[curie]["pubs"] = num_pubs
            curie_info[curie]["score"] = max(0, ((cooc / TOTAL_PUBS) -
                                                 (source_pubs / TOTAL_PUBS) *
                                                 (target_pubs / TOTAL_PUBS) *
                                                 (num_pubs / TOTAL_PUBS)) *
                                                 (normalizer_info.get(
                                                     "information_content", 1
                                                    )
                                                 / 10000))
    
    # Find the nodes with most significant co-occurrence
    pruned_curies = []
    while len(pruned_curies) < 50 and len(pruned_curies) < len(curies):
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
        lookup_nodes[category.removeprefix("biolink:")] = {
            "ids": category_curies,
            "categories": [category]
        }
    
    # Create queries matching each category to each other
    strider_multiquery = []
    for subject_index, subject_category in enumerate(lookup_nodes.keys()):
        for object_category in list(lookup_nodes.keys())[(subject_index + 1):]:
            lookup_edge = {
                "subject": subject_category,
                "object": object_category,
                "predicates": [
                    "biolink:related_to"
                ]
            }
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {
                            subject_category: lookup_nodes[subject_category],
                            object_category: lookup_nodes[object_category]
                        },
                        "edges": {
                            "e0": lookup_edge
                        }
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

    # Build a large kg and find paths
    kg = networkx.Graph()
    kg.add_nodes_from([source_node, target_node])
    for lookup_result in lookup_results:
        for edge_binding in lookup_result["analyses"][0]["edge_bindings"]["e0"]:
            edge_key = edge_binding["id"]
            edge = lookup_knowledge_graph["edges"][edge_key]
            e_subject = edge["subject"]
            e_object = edge["object"]

            if e_subject not in [source_node, target_node] or e_object not in [source_node, target_node]:
                if kg.has_edge(e_subject, e_object):
                    kg[e_subject][e_object]["keys"].append(edge_key)
                else:
                    kg.add_edge(e_subject, e_object, keys=[edge_key])

    paths = networkx.all_simple_paths(kg, source_node, target_node, 4)
    num_paths = 0
    curie_path_mapping = defaultdict(list)
    for path in paths:
        num_paths += 1
        for curie in path:
            if curie not in [source_node, target_node] and curie in curie_info.keys():
                # Handles constraints
                if unpinned_node_category in curie_info[curie]["categories"]:
                    curie_path_mapping[curie].append(path)

    # Build knowledge graph from paths
    results = []
    aux_graphs = {}
    knowledge_graph = {"nodes": copy.deepcopy(lookup_knowledge_graph["nodes"]), "edges": {}}
    for node_key, node in qgraph["nodes"].items():
        if node.get("ids", None) is not None:
            if pinned_node_ids[0] in node["ids"]:
                source_node_key = node_key
            elif pinned_node_ids[1] in node["ids"]:
                target_node_key = node_key
        else:
            unpinned_node_key = node_key
    for edge_key, edge in qgraph["edges"].items():
        if unpinned_node_key == edge["subject"] or unpinned_node_key == edge["object"]:
            if source_node_key == edge["subject"] or source_node_key == edge["object"]:
                before_edge_key = edge_key
            elif target_node_key == edge["subject"] or target_node_key == edge["object"]:
                after_edge_key = edge_key
        else:
            main_edge_key = edge_key
    
    # Builds results from paths according to structure
    for curie, curie_paths in curie_path_mapping.items():
        aux_edges_list = []
        before_curie_edges_list = []
        after_curie_edges_list = []
        for path in curie_paths:
            aux_edges = []
            before_curie_edges = []
            after_curie_edges = []
            before_curie = True
            for i, node in enumerate(path[:-1]):
                next_node = path[i+1]
                if node == curie:
                    before_curie = False
                for kedge_key in kg[node][next_node]["keys"]:
                    edge = lookup_knowledge_graph["edges"][kedge_key]
                    if (node in edge["subject"] or node in edge["object"]) and (next_node in edge["subject"] or next_node in edge["object"]):
                        knowledge_graph["edges"][kedge_key] = edge
                        # Handles support graphs from subclassing
                        for attribute in edge["attributes"]:
                            if attribute.get("attribute_type_id") == "biolink:support_graphs":
                                for support_graph in attribute.get("value", []):
                                    aux_graphs[support_graph] = lookup_aux_graphs[support_graph]
                                    for support_edge in lookup_aux_graphs[support_graph].get("edges", []):
                                        knowledge_graph["edges"][support_edge] = lookup_knowledge_graph["edges"][support_edge]
                        if kedge_key not in aux_edges:
                            aux_edges.append(kedge_key)
                        if before_curie and kedge_key not in before_curie_edges:
                            # these edges come before the intermediate node
                            before_curie_edges.append(kedge_key)
                        elif kedge_key not in after_curie_edges:
                            # these edges come after the intermediate node
                            after_curie_edges.append(kedge_key)
            aux_edges_list.append(aux_edges)
            before_curie_edges_list.append(before_curie_edges)
            after_curie_edges_list.append(after_curie_edges)
        aux_edges_keys = []
        before_curie_edges_keys = []
        after_curie_edges_keys = []
        for aux_edges in aux_edges_list:
            sha256 = hashlib.sha256()
            for x in set(aux_edges):
                sha256.update(bytes(x, encoding="utf-8"))
            aux_edges_key = sha256.hexdigest()
            if aux_edges_key not in aux_edges_keys:
                aux_graphs[aux_edges_key] = {"edges": list(aux_edges), "attributes": []}
                aux_edges_keys.append(aux_edges_key)
        for before_curie_edges in before_curie_edges_list:
            sha256 = hashlib.sha256()
            for x in set(before_curie_edges):
                sha256.update(bytes(x, encoding="utf-8"))
            before_curie_edges_key = sha256.hexdigest()
            if before_curie_edges_key not in before_curie_edges_keys:
                aux_graphs[before_curie_edges_key] = {"edges": list(before_curie_edges), "attributes": []}
                before_curie_edges_keys.append(before_curie_edges_key)
        for after_curie_edges in after_curie_edges_list:
            sha256 = hashlib.sha256()
            for x in set(after_curie_edges):
                sha256.update(bytes(x, encoding="utf-8"))
            after_curie_edges_key = sha256.hexdigest()
            if after_curie_edges_key not in after_curie_edges_keys:
                aux_graphs[after_curie_edges_key] = {"edges": list(after_curie_edges), "attributes": []}
                after_curie_edges_keys.append(after_curie_edges_key)
        main_edge = uuid.uuid1().hex
        before_edge = uuid.uuid1().hex
        after_edge = uuid.uuid1().hex
        knowledge_graph["edges"][main_edge] = {
            "subject": source_node,
            "object": target_node,
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:aragorn",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [
                {
                    "attribute_type_id": "biolink:support_graphs",
                    "value": aux_edges_keys
                },
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": "computational_model",
                    "attribute_source": "infores:aragorn"
                },
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "value": "prediction",
                    "attribute_source": "infores:aragorn"
                }
            ]
        }
        knowledge_graph["edges"][before_edge] = {
            "subject": source_node,
            "object": curie,
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:aragorn",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [
                {
                    "attribute_type_id": "biolink:support_graphs",
                    "value": before_curie_edges_keys
                },
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": "computational_model",
                    "attribute_source": "infores:aragorn"
                },
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "value": "prediction",
                    "attribute_source": "infores:aragorn"
                }
            ]
        }
        knowledge_graph["edges"][after_edge] = {
            "subject": curie,
            "object": target_node,
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:aragorn",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [
                {
                    "attribute_type_id": "biolink:support_graphs",
                    "value": after_curie_edges_keys
                },
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": "computational_model",
                    "attribute_source": "infores:aragorn"
                },
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "value": "prediction",
                    "attribute_source": "infores:aragorn"
                }
            ]
        }
        result = {
            "node_bindings": {
                source_node_key: [
                    {
                        "id": source_node,
                        "attributes": []
                    }
                ],
                unpinned_node_key: [
                    {
                        "id": curie,
                        "attributes": []
                    }
                ],
                target_node_key: [
                    {
                        "id": target_node,
                        "attributes": []
                    }
                ]
            },
            "analyses": [
                {
                    "resource_id": "infores:aragorn",
                    "edge_bindings": {
                        main_edge_key: [
                            {
                                "id": main_edge,
                                "attributes": []
                            }
                        ],
                        before_edge_key: [
                            {
                                "id": before_edge,
                                "attributes": []
                            }
                        ],
                        after_edge_key: [
                            {
                                "id": after_edge,
                                "attributes": []
                            }
                        ]
                    }
                }
            ]
        }
        results.append(result)
    result_message = {
        "message": {
            "query_graph": message["message"]["query_graph"],
            "knowledge_graph": knowledge_graph,
            "results": results,
            "auxiliary_graphs": aux_graphs
        }
    }

    return result_message, 200
