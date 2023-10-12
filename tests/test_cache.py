import pytest

from src.service_aggregator import match_results_to_query

def test_match_results_to_query():
    query_message = {
        "message": {
            "query_graph": {
                "nodes": {
                    "on": {
                        "ids": [
                            "MONDO:0008029"
                        ]
                    },
                    "sn": {
                        "categories": [
                            "biolink:ChemicalEntity"
                        ]
                    }
                },
                "edges": {
                    "qedge": {
                        "subject": "sn",
                        "object": "on",
                        "knowledge_type": "inferred",
                        "predicates": [
                            "biolink:treats"
                        ]
                    }
                }
            }
        }
    }
    cached_result = {
    "message": {
        "query_graph": {
            "nodes": {
                "disease": {
                    "ids": [
                        "MONDO:0008029"
                    ]
                },
                "chemical": {
                    "categories": [
                        "biolink:ChemicalEntity"
                    ]
                }
            },
            "edges": {
                "t_edge": {
                    "subject": "chemical",
                    "object": "disease",
                    "knowledge_type": "inferred",
                    "predicates": [
                        "biolink:treats"
                    ]
                }
            }
        },
        "knowledge_graph": {
            "nodes": {},
            "edges": {}
        },
        "results": [
            {
                "node_bindings": {
                    "disease": [
                        {
                            "id": "MONDO:0008029"
                        }
                    ],
                    "chemical": [
                        {
                            "id": "PUBCHEM.COMPOUND:1102"
                        }
                    ]
                },
                "analyses": [
                    {
                        "resource_id": "infores:aragorn",
                        "edge_bindings": {
                            "t_edge": [
                                {
                                    "id": "9057be2ea96e"
                                }
                            ]
                        },
                        "score": 0.5228822269739374
                    }
                ]
            }
        ],
        "auxiliary_graphs": {}
    },
    "logs": [],
    "status": "Success",
    "pid": "b65daeba13b2"
}
    result = match_results_to_query(cached_result, query_message, "sn", "on", "qedge")
    the_result = result["message"]["results"][0]
    assert len(the_result["node_bindings"]) == 2
    assert the_result["node_bindings"]["on"][0]["id"] == "MONDO:0008029"
    assert the_result["node_bindings"]["sn"][0]["id"] == "PUBCHEM.COMPOUND:1102"
    assert len(the_result["analyses"][0]["edge_bindings"]) == 1
    assert the_result["analyses"][0]["edge_bindings"]["qedge"] == [{"id":"9057be2ea96e"}]
