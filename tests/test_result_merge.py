import pytest
from src.service_aggregator import examine_query


def test_query_examination():
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "disease": {
                        "ids": [
                            "MONDO:0011399"
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
                        "object": "disease",
                        "subject": "chemical",
                        "predicates": [
                            "biolink:treats"
                        ],
                        "knowledge_type": "inferred"
                    }
                }
            }
        }
    }
    inferred, qnode, anode, pathfinder = examine_query(query)
    assert inferred
    assert qnode == 'disease'
    assert anode == 'chemical'
    assert not pathfinder

def test_query_examination_pathfinder():
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {
                        "ids": [
                            "PUBCHEM.COMPOUND:5291"
                        ],
                        "name": "imatinib"
                    },
                    "n1": {
                        "ids": [
                            "MONDO:0004979"
                        ],
                        "name": "asthma"
                    },
                    "un": {
                        "categories": [
                            "biolink:NamedThing"
                        ]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": [
                            "biolink:related_to"
                        ],
                        "knowledge_type": "inferred"
                    },
                    "e1": {
                        "subject": "n0",
                        "object": "un",
                        "predicates": [
                            "biolink:related_to"
                        ],
                        "knowledge_type": "inferred"
                    },
                    "e2": {
                        "subject": "n1",
                        "object": "un",
                        "predicates": [
                            "biolink:related_to"
                        ],
                        "knowledge_type": "inferred"
                    }
                }
            },
            "knowledge_graph": {
                "nodes": {},
                "edges": {}
            },
            "results": []
        }
    }
    inferred, qnode, anode, pathfinder = examine_query(query)
    assert not inferred
    assert pathfinder