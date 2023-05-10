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
    inferred, qnode, anode =  examine_query(query)
    assert inferred
    assert qnode == 'disease'
    assert anode == 'chemical'