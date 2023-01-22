import pytest
from src.service_aggregator import expand_query

def test_expand_query():
    q = {
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
    m = expand_query(q,{},"abcd")
    assert len(m) == 101 #This depends on how many rules we're allowing