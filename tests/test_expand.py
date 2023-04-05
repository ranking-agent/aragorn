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
    assert len(m) > 20 #This depends on how many rules we're allowing

def test_expand_qualified_query():
    q = {
        "message": {
            "query_graph": {
                "nodes": {
                    "gene": {
                        "categories": [
                            "biolink:Gene"
                        ],
                        "ids": [
                            "NCBIGene:23162"
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
                        "object": "gene",
                        "subject": "chemical",
                        "predicates": [
                            "biolink:affects"
                        ],
                        "knowledge_type": "inferred",
                        "qualifier_constraints": [
                            {
                                "qualifier_set": [
                                    {
                                        "qualifier_type_id": "biolink:object_aspect_qualifier",
                                        "qualifier_value": "activity_or_abundance"
                                    },
                                    {
                                        "qualifier_type_id": "biolink:object_direction_qualifier",
                                        "qualifier_value": "increased"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }
    m = expand_query(q,{},"abcd")
    assert len(m) > 20 #This depends on how many rules we're allowing