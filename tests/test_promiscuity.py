import pytest

from src.service_aggregator import get_promiscuous_qnodes

@pytest.mark.asyncio
async def test_qgraph():
    test_graph = {
        "message": {
            "query_graph": {
            "nodes": {
                "$source": {
                    "ids": [
                        "$source_id"
                    ],
                    "categories": [
                        "biolink:ChemicalEntity"
                    ]
                },
                "$target": {
                    "ids": [
                        "$target_id"
                    ],
                    "categories": [
                        "biolink:DiseaseOrPhenotypicFeature"
                    ]
                },
                "i": {
                    "categories": [
                            "biolink:ChemicalEntity"
                        ]
                    },
                    "f": {
                        "categories": [
                            "biolink:ChemicalEntity"
                        ]
                    }
                },
                "edges": {
                    "edge_0": {
                        "subject": "i",
                        "object": "$target",
                        "predicates": [
                            "biolink:ameliorates"
                        ]
                    },
                    "edge_1": {
                        "subject": "$source",
                        "object": "f",
                        "predicates": [
                            "biolink:has_part"
                        ]
                    },
                    "edge_2": {
                        "subject": "i",
                        "object": "f",
                        "predicates": [
                            "biolink:has_part"
                        ]
                    }
                }
            }
        }
    }
    better_be_f = await get_promiscuous_qnodes(test_graph)
    assert better_be_f == ["f"]