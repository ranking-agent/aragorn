"""Get more than pairwise literature cooccurence for a given list of curies."""
import json
import gzip
import os
import redis
import time
from typing import List

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
PMIDS_DB = os.environ.get("PMIDS_DB", 1)
CURIES_DB = os.environ.get("CURIES_DB", 2)
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

def get_the_pmids(curies: List[str]):
    """Returns a list of pmids for papers that mention all curies in list"""
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=PMIDS_DB,
        password=REDIS_PASSWORD
    )
    curie_pmids = []
    for curie in curies:
        pmids = r.get(curie)
        if pmids is None:
            pmids = []
        else:
            pmids = json.loads(gzip.decompress(pmids))
        curie_pmids.append(pmids)
    answer = list(set.intersection(*map(set, curie_pmids)))
    return answer

def get_the_curies(pmid: str):
    """Returns a list of all curies in the paper corresponding to the pmid."""
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=CURIES_DB,
        password=REDIS_PASSWORD
    )
    curies = r.get(pmid)
    if curies is None:
        curies = []
    else:
        curies = json.loads(gzip.decompress(curies))
    answer = list(curies)
    return answer
