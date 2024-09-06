"""Get more than pairwise literature cooccurence for a given list of curies."""
import json
import gzip
import redis
import time
from typing import List


def get_the_pmids(curies: List[str]):
    r = redis.Redis(
        host="localhost",
        port=6379,
        db=0
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
    r = redis.Redis(
        host="localhost",
        port=6379,
        db=1
    )
    curies = r.get(pmid)
    if curies is None:
        curies = []
    else:
        curies = json.loads(gzip.decompress(curies))
    answer = list(curies)
    return answer
