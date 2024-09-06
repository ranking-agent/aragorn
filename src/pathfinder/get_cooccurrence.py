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
    start = time.time()
    curie_pmids = []
    for curie in curies:
        pmids = r.get(curie)
        if pmids is None:
            pmids = []
        else:
            pmids = json.loads(gzip.decompress(pmids))
        # print(len(pmids))
        curie_pmids.append(pmids)
    answer = list(set.intersection(*map(set, curie_pmids)))
    end = time.time()
    # print(f"Got back {len(answer)} pmids in {end - start} seconds.")
    return answer

def get_the_curies(pmid: str):
    r = redis.Redis(
        host="localhost",
        port=6379,
        db=1
    )
    start = time.time()
    curies = r.get(pmid)
    if curies is None:
        curies = []
    else:
        curies = json.loads(gzip.decompress(curies))
    answer = list(curies)
    end = time.time()
    # print(f"Got back {len(answer)} curies in {end - start} seconds.")
    return answer


if __name__ == "__main__":
    print(len(get_the_pmids(["MONDO:0004979"])))
    print(len(get_the_pmids(["CHEBI:45783"])))
