import os
import redis
import json
import gzip
from fastapi import HTTPException, status

CACHE_HOST = os.environ.get("CACHE_HOST", "localhost")
CACHE_PORT = os.environ.get("CACHE_PORT", "6379")
CREATIVE_CACHE_DB = os.environ.get("CREATIVE_CACHE_DB", "0")
LOOKUP_CACHE_DB = os.environ.get("LOOKUP_CACHE_DB", "1")
CACHE_PASSWORD = os.environ.get("CACHE_PASSWORD", "")

class ResultsCache:
    def __init__(
        self,
        redis_host=CACHE_HOST,
        redis_port=CACHE_PORT,
        creative_redis_db=CREATIVE_CACHE_DB,
        lookup_redis_db=LOOKUP_CACHE_DB,
        redis_password=CACHE_PASSWORD,
    ):
        """Connect to cache."""
        self.creative_redis = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=creative_redis_db,
            password=redis_password,
        )
        self.lookup_redis = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=lookup_redis_db,
            password=redis_password,
        )

    def get_query_key(self, input_id, predicate, qualifiers, source_input, caller, workflow, mcq, member_ids):
        keydict = {'predicate': predicate, 'source_input': source_input, 'input_id': input_id, 'caller': caller, 'workflow': workflow}
        keydict.update(qualifiers)
        if mcq:
            #because we already have a bunch of keys without mcq, we only want to add these if we are doing the new mcq.
            member_ids.sort()
            keydict['mcq'] = True
            keydict['member_ids'] = member_ids
        return json.dumps(keydict, sort_keys=True)

    def get_result(self, input_id, predicate, qualifiers, source_input, caller, workflow, mcq, member_ids):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller, workflow, mcq, member_ids)
        try:
            result = self.creative_redis.get(key)
            if result is not None:
                result = json.loads(gzip.decompress(result))
        except Exception:
            # failed to get result from cache
            result = None
            pass
        return result


    def set_result(self, input_id, predicate, qualifiers, source_input, caller, workflow, mcq, member_ids, final_answer):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller, workflow, mcq, member_ids)

        try:
            self.creative_redis.set(key, gzip.compress(json.dumps(final_answer).encode()))
        except Exception:
            # failed to save result to cache
            pass

    def get_lookup_query_key(self, workflow, query_graph):
        keydict = {'workflow': workflow, 'query_graph': query_graph}
        return json.dumps(keydict, sort_keys=True)

    def get_lookup_result(self, workflow, query_graph):
        key = self.get_lookup_query_key(workflow, query_graph)
        try:
            result = self.lookup_redis.get(key)
            if result is not None:
                result = json.loads(gzip.decompress(result))
        except Exception:
            # failed to get lookup result
            result = None
            pass
        return result


    def set_lookup_result(self, workflow, query_graph, final_answer):
        key = self.get_lookup_query_key(workflow, query_graph)

        try:
            self.lookup_redis.set(key, gzip.compress(json.dumps(final_answer).encode()))
        except Exception:
            # failed to save lookup result
            pass

    
    def clear_creative_cache(self):
        self.creative_redis.flushdb()

    def clear_lookup_cache(self):
        self.lookup_redis.flushdb()

    def ping_cache(self):
        try:
            self.creative_redis.ping()
        except Exception:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
