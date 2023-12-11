import os
import redis
import json
import gzip

CACHE_HOST = os.environ.get("CACHE_HOST", "localhost")
CACHE_PORT = os.environ.get("CACHE_PORT", "6379")
CREATIVE_CACHE_DB = os.environ.get("CREATIVE_CACHE_DB", "0")
LOOKUP_CACHE_DB = os.environ.get("LOOKUP_CACHE_DB", "1")

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

    def get_query_key(self, input_id, predicate, qualifiers, source_input, caller, workflow):
        keydict = {'predicate': predicate, 'source_input': source_input, 'input_id': input_id, 'caller': caller, 'workflow': workflow}
        keydict.update(qualifiers)
        return json.dumps(keydict, sort_keys=True)

    def get_result(self, input_id, predicate, qualifiers, source_input, caller, workflow):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller, workflow)
        result = self.creative_redis.get(key)
        if result is not None:
            result = json.loads(gzip.decompress(result))
        return result


    def set_result(self, input_id, predicate, qualifiers, source_input, caller, workflow, final_answer):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller, workflow)

        self.creative_redis.set(key, gzip.compress(json.dumps(final_answer).encode()))

    def get_lookup_query_key(self, workflow, query_graph):
        keydict = {'workflow': workflow, 'query_graph': query_graph}
        return json.dumps(keydict, sort_keys=True)

    def get_lookup_result(self, workflow, query_graph):
        key = self.get_lookup_query_key(workflow, query_graph)
        result = self.lookup_redis.get(key)
        if result is not None:
            result = json.loads(gzip.decompress(result))
        return result


    def set_lookup_result(self, workflow, query_graph, final_answer):
        key = self.get_lookup_query_key(workflow, query_graph)

        self.lookup_redis.set(key, gzip.compress(json.dumps(final_answer).encode()))

    
    def clear_creative_cache(self):
        self.creative_redis.flushdb()

    def clear_lookup_cache(self):
        self.lookup_redis.flushdb()
