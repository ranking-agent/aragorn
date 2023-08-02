import os
import redis
import json

CACHE_HOST = os.environ.get("CACHE_HOST", "localhost")
CACHE_PORT = os.environ.get("CACHE_PORT", "6379")
CACHE_DB = os.environ.get("CACHE_DB", "0")
CACHE_PASSWORD = os.environ.get("CACHE_PASSWORD", "")

class ResultsCache:
    def __init__(self, redis_host=CACHE_HOST, redis_port=CACHE_PORT, redis_db=CACHE_DB, redis_password=CACHE_PASSWORD):
        """Connect to cache."""
        self.redis = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True)

    def get_query_key(self, input_id, predicate, qualifiers, source_input, caller):
        keydict = {'predicate': predicate, 'source_input': source_input, 'input_id': input_id, 'caller': caller}
        keydict.update(qualifiers)
        return json.dumps(keydict, sort_keys=True)

    def get_result(self, input_id, predicate, qualifiers, source_input, caller):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller)
        result = self.redis.get(key)
        if result is not None:
            result = json.loads(result)
        return result


    def set_result(self, input_id, predicate, qualifiers, source_input, caller, final_answer):
        key = self.get_query_key(input_id, predicate, qualifiers, source_input, caller)
        self.redis.set(key, json.dumps(final_answer))