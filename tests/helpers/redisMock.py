import fakeredis
import gzip
import json

async def redisMock(connection_pool=None):
    # Here's where I got documentation for how to do async fakeredis:
    # https://github.com/cunla/fakeredis-py/issues/66#issuecomment-1316045893
    redis = await fakeredis.FakeStrictRedis
    # set up mock function
    return redis