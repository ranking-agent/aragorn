"""Common Aragorn Utilities."""
import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
import asyncio
import datetime
import os


def create_log_entry(msg: str, err_level, timestamp = datetime.datetime.now().isoformat(), code=None) -> dict:
    # load the data
    ret_val = {"timestamp": timestamp, "level": err_level, "message": msg, "code": code}

    # return to the caller
    return ret_val


def get_channel_pool():
    # get the queue connection params
    q_username = os.environ.get("QUEUE_USER", "guest")
    q_password = os.environ.get("QUEUE_PW", "guest")
    q_host = os.environ.get("QUEUE_HOST", "127.0.0.1")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(f"amqp://{q_username}:{q_password}@{q_host}/")


    connection_pool: Pool = Pool(get_connection, max_size=4, loop=loop)


    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()


    channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)

    return channel_pool
