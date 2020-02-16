import rq
import redis
import itertools
import pandas as pd
from typing import List
import glob

from vkparsing.vk_auth_helper import fetch_user_friends_for_chunk


def by_chunk(iterable, chunk_size=1000):
    """
    Basically, [.........] -> [[...], [...], [...]]

    for chunk in by_chunk(big_iterable):
        for entry in chunk:
            process(chunk)
    """
    iterable = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterable, chunk_size))
        if not len(chunk):
            break
        yield chunk


def get_user_ids(glob_expression: str) -> List[int]:
    df = pd.concat(
        [pd.read_csv(fname, index_col=0) for fname in glob.glob(glob_expression)]
    )
    return df.id.values.tolist()


def enqueue(redis_connection: redis.Redis, glob_expression: str):
    user_ids = get_user_ids(glob_expression)
    q = rq.Queue(connection=redis_connection)
    for i, chunk in enumerate(by_chunk(user_ids)):
        q.enqueue(
            fetch_user_friends_for_chunk,
            kwargs={
                'user_ids': 'chunk',
                'part': f'{i:05d}',
            }
        )


def main(host: str, port: str):
    redis_connection = redis.Redis(host=host, port=port)
    enqueue(redis_connection, glob_expression="../members_*.csv")
