#  MIT License
#
#  Copyright (c) 2020 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
import asyncio
from random import uniform
from time import time as now
from typing import ClassVar, Optional, Any

from aioredis import Redis

from .redis_db import redis


class ItemListStore:
    """
    Item lists are stored in a sorted set in Redis,
    with the sort key being the time the item can
    next be processed.

    When an item is actually
    being processed, the item time is reset to a
    special marker time encoding the time its
    processing is expected to complete.  This
    allows discovering items that were left
    over from crashes in prior runs.

    This class is a singleton.
    """

    IN_PROCESS: ClassVar[float] = 1.0 * 60 * 60 * 24 * 7
    """
    Item lists that are going to be processed have their score set to
    a week from now, so they are way further out than delayed items.
    """

    TIMEOUT: ClassVar[float] = 1.0 * 60 * 20
    """
    If an item list has been in processing for 20 minutes, it's believed
    to have been left over from a prior run.
    """

    RETRY_DELAY: ClassVar[float] = 1.0 * 60 * 15
    """
    Retries of failed item lists are delayed 15 minutes to let
    the upstream systems recover from whatever their issue was. 
    """

    CLOCK_DRIFT: ClassVar[float] = 1.0 * 15
    """
    There are multiple participating clients of the Store and
    we allow their clocks to drift by 15 seconds relative to
    ours whenever we check the time on an item.
    """

    set_key: ClassVar[str] = redis.get_key("Item List Store")
    """
    The sorted set of items with their next-ready times.
    """

    new_item_channel_name: ClassVar[str] = redis.get_key("Item Arrival")

    db: Optional[Redis]

    @classmethod
    async def initialize(cls):
        """
        Make sure redis is connected and remember the connection pool.
        """
        await redis.connect_async()
        cls.db = redis.db

    @classmethod
    async def terminate(cls):
        """
        Make sure redis is closed.
        """
        cls.db = None
        await redis.close_async()

    @classmethod
    async def add_new_list(cls, key: str) -> Any:
        result = await cls.db.zadd(cls.set_key, score=now(), member=key)
        cls.db.publish(cls.new_item_channel_name, key)
        return result

    @classmethod
    async def add_retry_list(cls, key: str) -> Any:
        result = await cls.db.zadd(
            cls.set_key, score=now() + cls.RETRY_DELAY, member=key
        )
        return result

    @classmethod
    async def remove_item_list(cls, key: str) -> Any:
        result = await cls.db.zrem(cls.set_key, key)
        return result

    @classmethod
    async def select_new_item(cls) -> Optional[str]:
        """
        Wait until there's a new item, then return it.
        """
        try:
            channels = await cls.db.subscribe(cls.new_item_channel_name)
            key = await channels[0].get(encoding="utf-8")
            await cls.db.unsubscribe(cls.new_item_channel_name)
            return key
        except asyncio.CancelledError:
            return None

    @classmethod
    async def select_for_processing(cls, timeout: float = 0) -> Optional[str]:
        """
        Find the first item list that's ready for processing,
        mark it as in-process, and return it.  The select
        prioritizes any older item lists that were left from a prior run
        over item lists that haven't been processed before.
        If there is an item list that's not yet ready for processing,
        we will wait an optional `timeout` seconds for it to become ready
        unless (default timeout of 0, which means indefinitely).

        Returns:
            The item list key, if we selected one, None otherwise.
        """

        async def mark_for_processing(key: str) -> str:
            """
            Mark an item list key as being in process.  Returns the key.
            """
            new_score = now() + cls.IN_PROCESS
            pipe: Redis = cls.db.multi_exec()
            pipe.zadd(cls.set_key, score=new_score, member=key)
            await pipe.execute()
            return key

        # because we are using optimistic locking, keep trying if we
        # fail due to interference from other workers.
        while True:
            try:
                start = now()
                # optimistically lock the item set
                await cls.db.watch(cls.set_key)
                # first look for abandoned items
                items = await cls.db.zrangebyscore(
                    cls.set_key,
                    min=start + (cls.RETRY_DELAY + cls.CLOCK_DRIFT),
                    max=start + cls.IN_PROCESS - (cls.TIMEOUT + cls.CLOCK_DRIFT),
                    offset=0,
                    count=1,
                    encoding="ascii",
                )
                if items:
                    return await mark_for_processing(items[0])
                # next look for the first item that's ready now
                items = await cls.db.zrangebyscore(
                    cls.set_key,
                    max=start + cls.CLOCK_DRIFT,
                    offset=0,
                    count=1,
                    encoding="ascii",
                )
                if items:
                    return await mark_for_processing(items[0])
                # next look for the first delayed item and maybe wait for it
                items_and_scores = await cls.db.zrange(
                    cls.set_key, start=0, stop=0, withscores=True, encoding="ascii"
                )
                if items_and_scores:
                    item, when = items_and_scores[0]
                    delay = when - start
                    if 0 < timeout < delay:
                        # we can't wait for this item to be ready
                        return None
                    # wait for this item, then try again
                    # we add a random delay to avoid conflict with other processors
                    await asyncio.sleep(delay + uniform(0.1, cls.CLOCK_DRIFT))
                    continue
                # give up
                return None
            except redis.WatchError:
                continue
            finally:
                await cls.db.unwatch()
