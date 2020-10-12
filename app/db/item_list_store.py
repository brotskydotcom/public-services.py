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

from aioredis import Redis, RedisError

from .redis_db import redis
from ..base import log_error


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

    TIMEOUT: ClassVar[float] = 1.0 * 60 * 60 * 6
    """
    If an item list has been in processing for six hours, it's believed
    to have been left over from a prior run.
    """

    RETRY_DELAY: ClassVar[float] = 1.0 * 60 * 30
    """
    Retries of failed item lists are delayed 30 minutes to let
    the upstream systems recover from whatever their issue was. 
    """

    CLOCK_DRIFT: ClassVar[float] = 1.0 * 15
    """
    There are multiple participating clients of the Store and
    we allow their clocks to drift by 15 seconds relative to
    ours whenever we check the time on an item.
    """

    set_key_template: ClassVar[str] = "{list_type} Store"
    """
    Key template for a sorted set of typed item lists with their next-ready times.
    """

    circle_key_template: ClassVar[str] = "{list_type} Deferrals"
    """
    Key template for a circular list of deferred typed item lists.
    """

    channel_name_template: ClassVar[str] = "{list_type} Ready"
    """
    Redis pub/sub channel where item lists of the given type
    that are ready to process are published
    so _workers can pick them up.
    """

    db: Optional[Redis]

    @classmethod
    async def initialize(cls):
        """
        Make sure redis is connected and remember the connection pool.
        """
        await redis.connect_async()
        cls.db = redis.db

    @classmethod
    async def finalize(cls):
        """
        Make sure redis is closed.
        """
        await redis.close_async()
        cls.db = None

    @classmethod
    def _set_key(cls, list_type: str):
        return redis.get_key(cls.set_key_template.format(list_type=list_type))

    @classmethod
    def _circle_key(cls, list_type: str):
        return redis.get_key(cls.circle_key_template.format(list_type=list_type))

    @classmethod
    def _channel_name(cls, list_type: str):
        return redis.get_key(cls.channel_name_template.format(list_type=list_type))

    @classmethod
    async def add_new_list(cls, list_type: str, key: str) -> Any:
        """
        Add a newly posted item list to the set with no delay.
        Notify the item list channel that it's ready.
        """
        set_key = cls._set_key(list_type)
        channel_name = cls._channel_name(list_type)
        result = await cls.db.zadd(set_key, score=now(), member=key)
        cls.db.publish(channel_name, key)
        return result

    @classmethod
    async def add_retry_list(cls, list_type: str, key: str) -> Any:
        """
        Add a retry list to the set with an appropriate delay.
        Also arrange to notify the item list channel when it's ready.
        """
        set_key = cls._set_key(list_type)
        channel_name = cls._channel_name(list_type)

        async def notify_later():
            await asyncio.sleep(cls.RETRY_DELAY)
            cls.db.publish(channel_name, key)

        result = await cls.db.zadd(set_key, score=now() + cls.RETRY_DELAY, member=key)
        asyncio.create_task(notify_later())
        return result

    @classmethod
    async def remove_processed_list(cls, list_type: str, key: str) -> Any:
        """
        Remove a processed item list from the set.
        """
        set_key = cls._set_key(list_type)
        result = await cls.db.zrem(set_key, key)
        # fix #47: since we're done with the list, delete it
        await cls.db.delete(key)
        return result

    @classmethod
    async def add_deferred_list(cls, list_type: str, key: str) -> Any:
        """
        Add a deferred item list to the list of them.
        This is a circular list that adds on the left.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls.db.lpush(circle_key, key)
        return result

    @classmethod
    async def remove_deferred_list(cls, list_type: str, key: str) -> Any:
        """
        Remove an item list from the deferred list.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls.db.lrem(circle_key, 1, key)
        return result

    @classmethod
    async def get_deferred_count(cls, list_type: str) -> int:
        """
        Return the count of deferred item lists.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls.db.llen(circle_key)
        return result

    @classmethod
    async def select_for_undeferral(cls, list_type: str) -> Optional[str]:
        """
        Find the oldest deferred list and return it.
        The returned list is made the most recently deferred.
        Returns None if there are no more deferred lists.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls.db.rpoplpush(circle_key, circle_key)
        return result

    @classmethod
    async def select_from_channel(cls, list_type: str) -> Optional[str]:
        """
        Wait until there's an item sent to the channel, then return it.

        This takes advantage of the fact that the underlying connection
        pool has free connections and will automatically use one of them
        for the subscription.  That way we can be a subscriber while
        other tasks are also being publishers.
        """
        channel_name = cls._channel_name(list_type)
        try:
            channels = await cls.db.subscribe(channel_name)
            key = await channels[0].get(encoding="utf-8")
            await cls.db.unsubscribe(channel_name)
            return key
        except RedisError:
            # typically this error means that the process is shutting down
            # by closing connections.  There's no way to recover from it,
            # so we exit silently rather than report the error.
            return None

    @classmethod
    async def select_for_processing(cls, list_type: str) -> Optional[str]:
        """
        Find the first item list of the given type that's ready for processing,
        mark it as in-process, and return it.  The select
        prioritizes any older item lists that were left from a prior run
        over item lists that haven't been processed before.

        Returns:
            The item list key, if one is ready for processing, None otherwise.
        """
        set_key = cls._set_key(list_type)

        async def mark_for_processing(key: str) -> bool:
            """
            Mark an item list key as being in process.
            Returns whether setting the mark was successful.
            """
            new_score = now() + cls.IN_PROCESS
            pipe = cls.db.multi_exec()
            pipe.zadd(set_key, score=new_score, member=key)
            # return the exceptions rather than raising them because of
            # this issue: https://github.com/aio-libs/aioredis/issues/558
            values = await pipe.execute(return_exceptions=True)
            return values[0] == 0

        # because we are using optimistic locking, keep trying if we
        # fail due to interference from other _workers.
        while True:
            try:
                start = now()
                # optimistically lock the item set
                await cls.db.watch(set_key)
                # first look for abandoned item lists from a prior run
                item_lists = await cls.db.zrangebyscore(
                    set_key,
                    min=start + (cls.RETRY_DELAY + cls.CLOCK_DRIFT),
                    max=start + cls.IN_PROCESS - (cls.TIMEOUT + cls.CLOCK_DRIFT),
                    offset=0,
                    count=1,
                    encoding="ascii",
                )
                if not item_lists:
                    # next look for the first item list that's ready now
                    item_lists = await cls.db.zrangebyscore(
                        set_key,
                        max=start + cls.CLOCK_DRIFT,
                        offset=0,
                        count=1,
                        encoding="ascii",
                    )
                if not item_lists:
                    # no item lists ready for processing, give up
                    return None
                # found one to process
                item_list = item_lists[0]
                if await mark_for_processing(item_list):
                    return item_list
                # if marking fails, it's due to a conflict failure,
                # so loop and try again after taking a beat
                await asyncio.sleep(uniform(0.1, 0.8))
            finally:
                # we may have been interrupted and already
                # closed down the connection, so make sure
                # it still exists before we unwatch
                if cls.db:
                    await cls.db.unwatch()
