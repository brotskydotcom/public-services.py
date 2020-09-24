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
import pickle
from datetime import datetime
from typing import Optional, Tuple

from aioredis import Redis

from .redis_db import redis


class RecordCache:
    """
    We keep a Redis-based cache of records that we often need
    to look up.  We map from the key by which we look up the
    records to the last mod date of the record, and clients use
    the mod date to decide if the cache can be used.

    Note that this cache is to establish the existence of a
    record in Airtable, not to read its content.  If you need
    its content, then you need to fetch the record.

    This class is a singleton.
    """

    KEY_FORMAT = "RecordCache|||{record_type}|||{key}"
    """
    Each cache key has this format.
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
    async def add_record(cls, record_type: str, key: str, mod_date: datetime):
        """Add a record to the cache."""
        key_name = cls.KEY_FORMAT.format(record_type=record_type, key=key)
        cache_key = redis.get_key(key_name)
        value = pickle.dumps(mod_date)
        await cls.db.set(cache_key, value)

    @classmethod
    async def mark_missing(cls, record_type: str, key: str):
        """Mark a record as affirmatively not in the cache"""
        key_name = cls.KEY_FORMAT.format(record_type=record_type, key=key)
        cache_key = redis.get_key(key_name)
        value = pickle.dumps(None)
        await cls.db.set(cache_key, value)

    @classmethod
    async def get_record(
        cls, record_type: str, key: str
    ) -> Tuple[bool, Optional[datetime]]:
        """
        Look up a record in the cache.  The first value returned
        indicates whether there was a value in the cache.
        If that's True, then the second value is
        either None, which means that the
        record doesn't exist in the Airtable side,
        or it's the mod_date of the existing Airtable record.
        """
        if not key:
            return False, None
        key_name = cls.KEY_FORMAT.format(record_type=record_type, key=key)
        cache_key = redis.get_key(key_name)
        value = await cls.db.get(cache_key)
        if not value:
            return False, None
        value = pickle.loads(value)
        return True, value
