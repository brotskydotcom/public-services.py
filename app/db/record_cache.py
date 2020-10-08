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
from typing import Optional, Tuple, Dict

from aioredis import Redis

from .redis_db import redis
from ..base import prinl


class RecordCache:
    """
    We keep a Redis-based cache of records in Airtable.

    This cache is to establish the existence (or not) of a
    record in Airtable, not to read its content.  If you need
    its content, then you need to fetch the record.

    For each record type,
    we keep the set of keys that we have looked up
    for that type of record.

    For each record in the cache,
    we keep the last mod date and record ID of the record
    (or None if the record is cached as missing).

    This class is a singleton.
    """

    TYPE_FORMAT = "RecordCache|{record_type}|"
    """
    Each record type has this format for the key of its set of record keys.
    """

    KEY_FORMAT = "RecordCache|{record_type}|{key}|"
    """
    Each record has this format for its cache key.
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
    async def _get_key(cls, record_type: str, key: str) -> str:
        key_name = cls.KEY_FORMAT.format(record_type=record_type, key=key)
        record_key = redis.get_key(key_name)
        return record_key

    @classmethod
    async def _add_key(cls, record_type: str, key: str) -> str:
        type_name = cls.TYPE_FORMAT.format(record_type=record_type)
        type_key = redis.get_key(type_name)
        await cls.db.sadd(type_key, key)
        return await cls._get_key(record_type, key)

    @classmethod
    async def add_record(
        cls, record_type: str, key: str, mod_date: datetime, record_id: str
    ):
        """Add a record to the cache."""
        record_key = await cls._add_key(record_type, key)
        value = pickle.dumps((mod_date, record_id))
        await cls.db.set(record_key, value)

    @classmethod
    async def mark_missing(cls, record_type: str, key: str):
        """Mark a record as affirmatively not in Airtable"""
        record_key = await cls._add_key(record_type, key)
        value = pickle.dumps(None)
        await cls.db.set(record_key, value)

    @classmethod
    async def get_record(
        cls, record_type: str, key: str
    ) -> Tuple[bool, Optional[Tuple[datetime, str]]]:
        """
        Look up a record in the cache.

        The first value returned
        indicates whether there was a value in the cache.
        If that's True, then the second value is
        either None, which means that the
        record doesn't exist in the Airtable side,
        or it's a tuple of the mod_date and key of the Airtable record.
        """
        if not key:
            raise ValueError(f"Key field ({key}) must not be empty.")
        record_key = await cls._get_key(record_type, key)
        value = await cls.db.get(record_key)
        if not value:
            return False, None
        value = pickle.loads(value)
        return True, value

    @classmethod
    async def get_all_records(
        cls, record_type: str
    ) -> Dict[str, Optional[Tuple[datetime, str]]]:
        type_name = cls.TYPE_FORMAT.format(record_type=record_type)
        type_key = redis.get_key(type_name)
        result: Dict[str, Optional[Tuple[datetime, str]]] = {}
        prinl(f"Fetching all {record_type} record keys from cache...")
        all_keys = await cls.db.smembers(type_key, encoding="utf-8")
        total, value_count = len(all_keys), 0
        batch_size = 500 if total < 1000 else min(total // 5, 1000)
        prinl(f"Found {len(all_keys)} {record_type} keys; fetching values...")
        for i in range(0, total, batch_size):
            key_batch = all_keys[i : i + batch_size]
            record_key_batch = [
                await cls._get_key(record_type, key) for key in key_batch
            ]
            val_batch = await cls.db.mget(*record_key_batch)
            for key, value in zip(key_batch, val_batch):
                if val := pickle.loads(value):
                    value_count += 1
                result[key] = val
            # prinl(f"Fetched {i + len(key_batch)}/{total}...")
        prinl(
            f"Found {value_count} existing {record_type} records in cache; "
            f"{len(all_keys) - value_count} {record_type} keys are marked missing."
        )
        return result
