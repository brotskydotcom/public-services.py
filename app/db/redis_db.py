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
import os

from ..base import env, Environment


class RedisDatabase:
    """
    A wrapper object for redis connection pools.  Can dispense
    connections based either on redis-py (sync) or aioredis (async).

    Attributes:
        db: the instance variable you use to access Redis
        Error: the base error class for your type of connection

    Args:
        url: the URL that the database connects to.
            It defaults to the value of the environment variable REDIS_URL,
            and if that is not specified uses localhost database 0.
    """

    def __init__(self, url: str = os.getenv("REDIS_URL")):
        self.url = url or "redis://localhost:6379/0"
        self.db = None
        self.keys = {}
        self.Error = Exception

    async def connect_async(self):
        if self.db is not None:
            return

        from aioredis import RedisError, create_redis_pool

        self.Error = RedisError
        max_connections = 5 if env() is Environment.PROD else 2
        self.db = await create_redis_pool(self.url, maxsize=max_connections)

    async def close_async(self):
        if self.db is None:
            return

        self.db.close()
        await self.db.wait_closed()
        self.db = None

    def connect_sync(self):
        if self.db is not None:
            return

        from redis import RedisError, from_url

        self.Error = RedisError
        max_connections = 5 if env() is Environment.PROD else 2
        self.db = from_url(self.url, max_connections=max_connections)

    def close_sync(self):
        if self.db is None:
            return

        self.db.close()
        self.db = None

    def get_key(self, name: str) -> str:
        """
        Look for a registered key _name_, creating one if necessary.

        Note: Created keys are specific to the runtime environment.
        """
        if key := self.keys.get(name):
            return key
        key = f"{name} [{env().name}]"
        self.keys[name] = key
        return key


redis = RedisDatabase()
