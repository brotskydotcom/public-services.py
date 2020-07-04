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
from typing import Optional, ClassVar

import aioredis

from ..utils import env, Environment


class RedisDatabase:
    """
    A wrapper object for async redis connection pools.

    Attributes:
        db: the instance variable you use to access Redis

    Args:
        url: the (optional) URL that the database connects to.  If not
            specified, we look for the environment variable REDIS_URL
    """

    Error: ClassVar = aioredis.RedisError
    """A synonym for the base exception used by aioredis."""

    def __init__(self, url: Optional[str] = None):
        self.url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.db = None
        self.keys = {}

    async def connect(self):
        max_connections = 5 if env() is Environment.PROD else 2
        self.db = await aioredis.create_redis_pool(self.url, maxsize=max_connections)

    async def close(self):
        self.db.close()
        await self.db.wait_closed()

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
