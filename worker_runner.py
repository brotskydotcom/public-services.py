# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import asyncio

from app.workers.main import app

if __name__ == '__main__':
    asyncio.run(app())
