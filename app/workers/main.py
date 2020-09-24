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

#  MIT License
#
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
import os
from random import uniform
from typing import Optional

from .webhook_transfer import process_all_item_lists
from ..base import prinl, log_error
from ..db import ItemListStore, RecordCache
from ..utils import MapContext


async def worker():
    """
    The main worker loop.  This is meant to be run as a task,
    either in the server process or a separate worker process.

    You have to do MapContext and ItemListStore initialization
    and teardown around your call to this function.
    """
    try:
        while True:
            await process_all_item_lists()
            prinl(f"Waiting for new items to arrive...")
            key = await ItemListStore.select_from_channel()
            if not key:
                break
            prinl(f"New incoming item list: {key}")
            # minimize conflict between workers with random stagger
            await asyncio.sleep(uniform(0.1, 2.0))
        prinl(f"Worker stopped.")
    except asyncio.CancelledError:
        prinl(f"Worker cancelled.")
    except:
        log_error(f"Worker failure")
        raise


async def app():
    """
    The main worker app, run as the only task in a process.
    """
    MapContext.initialize()
    await MapContext.initialize()
    await ItemListStore.initialize()
    try:
        prinl(f"Worker started...")
        await worker()
    except KeyboardInterrupt:
        prinl(f"Worker shutdown.")
    except:
        prinl(f"Worker failed.")
    finally:
        await ItemListStore.finalize()
        await RecordCache.finalize()
        await MapContext.finalize()


class EmbeddedWorker:
    """
    A way of using a worker as a task, rather than as
    a top-level process, so it can be embedded in
    a web server process.
    """

    worker_task: Optional[asyncio.Task] = None

    @staticmethod
    async def app():
        """
        The worker run as an embedded task.  We assume all the
        initialization and teardown is done by our embedding process
        and happens before/after ours does.
        """
        try:
            prinl(f"Embedded worker started...")
            await worker()
        except:
            prinl(f"Embedded worker failed.")

    @classmethod
    def start(cls):
        if os.getenv("EMBEDDED_WORKER") is not None:
            cls.worker_task = asyncio.create_task(cls.app())

    @classmethod
    def stop(cls):
        if cls.worker_task:
            cls.worker_task.cancel()
        cls.worker_task = None
