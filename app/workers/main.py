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
from typing import List, ClassVar

from .csv_transfer import process_csv_lists
from .webhook_transfer import process_webhook_lists
from ..base import prinl, log_error
from ..db import ItemListStore
from ..utils import MapContext


async def worker(item_type: str):
    """
    The main worker loop.  This is meant to be run as a task,
    either in the web process or a separate worker process.
    It only processes items of the given type, so if you
    want to process both types in one worker you will need
    to run one of these tasks for each type of worker.

    You have to do MapContext and ItemListStore initialization
    and teardown around your call to this function.
    """
    if item_type not in ("webhook", "csv"):
        raise ValueError(f"Worker item type ({item_type}) must be 'webhook' or 'csv'")
    try:
        while True:
            if item_type == "webhook":
                await process_webhook_lists()
            else:
                await process_csv_lists()
            prinl(f"Waiting for new {item_type} items to arrive...")
            key = await ItemListStore.select_from_channel(item_type)
            if key:
                prinl(f"New incoming {item_type} item list: {key}")
            else:
                # this happens on shutdown, so we do a silent exit
                break
            # minimize conflict between multiple workers with random stagger
            await asyncio.sleep(uniform(0.1, 0.9))
    except asyncio.CancelledError:
        prinl(f"Cancelled: {item_type} worker.")
        raise


async def app(item_types: List[str]):
    """
    The main worker app, run as the only task in a process.
    Spawns tasks for each worker type and waits for them.
    """
    MapContext.initialize()
    await ItemListStore.initialize()
    try:
        await EmbeddedWorkers.start(item_types)
        await EmbeddedWorkers.run()
    except asyncio.CancelledError:
        raise
    except:
        log_error(f"Exception in worker")
        await EmbeddedWorkers.stop()
    finally:
        await ItemListStore.finalize()
        MapContext.finalize()


class EmbeddedWorkers:
    """
    A way of using _workers as tasks, rather than as
    a top-level process, so they can be embedded in
    a web server or other async process.
    """

    _item_types: ClassVar[List[str]] = []
    _workers: ClassVar[List[asyncio.Task]] = []

    @classmethod
    async def _cancel_workers(cls):
        """
        Forcibly cancel any running worker tasks.
        """
        for item_type, task in zip(cls._item_types, cls._workers):
            if task.done():
                continue
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except:
                log_error(f"Failure: {item_type} worker")

    @classmethod
    async def _main(cls):
        """
        Run _workers as sub-tasks.  We assume all the
        initialization, teardown, and error handling
        is done by our embedding process or the worker itself.
        """
        try:
            for item_type in cls._item_types:
                prinl(f"Starting {item_type} worker.")
                cls._workers.append(asyncio.create_task(worker(item_type)))
        except asyncio.CancelledError:
            await cls._cancel_workers()
            raise
        except:
            log_error(f"Exception in worker manager")
            await cls._cancel_workers()
            raise

    @classmethod
    async def start(cls, item_types: List[str] = None):
        """
        Starts embedded worker tasks for each of the given item types.
        If not item types are given, we check the OS environment variable
        EMBEDDED_WEBHOOK_TYPES and, if it's non-empty, we treat it as a
        list of item types separated by ':'

        No workers are started if the resulting list of item types is empty.
        """
        if cls._workers:
            raise NotImplementedError("Embedded workers are already started")
        if not item_types:
            if types := os.getenv("EMBEDDED_WORKER_TYPES", ""):
                item_types = types.split(":")
        if not item_types:
            return
        cls._item_types = item_types
        cls._workers = []
        await cls._main()

    @classmethod
    async def run(cls, ignore_exceptions=False):
        """
        Wait for currently running workers to complete.

        If ignore_exceptions is specified, an exception
        in one of the workers will not be raised to the caller.
        """
        await asyncio.gather(*cls._workers, return_exceptions=ignore_exceptions)

    @classmethod
    async def stop(cls):
        """
        Shuts down any running workers.
        """
        if cls._workers:
            await cls._cancel_workers()
        cls._workers = []
