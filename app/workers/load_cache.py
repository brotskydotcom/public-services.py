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
from typing import List

from ..base import prinl
from ..db import RecordCache
from ..utils import MapContext as MC, RecordBatch, fetch_all_records


async def load_cache(record_types: List[str]):
    prinl(f"Loading cache...")
    if not record_types:
        record_types = ("person", "event", "shift")
    await RecordCache.initialize()
    for record_type in record_types:
        MC.set(record_type)
        record_map = fetch_all_records()
        total = len(record_map)
        prinl(f"Adding {total} {record_type} records to cache...")
        batch = RecordBatch(record_type) if record_type in ["event", "shift"] else None
        for count, record in enumerate(record_map.values()):
            if batch:
                await batch.add_record(record.key)
            await RecordCache.add_record(
                record_type, record.key, record.mod_date, record.record_id
            )
            if (count + 1) % 10 == 0:
                prinl(f"Added {count+1}/{total} records...")
        prinl(f"Finished adding {total} {record_type} records to cache.")
        if batch:
            await batch.mark_unused_records_as_missing()
    await RecordCache.finalize()
    prinl(f"Finished loading cache.")
