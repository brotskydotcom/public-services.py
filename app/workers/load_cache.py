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
from ..utils import MapContext as MC, fetch_all_records


async def load_cache(record_types: List[str]):
    prinl(f"Loading cache...")
    if not record_types:
        record_types = ("event", "shift", "person")
    await RecordCache.initialize()
    for record_type in record_types:
        MC.set(record_type)
        cache_map = await RecordCache.get_all_records(record_type)
        record_map = fetch_all_records()
        updated_count, total = 0, len(record_map)
        prinl(f"Loading {total} {record_type} records in cache...")
        for count, (key, record) in enumerate(record_map.items()):
            if (
                not (cache_val := cache_map.get(key))
                or cache_val[0] != record.mod_date
                or cache_val[1] != record.record_id
            ):
                updated_count += 1
                await RecordCache.add_record(
                    record_type, record.key, record.mod_date, record.record_id
                )
                if updated_count % 10 == 0:
                    prinl(f"Updated {updated_count}, {total - (count + 1)} to go...")
        prinl(f"Updated {updated_count} {record_type} records in the cache.")
        missing_count, total = 0, len(cache_map)
        prinl(f"Sweeping {total} cache entries looking for missing records...")
        for count, (key, val) in enumerate(cache_map.items()):
            if record_map.get(key) is None and val is not None:
                missing_count += 1
                await RecordCache.mark_missing(record_type, key)
                if missing_count % 10 == 0:
                    prinl(f"Marked {missing_count}, {total - (count + 1)} to go...")
        prinl(f"Marked {missing_count} {record_type} records as missing.")
    await RecordCache.finalize()
    prinl(f"Finished loading cache.")
