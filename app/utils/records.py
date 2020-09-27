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
from typing import Dict, Any

from airtable import Airtable

from .constants import MapContext as MC
from .formats import ATRecord
from ..base import prinl
from ..db import RecordCache


async def lookup_record(key: str) -> bool:
    """
    Lookup a record with the given key, first checking the cache
    and then, if it misses, checking in Airtable.  Returns whether
    there is such a record, but not any info about it.
    """
    if key == "":
        return False
    record_type = MC.get()
    is_authoritative, mod_date = await RecordCache.get_record(record_type, key)
    if is_authoritative:
        return mod_date is not None
    # fell through the cache, look for a matching record
    at_key, at_base, at_table, at_typecast = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    record_dict = at.match(MC.at_key_field(), key)
    if not record_dict:
        prinl(f"Caching non-existent Airtable {record_type} record.")
        await RecordCache.mark_missing(record_type, key)
        return False
    prinl(f"Caching discovered Airtable {record_type} record.")
    if at_record := ATRecord.from_record(record_dict):
        await RecordCache.add_record(record_type, at_record.key, at_record.mod_date)
    else:
        raise ValueError(f"Matching record is not valid: {record_dict}")
    return True


async def insert_or_update_record(an_record: ATRecord, insert_only: bool = False):
    """
    Given an AN record for an already-set context, see if there's an existing
    AT record for the same key.  If not, insert or maybe update it,
    depending on the flag parameters.
    """
    record_type = MC.get()
    is_authoritative, mod_date = await RecordCache.get_record(
        record_type, an_record.key
    )
    if mod_date is not None:
        # found a cached record
        prinl(f"Found cached {record_type} record.")
        if insert_only:
            prinl("Per specified option, not updating Airtable record.")
            return
        if mod_date >= an_record.mod_date:
            prinl(f"Cached record is fresh; no need to update Airtable.")
            return
    at_key, at_base, at_table, at_typecast = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    if is_authoritative and mod_date is None:
        # no Airtable record
        record_dict = None
    else:
        # either it's a cache miss or we need the record to update it
        record_dict = at.match(MC.at_key_field(), an_record.key)
    if not record_dict:
        prinl(f"Uploading new {record_type} record; adding it to cache.")
        at.insert(an_record.all_fields(), typecast=at_typecast)
        await RecordCache.add_record(record_type, an_record.key, an_record.mod_date)
        return
    prinl(f"Retrieved matching Airtable {record_type} record.")
    if at_record := ATRecord.from_record(record_dict):
        an_record.at_match = at_record
        if not is_authoritative:
            prinl(f"Adding retrieved Airtable record to cache.")
            await RecordCache.add_record(record_type, at_record.key, at_record.mod_date)
    else:
        raise ValueError(f"Matching record is not valid: {record_dict}")
    if insert_only:
        prinl(f"Per specified option, not updating Airtable record.")
    else:
        updates = an_record.find_at_field_updates()
        if updates:
            prinl(f"Updating {len(updates)} fields in record.")
            at.update(at_record.record_id, updates, typecast=at_typecast)
            prinl("Updating cached record.")
            await RecordCache.add_record(record_type, an_record.key, at_record.mod_date)
        else:
            prinl(f"No fields need update in record.")


def fetch_all_records() -> Dict[str, ATRecord]:
    """
    Get all records from Airtable, returning a map from key value to record.
    If more than one record has a given key, the last one fetched is kept.
    """
    record_type = MC.get()
    prinl(f"Looking for {record_type} records...")
    at_key, at_base, at_table, _ = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    results: Dict[str, ATRecord] = {}
    for record_dict in at.get_all():
        record = ATRecord.from_record(record_dict)
        if record:
            results.update({record.key: record})
    prinl(f"Found {len(results)} {record_type} record(s).")
    return results


def compare_record_maps(
    at_map: Dict[str, ATRecord],
    an_map: Dict[str, ATRecord],
    assume_newer: bool = False,
) -> Dict[str, Dict]:
    prinl(
        f"Comparing {len(at_map)} Airtable record(s) "
        f"with {len(an_map)} Action Network record(s)..."
    )
    at_only, an_only, an_newer, matching = {}, dict(an_map), {}, {}
    for at_k, at_v in at_map.items():
        an_v = an_map.get(at_k)
        if an_v:
            del an_only[at_k]
            an_v.at_match = at_v  # remember airbase match
            if assume_newer or an_v.mod_date > at_v.mod_date:
                an_newer[at_k] = an_v
            else:
                matching[at_k] = an_v
        else:
            at_only[at_k] = at_v
    prinl(
        f"Found {len(an_only)} new, "
        f"{len(an_newer)} updated, and "
        f"{len(matching)} matching Action Network records."
    )
    if len(at_only) > 0:
        prinl(f"Found {len(at_only)} Airtable record(s) without a match.")
    result = {
        "at_only": at_only,
        "an_only": an_only,
        "an_newer": an_newer,
        "matching": matching,
    }
    return result


def make_record_updates(
    comparison_map: Dict[str, Dict[str, ATRecord]], assume_newer: bool = False
):
    """Update Airtable from newer Action Network records"""
    record_type = MC.get()
    at_key, at_base, at_table, at_typecast = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    an_only = comparison_map["an_only"]
    did_update = False
    if an_only:
        did_update = True
        prinl(f"Doing updates for {record_type} records...")
        prinl(f"Uploading {len(an_only)} new record(s)...")
        records = [r.all_fields() for r in an_only.values()]
        at.batch_insert(records, typecast=at_typecast)
    an_newer: Dict[str, ATRecord] = comparison_map["an_newer"]
    if an_newer:
        update_map: Dict[str, Dict[str, Any]] = {}
        for key, record in an_newer.items():
            updates = record.find_at_field_updates(assume_newer=assume_newer)
            if updates:
                update_map[record.at_match.record_id] = updates
        if update_map:
            if not did_update:
                prinl(f"Doing updates for {record_type} records...")
            did_update = True
            prinl(f"Updating {len(update_map)} existing record(s)...")
            for i, (record_id, updates) in enumerate(update_map.items()):
                at.update(record_id, updates, typecast=at_typecast)
                if (i + 1) % 10 == 0:
                    prinl(f"Processed {i+1}/{len(update_map)}...")
    if not did_update:
        prinl(f"No updates required for {record_type} records.")
