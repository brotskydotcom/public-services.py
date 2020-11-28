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
from typing import Dict, Any, Optional, Tuple

from airtable import Airtable

from .constants import MapContext as MC
from .formats import ATRecord
from ..base import prinl


async def insert_or_update_record(an_record: ATRecord):
    """
    Given an AN record for an already-set context, see if there's an existing
    AT record for the same key.  If not, insert it.
    """
    record_type = MC.get()
    at_key, at_base, at_table, at_typecast = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    record_dict = at.match(MC.at_key_field(), an_record.key)
    if not record_dict:
        prinl(f"Uploading new {record_type} record.")
        at.insert(an_record.all_fields(), typecast=at_typecast)
        return
    prinl(f"Retrieved matching Airtable {record_type} record.")
    if at_record := ATRecord.from_record(record_dict):
        an_record.at_match = at_record
    else:
        raise ValueError(f"Matching record is not valid: {record_dict}")
    updates = an_record.find_at_field_updates()
    if updates:
        prinl(f"Updating {len(updates)} fields in record.")
        at.update(at_record.record_id, updates, typecast=at_typecast)
    else:
        prinl(f"No fields need update in record.")


def fetch_all_records(keys_only: bool = False) -> Dict[str, ATRecord]:
    """
    Get all records from Airtable, returning a map from key to record.
    If multiple records are found for a given key, the preferred record
    (as determined by the formats module) is kept,
    and all the non-preferred records are deleted from Airtable.
    """
    record_type = MC.get()
    prinl(f"Looking for {record_type} records in Airtable...")
    at_key, at_base, at_table, _ = MC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    if keys_only:
        field_map = MC.core_field_map()
        fields = [field_map[MC.an_key_field()], field_map["Timestamp (EST)"]]
        all_records = at.get_all(fields=fields)
    else:
        all_records = at.get_all()
    prinl(f"Found {len(all_records)} records; looking for duplicates...")
    results: Dict[str, ATRecord] = {}
    to_delete = []
    for record_dict in all_records:
        record = ATRecord.from_record(record_dict)
        if record:
            if existing := results.get(record.key):
                if record.is_preferred_to(existing):
                    results[record.key] = record
                    to_delete.append(existing.record_id)
                else:
                    to_delete.append(record.record_id)
            else:
                results[record.key] = record
    if len(to_delete) > 0:
        prinl(f"Found {len(to_delete)} duplicates; removing them...")
        at.batch_delete(to_delete)
    prinl(f"Found {len(results)} distinct {record_type} record(s).")
    return results


def compare_record_maps(
    at_map: Dict[str, ATRecord],
    an_map: Dict[str, ATRecord],
    assume_newer: bool = False,
) -> Dict[str, Dict]:
    prinl(
        f"Comparing {len(at_map)} Airtable record(s) "
        f"with {len(an_map)} source record(s)..."
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
        f"{len(matching)} matching source records."
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
    comparison_map: Dict[str, Dict[str, ATRecord]],
    assume_newer: bool = False,
    delete_unmatched_except: Optional[Tuple] = None,
):
    """Update Airtable from newer source records"""
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
                if (i + 1) % 25 == 0:
                    prinl(f"Processed {i+1}/{len(update_map)}...")
    if not did_update:
        prinl(f"No updates required for {record_type} records.")
    at_only = comparison_map["at_only"]
    if at_only and delete_unmatched_except:
        field_name = delete_unmatched_except[0]
        field_val = delete_unmatched_except[1]
        record_ids = [
            record.record_id
            for record in at_only.values()
            if record.custom_fields.get(field_name) != field_val
        ]
        if record_ids:
            prinl(f"Deleting {len(record_ids)} unmatched Airtable record(s)...")
            at.batch_delete(record_ids)
