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
import asyncio
import os
import pickle
from datetime import datetime, timezone
from typing import Tuple, Dict

from ..base import prinl, log_error
from ..db import redis, ItemListStore as Store
from ..utils import (
    MapContext as MC,
    ATRecord,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)


async def process_csv_list(key: str):
    """
    Process the items in the list with the given `key`.
    If there are temporary failures with some of the items on the list,
    we make a list of the items that failed, and return it.
    """
    _, item_type, rc = key.split(":")
    csv_data = await redis.db.lpop(key)
    if not csv_data:
        # an empty key
        return
    headings, rows = pickle.loads(csv_data)
    count, total = 0, len(rows)
    if total == 0:
        # headings but no rows
        return
    prinl(f"Processing {total} {item_type} row(s) on list '{key}'...")
    if item_type == "event":
        transfer_events(headings, rows)
    elif item_type == "shift":
        transfer_shifts(headings, rows)
    else:
        prinl(f"CSV type is unknown ('{item_type}'), ignoring it.")


def transfer_events(headings, rows):
    MC.set("person")
    airtable_people = fetch_all_records(keys_only=True)
    MC.set("event")
    airtable_events = fetch_all_records()
    prinl(f"Processing {len(rows)} rows from event export...")
    events: Dict[str, ATRecord] = {}
    delete_unmatched_records = True
    for i, row in enumerate(rows):
        row_data = dict(zip(headings, row))
        event_record = ATRecord.from_mobilize_event(row_data)
        if not event_record:
            prinl(f"Invalid data: skipping event on row {i+2}.")
            continue
        email = event_record.core_fields["email"]
        if email and airtable_people.get(email) is not None:
            event_record.core_fields["email"] = [email]
        else:
            event_record.core_fields["email"] = ""
        if event_record.custom_fields.get("Event Visibility*") == "PERMANENT":
            delete_unmatched_records = False
        events[event_record.key] = event_record
    event_map = compare_record_maps(airtable_events, events)
    if delete_unmatched_records:
        make_record_updates(
            event_map, delete_unmatched_except=("Event Visibility*", "PERMANENT")
        )
    else:
        make_record_updates(event_map)


def transfer_shifts(headings, rows):
    min_shift_datetime = os.getenv("MINIMUM_SHIFT_DATETIME")
    force_shift_updates = os.getenv("FORCE_SHIFT_UPDATES")
    MC.set("person")
    airtable_people = fetch_all_records(keys_only=True)
    MC.set("event")
    airtable_events = fetch_all_records(keys_only=True)
    MC.set("shift")
    airtable_shifts = fetch_all_records()
    prinl(f"Processing {len(rows)} rows from shift export...")
    shifts: Dict[str, ATRecord] = {}
    attendees: Dict[str, ATRecord] = {}
    for i, row in enumerate(rows):
        row_data = dict(zip(headings, row))
        MC.set("shift")
        shift_record = ATRecord.from_mobilize_shift(row_data, min_shift_datetime)
        if not shift_record:
            continue
        event_id = shift_record.core_fields["event"]
        if airtable_events.get(event_id) is None:
            shift_record.core_fields["event"] = ""
        else:
            shift_record.core_fields["event"] = [event_id]
            # to restore broken email links on Airtable side
            if force_shift_updates:
                shift_record.mod_date = datetime.now(timezone.utc)
        shifts[shift_record.key] = shift_record
        MC.set("person")
        attendee_record = ATRecord.from_mobilize_person(row_data)
        if not airtable_people.get(attendee_record.key):
            # only add records for people not in Airtable,
            # since Action Network will eventually update them
            # from the volunteer uploads
            attendees[attendee_record.key] = attendee_record
    MC.set("person")
    attendee_map = compare_record_maps(airtable_people, attendees)
    make_record_updates(attendee_map)
    MC.set("shift")
    shift_map = compare_record_maps(airtable_shifts, shifts)
    make_record_updates(shift_map)


async def process_csv_lists() -> Tuple[int, int]:
    prinl(f"Processing ready CSV item lists...")
    count, retry_count = 0, 0
    try:
        while list_key := await Store.select_for_processing("csv"):
            count += 1
            await process_csv_list(list_key)
            await Store.remove_processed_list("csv", list_key)
    except redis.Error:
        log_error(f"Database failure")
    except asyncio.CancelledError:
        pass
    except:
        log_error(f"Unexpected failure")
    finally:
        prinl(f"Processed {count} CSV list(s).")
    return count, retry_count
