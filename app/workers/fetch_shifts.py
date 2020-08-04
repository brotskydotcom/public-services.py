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
import csv
from typing import Dict, Tuple

from ..utils import (
    MapContext as MC,
    ATRecord,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)


def fetch_mobilize_shifts(
    csv_name: str,
) -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    """
    Get application records from a Mobilize CSV.
    Returns them in a map from email-timeslot_id (key) to record.
    """
    print(f"Creating records for shifts and attendees...")
    shifts: Dict[str, ATRecord] = {}
    attendees: Dict[str, ATRecord] = {}
    with open(csv_name) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
        headings = next(csv_reader)
        for row in csv_reader:
            row_data: Dict[str, str] = {}
            for i, entry in enumerate(row):
                heading = headings[i]
                row_data[heading] = entry
            MC.set("shift")
            shift_record = ATRecord.from_mobilize(row_data)
            shift_record.core_fields["email"] = [row_data["email"]]
            shifts[shift_record.key] = shift_record

            MC.set("person")
            attendee_record = ATRecord.from_mobilize_person(row_data)
            attendees[attendee_record.key] = attendee_record

    print(f"Created {len(shifts)} records for shifts.")
    print(f"Created {len(attendees)} records for attendees.")
    return shifts, attendees


def fetch_airtable_shift_records() -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    print(f"Fetching Airtable records...")
    MC.set("shift")
    airtable_shifts = fetch_all_records()
    MC.set("person")
    airtable_people = fetch_all_records()

    print(f"Fetched {len(airtable_shifts)} existing Airtable records for shifts.")
    print(f"Fetched {len(airtable_people)} existing Airtable records for attendees.")
    return airtable_shifts, airtable_people


def transfer_shifts(csv_name: str):
    print(f"Transferring all shifts")
    airtable_shifts, airtable_people = fetch_airtable_shift_records()
    shifts, attendees = fetch_mobilize_shifts(csv_name)

    MC.set("person")
    people_comparison_map = compare_record_maps(airtable_people, attendees)
    # We are planning on first uploading any new Mobilize people to Action Network
    # to be imported to Airtable through the Action Network integration, so we are
    # setting the map of people records where Mobilize is newer than Airtable to be empty
    # so that we never override any people records that Action Network has already uploaded
    # to Airtable
    people_comparison_map["an_newer"] = {}
    make_record_updates(people_comparison_map)

    MC.set("shift")
    shift_comparison_map = compare_record_maps(airtable_shifts, shifts)
    make_record_updates(shift_comparison_map)
    print(f"Finished processing shifts.")
