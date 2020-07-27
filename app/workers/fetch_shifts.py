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
from typing import Dict, List, Tuple

import csv

from ..utils import (
    env,
    Environment,
    MapContext as MC,
    ATRecord,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)

def fetch_mobilize_shifts(csv_name: str) -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    """
    Get application records from a Mobilize CSV.
    Returns them in a map from email-timeslot_id (key) to record.
    """
    shifts: Dict[str, ATRecord] = {}
    attendees: Dict[str, ATRecord] = {} 
    with open(csv_name) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        headings = next(csv_reader)
        for row in csv_reader:
            row_data: Dict[str, str] = {}
            for i, entry in enumerate(row):
                heading = headings[i]
                row_data[heading] = entry
            MC.set("shift")
            shift_record = ATRecord.from_mobilize(row_data)

            MC.set("person")
            attendee_record = ATRecord.from_mobilize_person(row_data)

            shifts[shift_record.key] = shift_record
            attendees[attendee_record.key] = attendee_record
                        
    print(f"Created {len(shifts)} records for shifts.")
    return shifts, attendees

def fetch_airtable_shift_records() -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    MC.set("shift")
    airtable_shifts = fetch_all_records()
    MC.set("person")
    airtable_people = fetch_all_records()
    return airtable_shifts, airtable_people

def transfer_shifts(csv_name: str):
    print(f"Transferring all shifts")
    airtable_shifts, airtable_people = fetch_airtable_shift_records()  
    shifts, attendees =  fetch_mobilize_shifts(csv_name)

    MC.set("person")
    people_comparison_map = compare_record_maps(airtable_people, attendees)
    make_record_updates(people_comparison_map)

    MC.set("shift")
    shift_comparison_map = compare_record_maps(airtable_shifts, shifts)
    make_record_updates(shift_comparison_map)
    print(f"Finished processing shifts.")
