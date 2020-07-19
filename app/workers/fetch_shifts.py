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

def fetch_mobilize_shifts(csv_name: str) -> Dict[str, ATRecord]:
    """
    Get application records from a Mobilize CSV.
    Returns them in a map from email-timeslot_id (key) to record.
    """
    MC.set("shift")
    shifts: Dict[str, ATRecord] = {} 
    with open(csv_name) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        headings = next(csv_reader)
        for row in csv_reader:
            row_data: Dict[str, str] = {}
            for i, entry in enumerate(row):
                heading = headings[i]
                row_data[heading] = entry
            record = ATRecord.from_mobilize(row_data)
            shifts[record.key] = record
                        
    print(f"Created {len(shifts)} records for shifts.")
    if env() is Environment.DEV:
        ATRecord.dump_stats(len(shifts))
    return shifts

def transfer_shifts(csv_name: str):
    print(f"Transferring all shifts")
    MC.set("shift")
    airtable_map = fetch_all_records()
    mobilize_map=  fetch_mobilize_shifts(csv_name)
    comparison_map = compare_record_maps(airtable_map, mobilize_map)
    make_record_updates(comparison_map)
    print(f"Finished processing shifts.")
