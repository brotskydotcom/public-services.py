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

import requests

from ..utils import (
    env,
    Environment,
    MapContext as MC,
    ATRecord,
    ANHash,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)


def fetch_donation_records() -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    """
    Get donor and donation records from Airtable,
    making a key-record map of the results.
    Returns them in a tuple (donors, donations).
    """
    MC.set("person")
    donor_records = fetch_all_records()
    MC.set("donation")
    donation_records = fetch_all_records()
    return donor_records, donation_records


def fetch_donations() -> Dict[str, List[ATRecord]]:
    """
    Find all the donations and their donors,
    constructing a map from donor URL to
    the donations made by that donor.
    """
    print(f"Creating records for donations...")
    MC.set("donation")
    session = requests.session()
    session.headers = MC.an_headers()
    donations_url = MC.an_base + "/donations"
    donors: Dict[str, List[ATRecord]] = {}
    donation_count = 0
    # fetch and process each page of donations
    page, total_pages = 1, 1
    while page <= total_pages:
        query = f"?page={page}"
        response = session.get(donations_url + query)
        response.raise_for_status()
        response.encoding = "utf-8"
        donations = response.json()
        item = ANHash.from_parts("donation page", donations)
        donation_urls = item.get_link_urls("osdi:donations")
        total_pages = item.properties()["total_pages"]
        print(
            f"Processing {len(donation_urls)} donations "
            f"on page {page} of {total_pages}..."
        )
        page += 1
        for i, link in enumerate(donation_urls):
            response = session.get(link)
            if response.status_code != 200:
                print(f"Response error on item {i}: {response.status_code}")
                print(f"Donation url was: {link}")
                continue
            response.encoding = "utf-8"
            donation = response.json()
            item = ANHash.from_parts("donation", donation)
            donation_record = ATRecord.from_donation(item)
            if not donation_record:
                print(f"Invalid donation hash, skipping: {donation}")
                continue
            donor_url = item.get_link_url("osdi:person")
            if not donor_url:
                print(f"No donor link, skipping: {donation}")
                continue
            donation_count += 1
            if val := donors.get(donor_url):
                val.append(donation_record)
            else:
                donors[donor_url] = [donation_record]
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(donation_urls)}...")
    print(f"Created {donation_count} donation records for {len(donors)} donors.")
    return donors


def fetch_donors(
    donor_map: Dict[str, List[ATRecord]]
) -> Tuple[Dict[str, ATRecord], Dict[str, ATRecord]]:
    """
    Get people info about donors from Action Network.
    This gives us their donor record and the email
    address for all of their donation records.  We
    then return the key->record maps for both the
    donors and the donations.
    """
    print(f"Creating records for {len(donor_map)} donors...")
    MC.set("person")
    session = requests.session()
    session.headers = MC.an_headers()
    donors: Dict[str, ATRecord] = {}
    donations: Dict[str, ATRecord] = {}
    for i, (url, donation_records) in enumerate(donor_map.items()):
        response = session.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        donor_data = response.json()
        donor_record = ATRecord.from_person(donor_data)
        donors[donor_record.key] = donor_record
        for donation_record in donation_records:
            # the Email field is an Airtable link field, which means it needs to
            # contain a list of the primary key (Email) of the donor.
            donation_record.core_fields["Email"] = [donor_record.key]
            donations[donation_record.key] = donation_record
        if (i + 1) % 10 == 0:
            print(f"Processed {i+1}/{len(donor_map)}...")
    print(f"Created {len(donors)} donor records.")
    if env() is Environment.DEV:
        ATRecord.dump_stats(len(donors))
    print(f"Updated {len(donations)} donation records with donor emails.")
    return donors, donations


def transfer_all_donations():
    print(f"Transferring all donors and donations...")
    donor_at_map, donation_at_map = fetch_donation_records()
    donor_url_map = fetch_donations()
    donor_an_map, donation_an_map = fetch_donors(donor_url_map)
    donor_comparison_map = compare_record_maps(donor_at_map, donor_an_map)
    donation_comparison_map = compare_record_maps(donation_at_map, donation_an_map)
    MC.set("person")
    make_record_updates(donor_comparison_map)
    print(f"Finished processing donors.")
    MC.set("donation")
    make_record_updates(donation_comparison_map)
    print(f"Finished processing donations.")
