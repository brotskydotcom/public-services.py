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

from typing import Dict, Set, List

import requests

from ..utils import (
    ATRecord,
    ANHash,
    env,
    Environment,
    HashContext as HC,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)


def fetch_submitter_urls() -> Set[str]:
    """
    Find all the people who submitted the current form.
    Returns all the URLs for the individual people.

    Since the AN submissions endpoint pages its results,
    we have to iterate through each page.
    """
    print(f"Looking for submission hashes...")
    session = requests.session()
    session.headers = HC.an_headers()
    submitter_urls = set()
    page, total_pages = 1, 1
    while page <= total_pages:
        query = f"?page={page}"
        response = session.get(HC.an_submissions_url() + query)
        response.raise_for_status()
        response.encoding = "utf-8"
        submission = response.json()
        item = ANHash.from_parts(HC.get(), submission)
        links = item.links(rel="osdi:submissions")
        total_pages = item.properties()["total_pages"]
        print(
            f"Processing {len(links)} submissions "
            f"on page {page} of {total_pages}..."
        )
        page += 1
        for i, link in enumerate(links):
            response = session.get(link.href)
            if response.status_code >= 300:
                print(f"Response error on item {i}: {response.status_code}.")
                print(f"Submitter url was: {link}")
                continue
            response.encoding = "utf-8"
            submission = response.json()
            item = ANHash.from_parts(HC.get(), submission)
            submitter_url = item.link(rel="osdi:person").href
            submitter_urls.add(submitter_url)
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(links)}...")
    print(f"Found {len(submitter_urls)} submitters.")
    return submitter_urls


def fetch_submitters(submitter_urls: Set[str]) -> Dict[str, ATRecord]:
    """
    Get people info about submitters from Action Network.
    This includes their custom form fields, which are then
    turned into records mapped to their email address.
    """
    print(f"Creating records for {len(submitter_urls)} submitters...")
    session = requests.session()
    session.headers = HC.an_headers()
    people: Dict[str, ATRecord] = {}  # Map emails to records
    for i, url in enumerate(submitter_urls):
        response = session.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        submitter_data = response.json()
        record = ATRecord.from_person(submitter_data)
        people[record.key] = record
        if (i + 1) % 10 == 0:
            print(f"Processed {i+1}/{len(submitter_urls)}...")
    print(f"Created {len(people)} records for submitters.")
    if env() is Environment.DEV:
        ATRecord.dump_stats(len(people))
    return people


def transfer_all_forms(names: List[str]):
    for name in names:
        print(f"Transferring all submissions for form '{name}'...")
        HC.set(name)
        record_map = fetch_all_records()
        urls = fetch_submitter_urls()
        people_map = fetch_submitters(urls)
        comparison_map = compare_record_maps(record_map, people_map)
        make_record_updates(comparison_map)
        print(f"Finished processing for form '{name}'.")
