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
from typing import Dict, List, Set, Sequence

import requests

from ..base import prinl, prinlv, Environment, env
from ..utils import (
    MapContext as MC,
    ATRecord,
    ANHash,
    fetch_all_records,
    compare_record_maps,
    make_record_updates,
)


def fetch_submitter_urls(form_name: str) -> Set[str]:
    """
    Find all the people who submitted the current form.
    Returns all the URLs for the individual people.

    Since the AN submissions endpoint pages its results,
    we have to iterate through each page.
    """
    prinl(f"Looking for submissions of form {form_name}...")
    session = requests.session()
    session.headers = MC.an_headers()
    submitter_urls = set()
    page, total_pages = 1, 1
    while page <= total_pages:
        query = f"?page={page}"
        response = session.get(MC.an_submissions_url(form_name) + query)
        response.raise_for_status()
        response.encoding = "utf-8"
        submission = response.json()
        item = ANHash.from_parts("submissions", submission)
        urls = item.get_link_urls("osdi:submissions")
        total_pages = item.properties()["total_pages"]
        prinl(
            f"Processing {len(urls)} submissions " f"on page {page} of {total_pages}..."
        )
        page += 1
        for i, url in enumerate(urls):
            response = session.get(url)
            if response.status_code != 200:
                prinl(f"Response error on item {i}: {response.status_code}.")
                prinl(f"Submitter url was: {url}")
                continue
            response.encoding = "utf-8"
            submission = response.json()
            item = ANHash.from_parts(form_name, submission)
            submitter_url = item.get_link_url("osdi:person")
            if not submitter_url:
                prinl(f"No person on submission: {submission}")
                continue
            submitter_urls.add(submitter_url)
            if (i + 1) % 10 == 0:
                prinlv(f"Processed {i + 1}/{len(urls)}...")
    prinl(f"Found {len(submitter_urls)} submitter(s).")
    return submitter_urls


def fetch_all_people_urls() -> List[str]:
    """
    Find all the people on the AN mailing list.
    Returns all their person URLs.

    Since the AN people endpoint pages its results,
    we have to iterate through each page.
    """
    prinl(f"Looking for all people...")
    session = requests.session()
    session.headers = MC.an_headers()
    url = MC.an_people_url()
    people_urls = list()
    page = 0
    while url:
        response = session.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        submission = response.json()
        item = ANHash.from_parts("people", submission)
        links = item.get_link_urls("osdi:people")
        people_urls += links
        page += 1
        prinl(f"Found {len(links)} people on page {page}...")
        url = item.get_link_url("next")
    prinl(f"Found {len(people_urls)} people on {page} page(s).")
    return people_urls


def fetch_people(people_urls: Sequence[str]) -> Dict[str, ATRecord]:
    """
    Get info about people from Action Network,
    including their core fields and custom form fields,
    and make records out of them (indexed by email address).
    """
    prinl(f"Creating records for {len(people_urls)} people...")
    session = requests.session()
    session.headers = MC.an_headers()
    people: Dict[str, ATRecord] = {}  # Map emails to records
    for i, url in enumerate(people_urls):
        response = session.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        submitter_data = response.json()
        record = ATRecord.from_person(submitter_data)
        people[record.key] = record
        if (i + 1) % 25 == 0:
            prinlv(f"Processed {i+1}/{len(people_urls)}...")
    prinl(f"Created {len(people)} records for people.")
    if env() is Environment.DEV:
        ATRecord.dump_stats(len(people))
    return people


def transfer_people(form_names: List[str], assume_newer=False):
    prinl(f"Transferring people...")
    MC.set("person")
    record_map = fetch_all_records()
    if form_names:
        urls = []
        for form_name in form_names:
            urls += fetch_submitter_urls(form_name)
    else:
        urls = fetch_all_people_urls()
    people_map = fetch_people(urls)
    comparison_map = compare_record_maps(record_map, people_map, assume_newer)
    make_record_updates(comparison_map, assume_newer)
    prinl(f"Finished transferring people.")
