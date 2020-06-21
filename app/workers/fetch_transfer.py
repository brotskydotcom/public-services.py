from typing import Dict, Set, Any, List

import requests
from airtable import Airtable

from ..utils import ATRecord, ANSubmission
from ..utils import FormContext as FC


def fetch_records() -> Dict[str, ATRecord]:
    """
    Get application records from Airtable.
    Returns them in a map from email (key) to record.

    The Airtable API is paged, but the wrapper takes
    care of that under the covers.
    """
    at_key, at_base, at_table, _ = FC.at_connect_info()
    print(f"Looking for records in table '{at_table}'...")
    at = Airtable(at_base, at_table, api_key=at_key)
    results = {}    # keep a map from email (key) to record
    for record_dict in at.get_all():
        record = ATRecord.from_record(record_dict)
        if record:
            results.update({record.key: record})
    print(f"Found {len(results)} records.")
    return results


def fetch_submitter_urls() -> Set[str]:
    """
    Find all the people who submitted the current form.
    Returns all the URLs for the individual people.

    Since the AN submissions endpoint pages its results,
    we have to iterate through each page.
    """
    print(f"Looking for submission hashes...")
    session = requests.session()
    session.headers = FC.an_headers()
    submitter_urls = set()
    page, total_pages = 1, 1
    while page <= total_pages:
        query = f"?page={page}"
        response = session.get(FC.an_submissions_url() + query)
        response.raise_for_status()
        response.encoding = 'utf-8'
        item = ANSubmission(body=response.json())
        links = item.links(rel='osdi:submissions')
        total_pages = item.properties()['total_pages']
        print(f"Processing {len(links)} submissions "
              f"on page {page} of {total_pages}...")
        page += 1
        for i, link in enumerate(links):
            response = session.get(link.href)
            response.raise_for_status()
            response.encoding = 'utf-8'
            submission = response.json()
            item = ANSubmission(body=submission)
            submitter_url = item.link(rel='osdi:person').href
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
    session.headers = FC.an_headers()
    people: Dict[str, ATRecord] = {}    # Map emails to records
    for i, url in enumerate(submitter_urls):
        response = session.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8'
        submitter_data = response.json()
        record = ATRecord.from_submitter(submitter_data)
        people[record.key] = record
        if (i+1) % 10 == 0:
            print(f"Processed {i+1}/{len(submitter_urls)}...")
    print(f"Created {len(people)} records for submitters.")
    return people


def compare_record_maps(at_map: Dict[str, ATRecord],
                        an_map: Dict[str, ATRecord]) -> Dict[str, Dict]:
    print(f"Comparing {len(at_map)} records with {len(an_map)} submitters...")
    at_only, an_only, an_newer, matching = {}, dict(an_map), {}, {}
    for at_k, at_v in at_map.items():
        an_v = an_map.get(at_k)
        if an_v:
            del an_only[at_k]
            an_v.at_match = at_v    # remember airbase match
            if an_v.mod_date > at_v.mod_date:
                an_newer[at_k] = an_v
            else:
                matching[at_k] = an_v
        else:
            at_only[at_k] = at_v
    print(f"Found {len(an_only)} new, "
          f"{len(an_newer)} updated, and "
          f"{len(matching)} matching submitters.")
    if len(at_only) > 0:
        print(f"Note: there are {len(at_only)} applications "
              f"without a submission.")
    result = {'at_only': at_only, 'an_only': an_only,
              'an_newer': an_newer, 'matching': matching}
    return result


def make_at_updates(comparison_map: Dict[str, Dict[str, ATRecord]]):
    """Update Airtable from newer Action Network records"""
    at_key, at_base, at_table, at_typecast = FC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    an_only = comparison_map['an_only']
    did_update = False
    if an_only:
        did_update = True
        print(f"Updating table '{at_table}'...")
        print(f"Uploading {len(an_only)} new records...")
        records = [r.all_fields() for r in an_only.values()]
        at.batch_insert(records, typecast=at_typecast)
    an_newer: Dict[str, ATRecord] = comparison_map['an_newer']
    if an_newer:
        update_map: Dict[str, Dict[str, Any]] = {}
        for key, record in an_newer.items():
            updates = record.find_at_field_updates()
            if updates:
                update_map[record.at_match.record_id] = updates
        if update_map:
            if not did_update:
                print(f"Updating table '{at_table}'...")
            did_update = True
            print(f"Updating {len(update_map)} existing record(s)...")
            for i, (record_id, updates) in enumerate(update_map.items()):
                at.update(record_id, updates, typecast=at_typecast)
                if (i+1) % 10 == 0:
                    print(f"Processed {i+1}/{len(an_newer)}...")
    if not did_update:
        print(f"No updates required to table '{at_table}'.")


def transfer_all_forms(names: List[str]):
    for name in names:
        FC.set(name)
        record_map = fetch_records()
        urls = fetch_submitter_urls()
        people_map = fetch_submitters(urls)
        comparison_map = compare_record_maps(record_map, people_map)
        make_at_updates(comparison_map)
