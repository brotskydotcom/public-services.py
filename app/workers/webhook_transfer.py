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
import pickle

import aiohttp
from airtable import Airtable

from ..db import redis
from ..utils import ATRecord, ANHash, log_error, Environment, env
from ..utils import HashContext as HC


async def process_items(master_key: str, list_key: str):
    """
    Process the items in the list, queuing problems for later retry.
    """
    print(f"Processing webhook item list '{list_key}'...")
    success_count, fail_count = 0, 0
    environ, guid, retry_count = list_key.split(":")
    retry_key = ":".join((environ, guid, str(int(retry_count) + 1)))
    try:
        while item_data := await redis.db.lpop(list_key):
            form_name, body = pickle.loads(item_data)
            item = ANHash.from_parts(form_name, body)
            HC.set(form_name)
            print(f"Found submission for form {HC.get()}.")
            if await process_item(item):
                success_count += 1
                if env() is Environment.DEV:
                    logging_key = redis.get_key("Successfully processed")
                    redis.db.rpush(logging_key, item_data)
            else:
                fail_count += 1
                redis.db.rpush(retry_key, item_data)
                if env() is Environment.DEV:
                    logging_key = redis.get_key("Failed to process")
                    redis.db.rpush(logging_key, item_data)
        if fail_count > 0:
            print(f"Failed to process {fail_count} item(s).")
            if int(retry_count) >= 4:
                print(f"Have already tried 5 times, not trying again.")
            else:
                print(f"Will save failed item(s) for later retry.")
                redis.db.rpush(master_key, retry_key)
    except redis.Error:
        log_error(f"Failed to retrieve or update list items")
    print(f"List '{list_key}' done: processed {success_count} item(s) successfully.")


async def process_item(item: ANHash) -> bool:
    at_key, at_base, at_table, at_typecast = HC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    url = item.link(rel="osdi:person").href
    async with aiohttp.ClientSession(headers=HC.an_headers()) as s:
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    submitter = await r.json(encoding="utf-8")
                else:
                    print(f"Invalid person link: status {r.status}")
                    return True
        except:
            log_error("Error fetching submitter info")
            return False
    an_record = ATRecord.from_person(submitter)
    try:
        record_dict = at.match(HC.core_field_map()["Email"], an_record.key)
    except:
        log_error("Error searching for matching Airtable record")
        return False
    if record_dict:
        print(f"Found existing record for {an_record.key}.")
        at_record = ATRecord.from_record(record_dict)
        if not at_record:
            print(f"Matching record is not valid, skipping webhook.")
            return False
        an_record.at_match = at_record
        updates = an_record.find_at_field_updates()
        if updates:
            print(f"Updating {len(updates)} fields in record.")
            try:
                at.update(at_record.record_id, updates, typecast=at_typecast)
            except:
                log_error("Error updating record")
                return False
        else:
            print(f"No fields need update in record.")
    else:
        print(f"Uploading new record for {an_record.key}.")
        try:
            at.insert(an_record.all_fields(), typecast=at_typecast)
        except:
            log_error("Error uploading record")
            return False
    return True


async def transfer_all_webhook_items():
    print(f"Processing all webhook item lists...")
    ani_key: str = redis.get_key("Submitted Items")
    try:
        while list_key := await redis.db.lpop(ani_key, encoding="ascii"):
            await process_items(ani_key, list_key)
    except redis.Error:
        log_error(f"Error fetching webhook item list")
    print(f"Processed all webhook item lists.")
