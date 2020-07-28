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
from typing import Optional

import aiohttp

from ..db import redis
from ..utils import (
    Environment,
    env,
    log_error,
    MapContext as MC,
    ATRecord,
    ANHash,
    insert_or_update_record,
)


async def process_items(list_key: str) -> Optional[str]:
    """
    Process the items in the list, queuing problems for later retry.
    """
    print(f"Processing webhook items on '{list_key}'...")
    success_count, fail_count = 0, 0
    environ, guid, retry_count = list_key.split(":")
    retry_key = ":".join((environ, guid, str(int(retry_count) + 1)))
    while item_data := await redis.db.lpop(list_key):
        form_name, body = pickle.loads(item_data)
        item = ANHash.from_parts(form_name, body)
        try:
            if form_name == "donation":
                print(f"Found donation item.")
                await transfer_donation(item)
            elif form_name == "upload":
                print(f"Found upload item.")
                await transfer_person(item)
            else:
                print(f"Found {form_name} submission item.")
                await transfer_person(item)
            success_count += 1
            if env() is Environment.DEV:
                logging_key = redis.get_key("Successfully processed")
                await redis.db.rpush(logging_key, item_data)
        except ValueError:
            log_error(f"Invalid data, ignoring item: {body}")
            success_count += 1
        except:
            log_error(f"Temporary error, will retry later")
            fail_count += 1
            await redis.db.rpush(retry_key, item_data)
            if env() is Environment.DEV:
                logging_key = redis.get_key("Failed to process")
                await redis.db.rpush(logging_key, item_data)
    if fail_count > 0:
        print(f"Failed to process {fail_count} item(s).")
        if int(retry_count) >= 4:
            print(f"Have already tried 5 times, not trying again.")
        else:
            print(f"Will save failed item(s) for later retry.")
            return retry_key
    print(f"List '{list_key}' done: processed {success_count} item(s) successfully.")
    return None


async def transfer_donation(item: ANHash):
    """Transfer the donation to Airtable"""
    an_record = ATRecord.from_donation(item)
    if not an_record:
        raise ValueError(f"Invalid donation info")
    an_record.core_fields["Email"] = [await transfer_person(item)]
    MC.set("donation")
    insert_or_update_record(an_record)


async def transfer_person(item: ANHash) -> str:
    """Transfer the person to Airtable, returning their key field"""
    MC.set("person")
    url = item.get_link_url("osdi:person")
    if not url:
        raise ValueError("No person link")
    async with aiohttp.ClientSession(headers=MC.an_headers()) as s:
        async with s.get(url) as r:
            if r.status == 200:
                submitter = await r.json(encoding="utf-8")
            else:
                raise ValueError(f"Person lookup failed: status {r.status}")
    an_record = ATRecord.from_person(submitter)
    if not an_record:
        raise ValueError("Invalid person info")
    insert_or_update_record(an_record)
    return an_record.key


async def transfer_all_webhook_items():
    master_list: str = redis.get_key("Submitted Items")
    try:
        count = await redis.db.llen(master_list)
        print(f"Processing {count} webhook item list(s)...")
        for i in range(count):
            try_list = await redis.db.lpop(master_list, encoding="ascii")
            retry_list = await process_items(try_list)
            if retry_list:
                await redis.db.rpush(master_list, try_list)
        new_count = await redis.db.llen(master_list)
    except redis.Error:
        log_error(f"Error fetching or saving webhook item list")
    print(f"Processed {count} webhook item list(s); got {new_count} retry list(s).")
