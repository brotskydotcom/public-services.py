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
import pickle
from typing import Optional, Tuple

import aiohttp

from ..db import redis, ItemListStore as Store
from ..utils import (
    Environment,
    env,
    prinl,
    log_error,
    MapContext as MC,
    ATRecord,
    ANHash,
    insert_or_update_record,
)


async def process_item_list(key: str) -> Optional[str]:
    """
    Process the items in the list with the given `key`.
    If there are temporary failures with some of the items on the list,
    we make a list of the items that failed, and return it.
    """
    prinl(f"Processing webhook items on list '{key}'...")
    success_count, fail_count = 0, 0
    environ, ident, rc = key.split(":")
    retry_key = f"{environ}:{ident}:{int(rc) + 1}"
    while item_data := await redis.db.lpop(key):
        form_name, body = pickle.loads(item_data)
        item = ANHash.from_parts(form_name, body)
        try:
            if form_name == "donation":
                prinl(f"Found donation item.")
                await transfer_donation(item)
            elif form_name == "upload":
                prinl(f"Found upload item.")
                await transfer_person(item)
            else:
                prinl(f"Found {form_name} submission item.")
                await transfer_person(item)
            success_count += 1
            if env() is Environment.DEV:
                logging_key = redis.get_key("Successfully processed")
                await redis.db.rpush(logging_key, item_data)
        except ValueError as e:
            msg = e.args[0] if e.args else "Invalid data"
            log_error(f"{msg}, ignoring item: {body}")
            success_count += 1
        except:
            log_error(f"Temporary error, will retry later")
            fail_count += 1
            await redis.db.rpush(retry_key, item_data)
            if env() is Environment.DEV:
                logging_key = redis.get_key("Failed to process")
                await redis.db.rpush(logging_key, item_data)
    if fail_count > 0:
        prinl(f"Failed to process {fail_count} item(s) on list '{key}'.")
        if int(rc) >= 5:
            prinl(f"Have already retried 5 times, giving up on failed items.")
        else:
            prinl(f"Saving failed item(s) for retry on list '{retry_key}'.")
            return retry_key
    prinl(f"Successfully processed {success_count} item(s) on list '{key}'.")
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


async def process_all_item_lists() -> Tuple[int, int]:
    prinl(f"Processing ready item lists...")
    count, retry_count = 0, 0
    try:
        while list_key := await Store.select_for_processing():
            count += 1
            fail_key = await process_item_list(list_key)
            if fail_key:
                await Store.add_retry_list(fail_key)
            await Store.remove_item_list(list_key)
    except redis.Error:
        log_error(f"Database failure")
    except asyncio.CancelledError:
        pass
    except:
        log_error(f"Unexpected failure")
    finally:
        prinl(f"Processed {count} item list(s); got {retry_count} retry list(s).")
    return count, retry_count
