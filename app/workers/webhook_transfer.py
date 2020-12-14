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
from typing import Optional, Tuple

import aiohttp

from ..base import Environment, env, prinl, prinlv, log_error
from ..db import redis, ItemListStore as Store
from ..utils import (
    MapContext as MC,
    ATRecord,
    ANHash,
    insert_or_update_record,
)


async def process_webhook_list(key: str) -> Optional[str]:
    """
    Process the items in the webhook list with the given `key`.
    If there are temporary failures with some of the items on the list,
    we make a list of the items that failed, and return it.
    """
    prefix, rc = key.split(":")
    item_count = await redis.db.llen(key)
    if item_count == 0:
        return
    prinlv(f"Processing {item_count} webhook item(s) on list '{key}'...")
    count, good_count, bad_count = 0, 0, 0
    if int(rc) < 5:
        retry_key = f"{prefix}:{int(rc) + 1}"
        retry = True
    else:
        retry_key = f"{prefix}:0"
        retry = False
    while item_data := await redis.db.lpop(key):
        count += 1
        form_name, body = pickle.loads(item_data)
        prinlv(f"Item #{count}/{item_count} has type '{form_name}'.")
        item = ANHash.from_parts(form_name, body)
        try:
            if form_name == "donation":
                # AN donation record
                await transfer_donation(item)
            else:
                # either an AN person upload or an AN web form submission,
                # both just give us a new contact to transfer.
                await transfer_person(item)
            good_count += 1
            if env() is Environment.DEV:
                logging_key = redis.get_key("Successfully processed")
                await redis.db.rpush(logging_key, item_data)
        except ValueError as e:
            msg = e.args[0] if e.args else "Invalid data"
            debug_id = item.get_link_url("self") or hash(body)
            prinlv(f"{msg}, ignoring item #{count}: {debug_id}")
            good_count += 1
        except:
            log_error(f"Error on {form_name} item #{count}, will retry later")
            bad_count += 1
            await redis.db.rpush(retry_key, item_data)
    prinlv(
        f"Successfully processed {good_count} of {item_count} item(s) on list '{key}'."
    )
    if bad_count > 0:
        prinl(f"Failed to process {bad_count} of {item_count} item(s) on list '{key}'.")
        if retry:
            prinl(f"Saving failed item(s) for retry on list '{retry_key}'.")
            return retry_key
        else:
            prinl(f"Deferring failed item(s) for later on list '{retry_key}'.")
            await Store.add_deferred_list("webhook", retry_key)
    return None


async def transfer_donation(item: ANHash):
    """Transfer the donation to Airtable"""
    an_record = ATRecord.from_donation(item)
    if not an_record:
        raise ValueError(f"Donation amount is 0")
    email = await transfer_person(item)
    await transfer_donation_page(item)
    MC.set("donation")
    an_record.core_fields["Email"] = [email]
    await insert_or_update_record(an_record)


async def transfer_donation_page(item: ANHash):
    """Transfer the donation page to Airtable."""
    MC.set("donation page")
    url = item.get_link_url("osdi:fundraising_page")
    if not url:
        raise ValueError("No fundraising page link")
    async with aiohttp.ClientSession(headers=MC.an_headers()) as s:
        async with s.get(url) as r:
            if r.status == 200:
                page_data = await r.json(encoding="utf-8")
            else:
                raise ValueError(f"Donation page lookup failed: status {r.status}")
    page_id = url[url.rfind("/") + 1 :]
    an_record = ATRecord.from_donation_page(page_id, page_data)
    if not an_record:
        raise ValueError("Invalid donation page info")
    await insert_or_update_record(an_record)


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
    await insert_or_update_record(an_record)
    return an_record.key


async def process_webhook_lists() -> Tuple[int, int]:
    prinlv(f"Processing ready webhook item lists...")
    count, retry_count = 0, 0
    try:
        while list_key := await Store.select_for_processing("webhook"):
            count += 1
            fail_key = await process_webhook_list(list_key)
            if fail_key:
                retry_count += 1
                await Store.add_retry_list("webhook", fail_key)
            await Store.remove_processed_list("webhook", list_key)
    except redis.Error:
        log_error(f"Database failure during webhook processing")
    except:
        log_error(f"Unexpected exception during webhook processing")
    finally:
        prinlv(f"Processed {count} item list(s); got {retry_count} retry list(s).")
    return count, retry_count
