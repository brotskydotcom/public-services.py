import os

import aiohttp
from airtable import Airtable

from ..db import database
from ..db.model import an_form_items as ani
from ..utils import ATRecord, ANSubmission, log_error
from ..utils import FormContext as FC


async def process_items():
    print(f"Looking for unprocessed webhook items...")
    get_items = ani.select().order_by(ani.c.id.asc())
    delete_item_list = []
    try:
        async for row in database.iterate(get_items):
            item_id = row[ani.c.id]
            item = ANSubmission.from_body_text(id=item_id, body_text=row[ani.c.body])
            FC.set(row[ani.c.form_name])
            print(
                f"Found submission for form {FC.get()}, "
                f"table {FC.at_connect_info()[2]}."
            )
            if await process_item(item):
                delete_item_list.append(item_id)
        # check if we can delete the final receipt
        if delete_item_list:
            print(f"Deleting {len(delete_item_list)} fully processed item(s).")
            async with database.transaction():
                for item_id in delete_item_list:
                    delete_item = ani.delete().where(ani.c.id == item_id)
                    await database.execute(delete_item)
    except:
        log_error("Error while accessing database")
    print(f"Item processing done.")


async def process_item(item: ANSubmission) -> bool:
    at_key, at_base, at_table, at_typecast = FC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    url = item.link(rel="osdi:person").href
    async with aiohttp.ClientSession(headers=FC.an_headers()) as s:
        try:
            async with s.get(url) as r:
                if r.status != 200:
                    print(
                        f"GRU submission {item.id} has an invalid person link: "
                        f"status {r.status}"
                    )
                    return True
                submitter = await r.json(encoding="utf-8")
        except:
            log_error("Error fetching submitter info")
            return False
    an_record = ATRecord.from_submitter(submitter)
    try:
        record_dict = at.match(FC.core_field_map()["Email"], an_record.key)
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
    await process_items()
