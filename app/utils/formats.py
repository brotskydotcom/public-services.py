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
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional, ClassVar

from airtable import Airtable
from dateutil.parser import parse
from dateutil.tz import gettz
from haleasy import HALEasy, LinkNotFoundError

from .constants import HashContext as HC


class ANHash(HALEasy):
    form_name: str
    body: Any

    @classmethod
    def from_parts(cls, form_name: str, body: Any) -> ANHash:
        self = cls("https://actionnetwork.org", json_str=json.dumps(body))
        self.form_name = form_name
        self.body = body
        return self

    @classmethod
    def find_items(cls, data: List[Dict]) -> List[ANHash]:
        """
        Find all the submissions of known hashes in the data list.
        """
        items = []
        for d in data:
            for k, v in d.items():
                if k not in ("osdi:submission", "osdi:donation"):
                    continue
                self = cls("https://actionnetwork.org", json_str=json.dumps(v))
                self.body = v
                if k == "osdi:donation":
                    self.form_name = "donation"
                    items.append(self)
                else:
                    try:
                        form_url = self.link(rel="osdi:form").href
                        if form_name := HC.lookup_form(form_url):
                            self.form_name = form_name
                            items.append(self)
                    except LinkNotFoundError:
                        pass
        return items


@dataclass
class ATRecord:
    key: str
    mod_date: datetime
    core_fields: Dict[str, str]
    custom_fields: Dict[str, Any]
    record_id: Optional[str] = ""
    at_match: Optional[ATRecord] = None

    est: ClassVar = gettz("EST")
    epoch: ClassVar = "1999-01-01 12:00:00 EST"

    an_core_fields: ClassVar[Dict[str, int]] = {}
    an_custom_fields: ClassVar[Dict[str, int]] = {}

    @classmethod
    def dump_stats(cls, count: int, reset: bool = True):
        print(f"Action Network core field counts for {count} people:")
        for fn, fc in ATRecord.an_core_fields.items():
            print(f"\t{fn}: {fc}")
        print(f"Action Network custom field counts for {count} people:")
        for fn, fc in ATRecord.an_custom_fields.items():
            print(f"\t{fn}: {fc}")
        if reset:
            cls.an_core_fields = {}
            cls.an_custom_fields = {}

    @classmethod
    def _from_fields(cls, key: str, core: Dict, custom: Dict) -> ATRecord:
        time_str = core["Timestamp (EST)"]
        time = parse(time_str, tzinfos={"EST": cls.est})
        return cls(
            key=core[key], mod_date=time, core_fields=core, custom_fields=custom,
        )

    @classmethod
    def from_record(cls, record_data: Dict) -> Optional[ATRecord]:
        custom_fields = dict(record_data["fields"])
        core_field_map = HC.core_field_map()
        if not custom_fields.get(core_field_map["Email"]):
            print(f"Airtable record has no Email field; skipping it: {record_data}")
            return None
        if not custom_fields.get(core_field_map["Timestamp (EST)"]):
            print(f"Airtable record has no Timestamp field; adding it: {record_data}")
            custom_fields[core_field_map["Timestamp (EST)"]] = cls.epoch
        core_fields = {}
        for an_name, at_name in core_field_map.items():
            if (value := custom_fields.get(at_name)) is not None:
                core_fields[an_name] = value
                del custom_fields[at_name]
        result = cls._from_fields(
            key=next(iter(core_field_map.keys())),
            core=core_fields,
            custom=custom_fields,
        )
        result.record_id = record_data["id"]
        return result

    @classmethod
    def from_person(cls, sub_data: Dict[str, Any]) -> ATRecord:
        for name in sub_data.keys():
            if name != "custom_fields":
                cls.an_core_fields[name] = cls.an_core_fields.get(name, 0) + 1
        emails = sub_data["email_addresses"]
        email = next((a for a in emails if a.get("primary")), emails[0])
        addresses = sub_data["postal_addresses"]
        address = next((a for a in addresses if a.get("primary")), addresses[0])
        street = address.get("address_lines", [""])[0]
        an_core_fields = {
            "Email": email["address"],
            "First name": sub_data.get("given_name", ""),
            "Last name": sub_data.get("family_name", ""),
            "Address": street,
            "City": address.get("locality", ""),
            "State": address.get("region", ""),
            "Zip Code": address.get("postal_code", ""),
            "Timestamp (EST)": cls.convert_to_est(sub_data["modified_date"]),
        }
        core_fields: Dict[str, str] = {}
        core_field_map = HC.core_field_map()
        for an_name, an_value in an_core_fields.items():
            target_name = core_field_map.get(an_name)
            if target_name:
                core_fields[an_name] = an_value
        custom_fields: Dict[str, Any] = {}
        for an_name, an_value in sub_data["custom_fields"].items():
            cls.an_custom_fields[an_name] = cls.an_custom_fields.get(an_name, 0) + 1
            target_name = HC.target_custom_field(an_name)
            if target_name:
                custom_fields[target_name] = an_value
        return cls._from_fields(key="Email", core=core_fields, custom=custom_fields)

    @classmethod
    def from_donation(cls, item: ANHash) -> Optional[ATRecord]:
        # key: str = item["identifiers"][0]
        donation_url: str = item.link(rel="self").href
        key = donation_url[donation_url.rfind("/") + 1 :]
        # donations are never updated - the mod date is from the donor
        create_date = cls.convert_to_est(item["created_date"])
        # find the amount
        amount = float(item["amount"])
        if amount == 0:
            print(f"Donation's amount is 0, ignoring it.")
            return None
        if (currency := item["currency"]) != "usd":
            print(f"Donation {key} currency ({currency}) is unexpected.")
        # compare with EPP total
        # total = 0
        # for part in item["recipients"]:
        #     if part["display_name"] == "Everyday People PAC":
        #         total += float(part["amount"])
        # if total != amount:
        #     print(f"Donation's EPP portion ({total}) doesn't match total ({amount})")
        #     print(f"Full recipient list is: {item['recipients']}")
        # find the recurrence
        if item["action_network:recurrence"]["recurring"]:
            period = item["action_network:recurrence"]["period"]
            if not period:
                period = "None"
            if period.lower() in ("monatlich", "elke maand"):
                period = "Monthly"
            if period not in ("None", "Weekly", "Monthly", "Every 3 Months", "Yearly"):
                period = "Other"
        else:
            period = "None"
        return cls._from_fields(
            key="Donation ID",
            core={
                "Donation ID": key,
                "Email": "unknown",
                "Donation Date": create_date[0:10],
                "Donation Amount": amount,
                "Recurrence": period,
                "Timestamp (EST)": create_date,
            },
            custom={},
        )

    @classmethod
    def convert_to_est(cls, utc_str: str) -> str:
        utc_time = parse(utc_str)
        est_time = utc_time.astimezone(cls.est)
        return est_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def add_core_fields(self, updates: Dict[str, Any]):
        # Update all the core fields that have changed.
        core_field_map = HC.core_field_map()
        updates.update(
            (at_name, self.core_fields[an_name])
            for an_name, at_name in core_field_map.items()
            if self.core_fields[an_name] != self.at_match.core_fields[an_name]
        )

    def all_fields(self) -> Dict[str, Any]:
        core_field_map = HC.core_field_map()
        core_fields = {
            core_field_map[k]: v
            for k, v in self.core_fields.items()
            if core_field_map.get(k)
        }
        return {**core_fields, **self.custom_fields}

    def find_at_field_updates(self) -> Dict[str, Any]:
        """Find fields that are newer on the Action Network side"""
        if not self.at_match:
            raise ValueError("Can't find updates without a matching record")
        if self.mod_date <= self.at_match.mod_date:
            # never update from an older AN record
            return {}
        an_fields = self.custom_fields
        at_fields = self.at_match.custom_fields
        updates = {}
        for an_k, an_v in an_fields.items():
            at_v = at_fields.get(an_k)
            if an_v and an_v != at_v:
                updates[an_k] = an_v
        if updates or not an_fields:
            self.add_core_fields(updates)
        return updates


def compare_record_maps(
    at_map: Dict[str, ATRecord], an_map: Dict[str, ATRecord]
) -> Dict[str, Dict]:
    print(
        f"Comparing {len(at_map)} Airtable record(s) "
        f"with {len(an_map)} Action Network record(s)..."
    )
    at_only, an_only, an_newer, matching = {}, dict(an_map), {}, {}
    for at_k, at_v in at_map.items():
        an_v = an_map.get(at_k)
        if an_v:
            del an_only[at_k]
            an_v.at_match = at_v  # remember airbase match
            if an_v.mod_date > at_v.mod_date:
                an_newer[at_k] = an_v
            else:
                matching[at_k] = an_v
        else:
            at_only[at_k] = at_v
    print(
        f"Found {len(an_only)} new, "
        f"{len(an_newer)} updated, and "
        f"{len(matching)} matching Action Network records."
    )
    if len(at_only) > 0:
        print(f"Found {len(at_only)} Airtable record(s) without a match.")
    result = {
        "at_only": at_only,
        "an_only": an_only,
        "an_newer": an_newer,
        "matching": matching,
    }
    return result


def fetch_all_records() -> Dict[str, ATRecord]:
    """
    Get application records from Airtable.
    Returns them in a map from email (key) to record.
    """
    at_key, at_base, at_table, _ = HC.at_connect_info()
    print(f"Looking for {HC.get()} records...")
    at = Airtable(at_base, at_table, api_key=at_key)
    results: Dict[str, ATRecord] = {}
    for record_dict in at.get_all():
        record = ATRecord.from_record(record_dict)
        if record:
            results.update({record.key: record})
    print(f"Found {len(results)} {HC.get()} record(s).")
    return results


def make_record_updates(comparison_map: Dict[str, Dict[str, ATRecord]]):
    """Update Airtable from newer Action Network records"""
    at_key, at_base, at_table, at_typecast = HC.at_connect_info()
    at = Airtable(at_base, at_table, api_key=at_key)
    an_only = comparison_map["an_only"]
    did_update = False
    if an_only:
        did_update = True
        print(f"Updating {HC.get()} records...")
        print(f"Uploading {len(an_only)} new record(s)...")
        records = [r.all_fields() for r in an_only.values()]
        at.batch_insert(records, typecast=at_typecast)
    an_newer: Dict[str, ATRecord] = comparison_map["an_newer"]
    if an_newer:
        update_map: Dict[str, Dict[str, Any]] = {}
        for key, record in an_newer.items():
            updates = record.find_at_field_updates()
            if updates:
                update_map[record.at_match.record_id] = updates
        if update_map:
            if not did_update:
                print(f"Updating {HC.get()} records...")
            did_update = True
            print(f"Updating {len(update_map)} existing record(s)...")
            for i, (record_id, updates) in enumerate(update_map.items()):
                at.update(record_id, updates, typecast=at_typecast)
                if (i + 1) % 10 == 0:
                    print(f"Processed {i+1}/{len(an_newer)}...")
    if not did_update:
        print(f"No updates required for {HC.get()} records.")
