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

from dateutil.parser import parse
from dateutil.tz import gettz
from haleasy import HALEasy, LinkNotFoundError

from .constants import MapContext as MC
from ..base import prinl


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
                if k not in (
                    "osdi:submission",
                    "osdi:donation",
                    "action_network:upload",
                ):
                    continue
                self = cls("https://actionnetwork.org", json_str=json.dumps(v))
                self.body = v
                if k == "osdi:donation":
                    self.form_name = "donation"
                    items.append(self)
                elif k == "action_network:upload":
                    self.form_name = "upload"
                    items.append(self)
                else:
                    if form_url := self.get_link_url("osdi:form"):
                        if form_name := MC.lookup_form_url(form_url):
                            self.form_name = form_name
                            items.append(self)
        return items

    def get_link_url(self, rel: str) -> Optional[str]:
        try:
            return self.link(rel=rel).href
        except LinkNotFoundError:
            return None

    def get_link_urls(self, rel: str) -> List[str]:
        return [link.href for link in self.links(rel=rel)]


@dataclass
class ATRecord:
    key: str
    mod_date: datetime
    # TODO: Fix the asymmetry in the naming of fields
    # - core fields have their *source* name
    # - custom fields have their *target* name
    core_fields: Dict[str, Any]
    custom_fields: Dict[str, Any]
    record_id: Optional[str] = ""
    at_match: Optional[ATRecord] = None

    est: ClassVar = gettz("EST")
    epoch: ClassVar = "1999-01-01 12:00:00 EST"

    an_core_fields: ClassVar[Dict[str, int]] = {}
    an_custom_fields: ClassVar[Dict[str, int]] = {}

    @classmethod
    def dump_stats(cls, count: int, reset: bool = True):
        prinl(f"Action Network core field counts for {count} people:")
        for fn, fc in ATRecord.an_core_fields.items():
            prinl(f"\t{fn}: {fc}")
        prinl(f"Action Network custom field counts for {count} people:")
        for fn, fc in ATRecord.an_custom_fields.items():
            prinl(f"\t{fn}: {fc}")
        if reset:
            cls.an_core_fields = {}
            cls.an_custom_fields = {}

    @classmethod
    def _from_fields(cls, key: str, core: Dict, custom: Dict) -> ATRecord:
        time_str = core["Timestamp (EST)"]
        time = parse(time_str, tzinfos={"EST": cls.est})
        return cls(key=core[key], mod_date=time, core_fields=core, custom_fields=custom)

    @classmethod
    def from_record(cls, record_data: Dict) -> Optional[ATRecord]:
        record_id = record_data["id"]
        key = MC.an_key_field()
        custom_fields = dict(record_data["fields"])
        core_field_map = MC.core_field_map()
        if not custom_fields.get(core_field_map[key]):
            prinl(f"Airtable {MC.get()} record {record_id} has no {key} field.")
            return None
        if not custom_fields.get(core_field_map["Timestamp (EST)"]):
            prinl(f"Airtable {MC.get()} record {record_id} has no import timestamp.")
            custom_fields[core_field_map["Timestamp (EST)"]] = cls.epoch
        core_fields = {}
        for an_name, at_name in core_field_map.items():
            if (value := custom_fields.get(at_name)) is not None:
                core_fields[an_name] = value
                del custom_fields[at_name]
        result = cls._from_fields(key=key, core=core_fields, custom=custom_fields)
        result.record_id = record_id
        return result

    @classmethod
    def from_person(cls, sub_data: Dict[str, Any]) -> ATRecord:
        for name in sub_data.keys():
            if name != "custom_fields":
                cls.an_core_fields[name] = cls.an_core_fields.get(name, 0) + 1
        emails: List[Dict] = sub_data["email_addresses"]
        email = next((a for a in emails if a.get("primary")), emails[0])
        addresses: List[Dict] = sub_data["postal_addresses"]
        address = next((a for a in addresses if a.get("primary")), {})
        first, last = sub_data.get("given_name", ""), sub_data.get("family_name", "")
        mobile_number, mobile_opt_in, landline_number = "", False, ""
        phones: List[Dict] = sub_data["phone_numbers"]
        phone = next((p for p in phones if p.get("primary")), {})
        if number := phone.get("number"):
            if phone.get("number_type") == "Mobile":
                mobile_number = number
                if phone.get("status") == "subscribed":
                    mobile_opt_in = True
            else:
                landline_number = number
        an_core_fields = {
            "Email": email["address"],
            "First name": first,
            "Last name": last,
            "Full name": f"{first} {last}",
            "Address": address.get("address_lines", [""])[0],
            "City": address.get("locality", ""),
            "State": address.get("region", ""),
            "Zip Code": address.get("postal_code", ""),
            "Timestamp (EST)": cls.convert_to_est(sub_data["modified_date"]),
            "Landline Number": landline_number,
            "Mobile Number": mobile_number,
            "Mobile Opt-In": mobile_opt_in,
        }
        core_fields: Dict[str, str] = {}
        core_field_map = MC.core_field_map()
        for an_name, an_value in an_core_fields.items():
            target_name = core_field_map.get(an_name)
            if target_name:
                core_fields[an_name] = an_value
        custom_fields: Dict[str, Any] = {}
        for an_name, an_value in sub_data["custom_fields"].items():
            cls.an_custom_fields[an_name] = cls.an_custom_fields.get(an_name, 0) + 1
            target_name = MC.target_custom_field(an_name)
            if target_name:
                custom_fields[target_name] = an_value
        return cls._from_fields(key="Email", core=core_fields, custom=custom_fields)

    @classmethod
    def from_donation(cls, item: ANHash) -> Optional[ATRecord]:
        # key: str = item["identifiers"][0]
        donation_url: str = item.link(rel="self").href
        key = donation_url[donation_url.rfind("/") + 1 :]
        # donations are never updated - the mod date is from the donor or amount
        create_date = cls.convert_to_est(item["created_date"])
        # find the amount
        amount = float(item["amount"])
        if amount == 0:
            return None
        if (currency := item["currency"]).lower() != "usd":
            prinl(f"Donation {key} currency ({currency}) is unexpected.")
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
    def from_donation_page(cls, page_id, data: Dict[str, Any]) -> ATRecord:
        core_fields = {
            "page_id": page_id,
            "Timestamp (EST)": cls.convert_to_est(data["modified_date"]),
        }
        custom_fields = {}
        for field in [
            "name",
            "title",
            "browser_url",
            "total_donations",
            "total_amount",
        ]:
            target_name = MC.target_custom_field(field)
            target_data = data.get(field)
            if target_name and target_data:
                custom_fields[target_name] = target_data
        return cls._from_fields(key="page_id", core=core_fields, custom=custom_fields)

    @classmethod
    def from_mobilize_event(cls, data: Dict[str, str]) -> Optional[ATRecord]:
        event_id = data.get("id")
        if not event_id:
            prinl(f"Event data has no 'id' field.")
            return None
        slot_id = data.get("timeslot_id")
        if slot_id:
            row_id = f"Event {event_id}-{slot_id}"
        else:
            row_id = f"Event {event_id}"
        updated_at = data.get("updated_at")
        if not updated_at:
            prinl(f"Event data has no 'updated_at' field.")
            return None
        event_state = data.get("state") or data.get("organization_state")
        core: Dict[str, str] = {
            "row_id": row_id,
            "Timestamp (EST)": cls.convert_to_est(updated_at),
            "email": data.get("event_owner_email_address"),
            "event_state": event_state if event_state else "",
        }
        custom: Dict[str, str] = {}
        for name, value in data.items():
            target_name = MC.target_custom_field(name)
            if target_name:
                custom[target_name] = value
        return cls._from_fields(key="row_id", core=core, custom=custom)

    @classmethod
    def from_mobilize_shift(cls, data: Dict[str, str]) -> ATRecord:
        email = data["email"]
        if event_id := data.get("event id"):
            if timeslot_id := data.get("timeslot id"):
                shift_id = f"User {email} at Event {event_id}-{timeslot_id}"
                event_id = f"Event {event_id}-{timeslot_id}"
            else:
                shift_id = f"User {email} at Event {event_id}"
                event_id = f"Event {event_id}"
        else:
            shift_id = f"User {email} at Unknown event"
            event_id = "<Unknown event>"
        for key in ["attended", "rating", "Spanish", "status"]:
            if not data.get(key):
                data.pop(key, None)
        updated_time = data["signup updated time"]
        est_time_str = cls.convert_to_est(updated_time)
        core_fields: Dict[str, str] = {
            "shift id": shift_id,
            "Timestamp (EST)": est_time_str,
            "event": event_id,
            "email": [email],
        }
        custom_fields: Dict[str, Any] = {}
        for name, value in data.items():
            target_name = MC.target_custom_field(name)
            if target_name:
                custom_fields[target_name] = value
        return cls._from_fields(key="shift id", core=core_fields, custom=custom_fields)

    @classmethod
    def from_mobilize_person(cls, data: Dict[str, str]) -> ATRecord:
        person_core_fields = {
            "Email": data["email"],
            "First name": data["first name"],
            "Last name": data["last name"],
            "Full name": data["first name"] + " " + data["last name"],
            "Timestamp (EST)": cls.convert_to_est(data["signup updated time"]),
        }
        person_custom_fields = {}
        target_name = MC.target_custom_field("utm_source")
        target_data = data.get("utm_source")
        if target_name and target_data:
            person_custom_fields[target_name] = target_data
        return cls._from_fields(
            key="Email", core=person_core_fields, custom=person_custom_fields
        )

    @classmethod
    def convert_to_est(cls, utc_str: str) -> str:
        utc_time = parse(utc_str)
        est_time = utc_time.astimezone(cls.est)
        return est_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def add_core_fields(self, updates: Dict[str, Any]):
        # Update all the core fields that have changed.
        for an_name, at_name in MC.core_field_map().items():
            if val := self.core_fields.get(an_name):
                if val != self.at_match.core_fields.get(an_name):
                    updates.update({at_name: val})

    def all_fields(self) -> Dict[str, Any]:
        core_field_map = MC.core_field_map()
        core_fields = {
            core_field_map[k]: v
            for k, v in self.core_fields.items()
            if v and core_field_map.get(k)
        }
        return {**core_fields, **self.custom_fields}

    def find_at_field_updates(self, assume_newer: bool = False) -> Dict[str, Any]:
        """Find fields that are newer on the Action Network side"""
        if not self.at_match:
            raise ValueError("Can't find updates without a matching record")
        if not assume_newer and self.mod_date <= self.at_match.mod_date:
            # never update except from a strictly newer AN record
            return {}
        an_fields = self.custom_fields
        at_fields = self.at_match.custom_fields
        updates = {}
        for an_k, an_v in an_fields.items():
            at_v = at_fields.get(an_k)
            if an_v and an_v != at_v:
                if an_k.lower().find("time") >= 0:
                    try:
                        an_date = parse(an_v)
                        at_date = parse(at_v)
                        if an_date == at_date:
                            continue
                    except (ValueError, OverflowError):
                        pass
                updates[an_k] = an_v
        if updates or not an_fields:
            self.add_core_fields(updates)
        return updates

    def is_preferred_to(self, other_record: ATRecord):
        record_type = MC.get()
        preferred_fields = {
            "event": ("Partner", "Event State*"),
            "shift": ("email", "Event*"),
        }
        for field_name in preferred_fields.get(record_type, ()):
            if self.custom_fields.get(
                field_name
            ) and not other_record.custom_fields.get(field_name):
                return True
        is_newer = self.mod_date >= other_record.mod_date
        return is_newer
