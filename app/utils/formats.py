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

from .constants import FormContext as FC


class ANSubmission(HALEasy):
    id: int
    form_name: str
    body: Any

    def __init__(
        self, *args, id: Optional[int] = 0, body: Optional[Dict] = None, **kwargs
    ):
        self.id = id
        self.body = body or {}
        args = args or ("https://actionnetwork.org",)
        if kwargs.get("json_str") is None:
            kwargs.update(json_str=json.dumps(body))
        super().__init__(*args, **kwargs)

    @classmethod
    def from_body_text(cls, id: int, body_text: str) -> ANSubmission:
        json_data: Dict = json.loads(body_text)
        result = cls(id=id, body=json_data)
        return result

    @classmethod
    def find_items(cls, data: List[Dict]) -> List[ANSubmission]:
        """
        Find all the submissions of known forms in the data list.
        """
        items = []
        for d in data:
            for k, v in d.items():
                if k != "osdi:submission":
                    continue
                item = cls(id=len(items) + 1, body=v)
                if form_name := item.get_form_name():
                    item.form_name = form_name
                    items.append(item)
        return items

    def as_json(self) -> str:
        return json.dumps(self.body)

    def get_form_name(self) -> Optional[str]:
        """
        Check if this submission has the right name for the context.
        If so, return the form name.  If not, return None.
        """
        try:
            form_name = FC.lookup(self.link(rel="osdi:form").href)
        except LinkNotFoundError:
            return None
        return form_name


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
    def from_fields(cls, core_fields: Dict, custom_fields: Dict) -> ATRecord:
        time_str = core_fields["Timestamp (EST)"]
        time = parse(time_str, tzinfos={"EST": cls.est})
        return cls(
            key=core_fields["Email"],
            mod_date=time,
            core_fields=core_fields,
            custom_fields=custom_fields,
        )

    @classmethod
    def from_record(cls, record_data: Dict) -> Optional[ATRecord]:
        custom_fields = dict(record_data["fields"])
        core_field_map = FC.core_field_map()
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
        result = cls.from_fields(core_fields=core_fields, custom_fields=custom_fields)
        result.record_id = record_data["id"]
        return result

    @classmethod
    def from_submitter(cls, sub_data: Dict[str, Any]) -> ATRecord:
        for name in sub_data.keys():
            if name != "custom_fields":
                cls.an_core_fields[name] = cls.an_core_fields.get(name, 0) + 1
        emails = sub_data["email_addresses"]
        email = next((a for a in emails if a.get("primary")), emails[0])
        addresses = sub_data["postal_addresses"]
        address = next((a for a in addresses if a.get("primary")), addresses[0])
        street = address.get("address_lines", [""])[0]
        phone_fields = {
            k: v
            for k, v in sub_data.items()
            if k.find("hone") >= 0 or k.find("obile") > 0
        }
        if phone_fields:
            print(f"Found phone fields: {phone_fields}")
            pass
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
        core_field_map = FC.core_field_map()
        for an_name, an_value in an_core_fields.items():
            target_name = core_field_map.get(an_name)
            if target_name:
                core_fields[an_name] = an_value
        custom_fields: Dict[str, Any] = {}
        for an_name, an_value in sub_data["custom_fields"].items():
            cls.an_custom_fields[an_name] = cls.an_custom_fields.get(an_name, 0) + 1
            target_name = FC.target_custom_field(an_name)
            if target_name:
                custom_fields[target_name] = an_value
        return cls.from_fields(core_fields=core_fields, custom_fields=custom_fields)

    @classmethod
    def convert_to_est(cls, utc_str: str) -> str:
        utc_time = parse(utc_str)
        est_time = utc_time.astimezone(cls.est)
        return est_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def add_core_fields(self, updates: Dict[str, Any]):
        # Don't update email since that's the key field
        # used to do the match in the first place.
        core_field_map = FC.core_field_map()
        updates.update(
            (at_name, self.core_fields[an_name])
            for an_name, at_name in core_field_map.items()
            if an_name != "Email"
        )

    def all_fields(self) -> Dict[str, Any]:
        core_field_map = FC.core_field_map()
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
        if updates:
            self.add_core_fields(updates)
        return updates
