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

    def __init__(self,
                 *args,
                 id: Optional[int] = 0,
                 body: Optional[Dict] = None,
                 **kwargs):
        self.id = id
        self.body = body or {}
        args = args or ('https://actionnetwork.org',)
        if kwargs.get('json_str') is None:
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
                if k != 'osdi:submission':
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
    record_id: Optional[str] = ''
    at_match: Optional[ATRecord] = None

    est: ClassVar = gettz('EST')

    core_field_list: ClassVar[List[str]] = [
        # the core AN fields to update on the AT side
        # whenever an update is made to the AT side
        'Email',
        'First name',
        'Last name',
        'Timestamp (EST)',
    ]

    @classmethod
    def from_fields(cls, core_fields: Dict, custom_fields: Dict) -> ATRecord:
        time_str = core_fields['Timestamp (EST)']
        time = parse(time_str, tzinfos={'EST': cls.est})
        return cls(key=core_fields['Email'], mod_date=time,
                   core_fields=core_fields,
                   custom_fields=custom_fields)

    @classmethod
    def from_record(cls, record_data: Dict) -> Optional[ATRecord]:
        custom_fields = dict(record_data['fields'])
        core_fields = {}
        for name in cls.core_field_list:
            if (value := custom_fields.get(name)) is not None:
                core_fields[name] = value
                del custom_fields[name]
            else:
                print(f"Record schema is missing required "
                      f"field {name}: {record_data}")
                return None
        result = cls.from_fields(core_fields=core_fields,
                                 custom_fields=custom_fields)
        result.record_id = record_data['id']
        return result

    @classmethod
    def from_submitter(cls, sub_data: Dict[str, Any]) -> ATRecord:
        addresses = sub_data['email_addresses']
        address = next((a for a in addresses if a.get('primary')), addresses[0])
        core_fields = {
            'Email': address['address'],
            'First name': sub_data['given_name'],
            'Last name': sub_data['family_name'],
            'Timestamp (EST)': cls.convert_to_est(sub_data['modified_date'])
        }
        custom_fields: Dict[str, Any] = {}
        for source_name, source_value in sub_data['custom_fields'].items():
            target_name = FC.target_field(source_name)
            if target_name:
                custom_fields[target_name] = source_value
        return cls.from_fields(core_fields=core_fields,
                               custom_fields=custom_fields)

    @classmethod
    def convert_to_est(cls, utc_str: str) -> str:
        utc_time = parse(utc_str)
        est_time = utc_time.astimezone(cls.est)
        return est_time.strftime('%Y-%m-%d %H:%M:%S %Z')

    def add_core_fields(self, updates: Dict[str, Any]):
        # Don't update email since that's the key field
        # used to do the match in the first place.
        updates.update((name, self.core_fields[name])
                       for name in self.core_field_list if name != 'Email')

    def all_fields(self) -> Dict[str, Any]:
        return {**self.core_fields, **self.custom_fields}

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
