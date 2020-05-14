from __future__ import annotations

import os
from typing import ClassVar, Dict, Optional

from dataclasses import dataclass


class FormContext:
    """
    This is a no-instance class.  It maintains context relevant
    to each of the known AN forms, maintains a notion of the
    "current" form type being processed, and vends the
    context data to callers.
    """

    an_base: ClassVar[str] = 'https://actionnetwork.org/api/v2'

    @dataclass
    class FormData:
        name: str
        an_headers: Dict[str, str]
        an_form_id: str
        an_custom_field_map: Dict[str, str]
        at_account_key: str
        at_database_key: str
        at_table_name: str
        at_typecast: bool

    known_forms: ClassVar[Dict[str, FormData]] = {
        'gru': FormData(
            name='gru',
            an_headers={'OSDI-API-Token': os.getenv('AN_API_TOKEN', 'none')},
            an_form_id='d7a73085-2395-4de7-8f9b-147c8fcc1ab2',
            an_custom_field_map={},
            at_account_key=os.getenv('AT_GRU_ACCOUNT_KEY', 'none'),
            at_database_key='appWMDeTEzqBxrNp2',
            at_table_name='Applications',
            at_typecast=False,
        ),
        # 'stv': FormData(
        #     name='stv',
        #     an_headers={'OSDI-API-Token': os.getenv('AN_API_TOKEN', 'none')},
        #     an_form_id='d7a73085-2395-4de7-8f9b-147c8fcc1ab2',
        #     an_custom_field_map={},
        #     at_account_key=os.getenv('AT_STV_ACCOUNT_KEY', 'none'),
        #     at_database_key='unknown',
        #     at_table_name='unknown',
        #     at_typecast=True,
        # ),
    }

    current: ClassVar[Optional[FormData]] = None

    @classmethod
    def set(cls, name: str):
        """Set the current context by name."""
        cls.current = cls.known_forms.get(name)
        if not cls.current:
            raise ValueError(f"Not a known form: {name}")
        print(f"Form context set to {name}.")

    @classmethod
    def get(cls) -> str:
        """Get the name of the current context"""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return cls.current.name

    @classmethod
    def lookup(cls, url: str) -> Optional[str]:
        """
        Lookup the name of the form in the URL.
        Returns none if it's not a known form URL.
        """
        base = 'https://actionnetwork.org/api/v2/forms/'
        for name, data in cls.known_forms.items():
            if url.startswith(base + data.an_form_id):
                return name
        return None

    @classmethod
    def target_field(cls, field_name: str) -> Optional[str]:
        """
        Returns the AT target name for an AN field name, if any.
        Uses the current context to find the field map.
        """
        if not cls.current:
            raise ValueError("You must set the context before using it")
        # GRU fields carry over as is
        if cls.current.name == 'gru' and field_name.startswith('gru_'):
            return field_name
        # otherwise look the field up
        return cls.current.an_custom_field_map.get(field_name)

    @classmethod
    def an_submissions_url(cls) -> str:
        """Return the url for AN submissions on the current form."""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        base = 'https://actionnetwork.org/api/v2/forms/'
        return base + cls.current.an_form_id + '/submissions/'

    @classmethod
    def an_headers(cls) -> Dict:
        """Return the headers to use on AN calls."""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return cls.current.an_headers

    @classmethod
    def at_connect_info(cls) -> (str, str, str):
        """Return the account key, the database key, and the table name."""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return (cls.current.at_account_key,
                cls.current.at_database_key,
                cls.current.at_table_name,
                cls.current.at_typecast)
