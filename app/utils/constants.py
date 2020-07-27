#  MIT License
#
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
import os
from dataclasses import dataclass, asdict, field
from typing import ClassVar, Dict, Optional, Any, List, Tuple

import botocore.client
import botocore.session

from app.utils import env, Environment, lookup_env


class MapContext:
    """
    This is a no-instance class.  It maintains context relevant
    to each of the known AN hash types and their connected forms,
    maintains a notion of the "current" hash/form pair being processed,
    and vends the context data to callers.
    """

    an_base: ClassVar[str] = "https://actionnetwork.org/api/v2"

    @dataclass
    class MapData:
        name: str
        an_headers: Dict[str, str]
        at_account_key: str
        at_database_key: str
        at_table_name: str
        at_typecast: bool
        an_core_field_map: Dict[str, str]
        an_forms: Dict[str, str] = field(default_factory=dict)
        an_custom_field_prefixes: List[str] = field(default_factory=list)
        an_custom_field_map: Dict[str, str] = field(default_factory=dict)

    known_maps: ClassVar[Dict[str, MapData]] = {}
    current: ClassVar[Optional[MapData]] = None

    @classmethod
    def get_client_and_target(cls) -> Tuple[botocore.client.BaseClient, str, str]:
        key = os.getenv("AWS_ACCESS_KEY_ID", "")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        region = os.getenv("AWS_REGION_NAME", "")
        bucket = os.getenv("AWS_BUCKET_NAME", "public-services.brotsky.net")
        path = os.getenv("AWS_CONFIG_PATH", "config/mappings.v1.json")
        if not key or not secret or not region or not bucket or not path:
            raise EnvironmentError("Complete AWS connect info not found")
        session = botocore.session.get_session()
        return (
            session.create_client(
                service_name="s3",
                region_name=region,
                aws_access_key_id=key,
                aws_secret_access_key=secret,
            ),
            bucket,
            path,
        )

    @classmethod
    def load_config_from_json(cls, form_data: List[Dict[str, Any]]):
        contexts = {}
        for d in form_data:
            if lookup_env(d.get("env")) is env():
                del d["env"]  # env is not part of the form data
                contexts[d["name"]] = cls.MapData(**d)
        if not (contexts.get("person") and contexts.get("donation")):
            raise ValueError(f"Contexts must include 'person' and 'donation'")
        cls.known_maps = contexts
        print(f"Loaded {len(contexts)} contexts in {env().name} environment.")

    @classmethod
    def load_config_from_aws(cls):
        print("Loading form context from AWS...")
        client, bucket, key = cls.get_client_and_target()
        response = client.get_object(Bucket=bucket, Key=key)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise RuntimeError(f"Failure to download configuration: {response}")
        form_data = json.load(response["Body"])
        response["Body"].close()
        cls.load_config_from_json(form_data)

    @classmethod
    def load_config_locally(cls, config_path: str):
        print(f"Loading config from '{config_path}'...")
        with open(config_path, "r", encoding="utf-8") as fp:
            form_data = json.load(fp)
        cls.load_config_from_json(form_data)

    @classmethod
    def put_config_to_aws(cls):
        content = [asdict(form_data) for form_data in cls.known_maps.values()]
        if not content:
            raise ValueError("Saving an empty config not allowed")
        body = (json.dumps(content, indent=2) + "\n").encode("utf-8")
        client, bucket, key = cls.get_client_and_target()
        response = client.put_object(
            Bucket=bucket,
            Key=key,
            ContentEncoding="utf-8",
            ContentType="application/json",
            Body=body,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] >= 400:
            raise RuntimeError(f"Failed to write config: {response}")

    @classmethod
    def get(cls) -> str:
        """Get the name of the current context"""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return cls.current.name

    @classmethod
    def set(cls, name: str):
        """Set the current context by name or form name."""
        cls.current = cls.known_maps.get(name)
        if not cls.current:
            raise ValueError(f"Not a known context: {name}")

    @classmethod
    def lookup_form_url(cls, url: str) -> Optional[str]:
        """
        Look up a matching context from a form URL,
        returning None if there is no match.
        """
        base = cls.an_base + "/forms/"
        for _, data in cls.known_maps.items():
            for form_name, form_id in data.an_forms.items():
                if url.startswith(base + form_id):
                    return form_name
        return None

    @classmethod
    def core_field_map(cls) -> Dict[str, str]:
        """
        Returns the AT -> AN field name map for the core fields.
        Uses the current context to find the field map.
        """
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return cls.current.an_core_field_map

    @classmethod
    def target_custom_field(cls, field_name: str) -> Optional[str]:
        """
        Returns the AT target custom field name for an AN field name, if any.
        Uses the current context to find the field map.
        """
        if not cls.current:
            raise ValueError("You must set the context before using it")
        # add fields with one of the known custom field prefixes
        for prefix in cls.current.an_custom_field_prefixes:
            if field_name.startswith(prefix):
                return field_name
        # otherwise look the field up
        return cls.current.an_custom_field_map.get(field_name)

    @classmethod
    def an_key_field(cls) -> str:
        """Returns the AN-side key field"""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return next(iter(cls.current.an_core_field_map.keys()))

    @classmethod
    def at_key_field(cls) -> str:
        """Returns the AT-side key field"""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return next(iter(cls.current.an_core_field_map.values()))

    @classmethod
    def an_submissions_url(cls, form_name: str) -> Optional[str]:
        """Return the url for AN submissions on the given form."""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        base = cls.an_base + "/forms/"
        form_id = cls.current.an_forms[form_name]
        return base + form_id + "/submissions/"

    @classmethod
    def an_people_url(cls) -> str:
        """Return the url for AN people records."""
        if not cls.current:
            raise ValueError("You must set the context before using it")
        return cls.an_base + "/people"

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
        return (
            cls.current.at_account_key,
            cls.current.at_database_key,
            cls.current.at_table_name,
            cls.current.at_typecast,
        )

    @classmethod
    def initialize(cls):
        if not cls.known_maps:
            form_path = env() is Environment.DEV and os.getenv("FORM_PATH")
            if form_path:
                cls.load_config_locally(form_path)
            else:
                cls.load_config_from_aws()


# initialize on load
MapContext.initialize()
