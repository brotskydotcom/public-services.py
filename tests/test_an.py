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
import json
import os
import time
from multiprocessing import Process, set_start_method

import pytest
import requests
import uvicorn

from app.utils import env, Environment
from app.services.main import app
from app.db import redis_db


@pytest.fixture(scope="session")
def server() -> str:
    if os.getenv("EXTERNAL_SERVER"):
        set_start_method("spawn", force=True)
        # This cannot work because FastAPI has local functions
        # that can't be pickled.
        proc = Process(
            target=uvicorn.run,
            args=(app,),
            kwargs={"host": "localhost", "port": 8080, "log_level": "debug"},
            daemon=True,
        )
        proc.start()
        time.sleep(0.5)  # give the server time to start
        response = requests.get("http://localhost:8080/docs")
        if response.status_code != 200:
            raise RuntimeError("Tests require a DEV server at localhost:8080")
        yield "http://localhost:8080"
        proc.terminate()
    else:
        response = requests.get("http://localhost:8080/docs")
        if response.status_code != 200:
            raise RuntimeError("Tests require a DEV server at localhost:8080")
        yield "http://localhost:8080"


@pytest.fixture(scope="session")
def database() -> redis_db.RedisDatabase:
    if env() is not Environment.DEV:
        raise RuntimeError("Tests must be run in a DEV environment")
    redis_db.redis.connect_sync()
    yield redis_db.redis
    redis_db.redis.db.delete(redis_db.redis.get_key("Successfully processed"))
    redis_db.redis.db.delete(redis_db.redis.get_key("Failed to process"))
    redis_db.redis.close_sync()


def test_webhook_bad_form(server, database):
    """
    Post a webhook for an incorrect form,
    make sure it is not accepted.
    """
    host: str = server
    redis = database
    redis.db.delete(redis.get_key("Successfully processed"))
    redis.db.delete(redis.get_key("Failed to process"))
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = [test_cases["incorrect form id and person id"]]
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    time.sleep(2.0)  # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {"accepted": 0}
    assert redis.db.llen(redis.get_key("Successfully processed")) == 0
    assert redis.db.llen(redis.get_key("Failed to process")) == 0


def test_case_bad_person(server, database):
    """
    Post a webhook for a good form but invalid person,
    make sure it is accepted.
    """
    host = server
    redis = database
    redis.db.delete(redis.get_key("Successfully processed"))
    redis.db.delete(redis.get_key("Failed to process"))
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = [test_cases["incorrect person id"]]
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    time.sleep(2.0)  # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {"accepted": 1}
    assert redis.db.llen(redis.get_key("Successfully processed")) == 1
    assert redis.db.llen(redis.get_key("Failed to process")) == 0


def test_webhook_bad_form_and_bad_person(server, database):
    """
    Post a webhook with two entries: one bad form, one bad person,
    make sure only the bad person is accepted.
    """
    host = server
    redis = database
    redis.db.delete(redis.get_key("Successfully processed"))
    redis.db.delete(redis.get_key("Failed to process"))
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = [
        test_cases["incorrect form id and person id"],
        test_cases["incorrect person id"],
    ]
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    time.sleep(2.0)  # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {"accepted": 1}
    assert redis.db.llen(redis.get_key("Successfully processed")) == 1
    assert redis.db.llen(redis.get_key("Failed to process")) == 0


def test_webhook_bad_person_noprocess_retrieve_process_retrieve(server, database):
    """
    Post a bad person form with delayed processing, then retrieve the
    posted submission, then post again without delayed processing,
    then confirm that it has been processed.
    """
    host = server
    redis = database
    redis.db.delete(redis.get_key("Successfully processed"))
    redis.db.delete(redis.get_key("Failed to process"))
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = test_cases["incorrect person id"]
    endpoint_p = "/action_network/notification"
    _ = requests.post(
        host + endpoint_p + "?delay_processing=true",
        json=[test_case],
        headers={"Accept": "application/json"},
    )
    time.sleep(2.0)  # give server chance to process webhook task
    assert redis.db.llen(redis.get_key("Successfully processed")) == 0
    assert redis.db.llen(redis.get_key("Failed to process")) == 0
    endpoint_g = "/action_network/submissions"
    response = requests.get(host + endpoint_g, headers={"Accept": "application/json"})
    assert response.status_code == 200
    sub_data = test_case["osdi:submission"]
    data = response.json()
    assert len(data) == 1
    assert data[0]["form_name"] == "gru"
    body = data[0]["body"]
    for k, v in body.items():
        assert sub_data[k] == v
    _ = requests.post(
        host + endpoint_p + "?delay_processing=false",
        json=[test_case],
        headers={"Accept": "application/json"},
    )
    time.sleep(2.0)  # give server chance to process webhook task
    response = requests.get(host + endpoint_g, headers={"Accept": "application/json"})
    assert response.status_code == 200
    assert response.json() == []
    assert redis.db.llen(redis.get_key("Successfully processed")) == 2
    assert redis.db.llen(redis.get_key("Failed to process")) == 0
