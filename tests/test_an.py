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
from multiprocessing import Process

import pytest
import requests
import uvicorn

from app.services.main import app


@pytest.fixture(scope="session")
def server():
    if os.getenv("EXTERNAL_SERVER"):
        proc = Process(
            target=uvicorn.run,
            args=(app,),
            kwargs={"host": "localhost", "port": 8080, "log_level": "debug"},
            daemon=True,
        )
        proc.start()
        time.sleep(0.5)  # give the server time to start
        yield
        proc.terminate()
    else:
        yield


def test_webhook_invalid_form(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = [test_cases["incorrect form id and person id"]]
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    assert response.status_code == 200
    assert response.json() == {"accepted": 0}


def test_webhook_valid_form(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = [test_cases["incorrect person id"]]
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    time.sleep(1.0)  # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {"accepted": 1}


def test_webhook_one_of_two_valid_forms(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = list(test_cases.values())
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(
        host + endpoint, json=test_case, headers={"Accept": "application/json"}
    )
    time.sleep(1.0)  # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {"accepted": 1}


def test_webhook_one_valid_delay_retrieve_then_process(server):
    """
    Post a valid form with delayed processing, then retrieve the
    posted submission, then post again without delayed processing,
    then confirm that it has been processed.
    """
    _fixture = server
    with open("tests/webhooks.json", "r") as f:
        test_cases = json.load(f)
    test_case = test_cases["incorrect person id"]
    host = "http://localhost:8080"
    endpoint_p = "/action_network/notification"
    _ = requests.post(
        host + endpoint_p + "?delay_processing=true",
        json=[test_case],
        headers={"Accept": "application/json"},
    )
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
    time.sleep(1.0)  # give server chance to process webhook task
    response = requests.get(host + endpoint_g, headers={"Accept": "application/json"})
    assert response.status_code == 200
    assert response.json() == []
