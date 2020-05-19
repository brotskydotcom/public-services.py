import json
import os
import time
from multiprocessing import Process

import pytest
import requests
import uvicorn

from app.services.main import app


@pytest.fixture(scope='session')
def server():
    if os.getenv("EXTERNAL_SERVER"):
        proc = Process(target=uvicorn.run,
                       args=(app,),
                       kwargs={'host': 'localhost',
                               'port': 8080,
                               'log_level': 'debug'},
                       daemon=True,)
        proc.start()
        time.sleep(0.5)  # give the server time to start
        yield
        proc.terminate()
    else:
        yield


def test_webhook_invalid_form(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open('tests/webhooks.json', 'r') as f:
        test_cases = json.load(f)
    test_case = [test_cases['incorrect form id and person id']]
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(host + endpoint,
                             json=test_case,
                             headers={'Accept': 'application/json'})
    time.sleep(1.0)     # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {'accepted': 0}


def test_webhook_valid_form(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open('tests/webhooks.json', 'r') as f:
        test_cases = json.load(f)
    test_case = [test_cases['incorrect person id']]
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(host + endpoint,
                             json=test_case,
                             headers={'Accept': 'application/json'})
    time.sleep(1.0)     # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {'accepted': 1}


def test_webhook_one_of_two_valid_forms(server):
    """Try to post a webhook for an incorrect form"""
    _fixture = server
    with open('tests/webhooks.json', 'r') as f:
        test_cases = json.load(f)
    test_case = list(test_cases.values())
    host = "http://localhost:8080"
    endpoint = "/action_network/notification"
    response = requests.post(host + endpoint,
                             json=test_case,
                             headers={'Accept': 'application/json'})
    time.sleep(1.0)     # give server chance to process webhook task
    assert response.status_code == 200
    assert response.json() == {'accepted': 1}


def test_webhook_one_valid_delay_retrieve_then_process(server):
    """
    Post a valid form with delayed processing, then retrieve the
    posted submission, then post again without delayed processing,
    then confirm that it has been processed.
    """
    _fixture = server
    with open('tests/webhooks.json', 'r') as f:
        test_cases = json.load(f)
    test_case = test_cases['incorrect person id']
    host = "http://localhost:8080"
    endpoint_p = "/action_network/notification"
    _ = requests.post(host + endpoint_p + '?delay_processing=true',
                      json=[test_case],
                      headers={'Accept': 'application/json'})
    time.sleep(1.0)     # give server chance to process webhook task
    endpoint_g = "/action_network/submissions"
    response = requests.get(host + endpoint_g,
                            headers={'Accept': 'application/json'})
    assert response.status_code == 200
    sub_data = test_case['osdi:submission']
    data = response.json()
    assert len(data) == 1
    assert data[0]['form_name'] == 'gru'
    body = json.loads(data[0]['body'])
    for k, v in body.items():
        assert sub_data[k] == v
    _ = requests.post(host + endpoint_p + '?delay_processing=false',
                      json=[test_case],
                      headers={'Accept': 'application/json'})
    time.sleep(1.0)     # give server chance to process webhook task
    response = requests.get(host + endpoint_g,
                            headers={'Accept': 'application/json'})
    assert response.status_code == 200
    assert response.json() == []
