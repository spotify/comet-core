# Copyright 2018 Spotify AB. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test api_helper module"""

from unittest import mock

import pytest
from flask import Response, g

from comet_core.api import CometApi


@pytest.fixture
def client():  # pylint: disable=missing-param-doc,missing-type-doc
    """Create a Flask test client fixture

    Yields:
        flask.testing.FlaskClient: a Flask testing client
    """
    api = CometApi(hmac_secret="secret")

    @api.register_auth()
    def override():
        if g.test_authorized_for:
            return g.test_authorized_for
        return []

    @api.register_hydrator()
    def test_hydrate(issues):
        return [{"fingerprint": x.fingerprint} for x in issues]

    @api.register_request_hydrator()
    def request_hydrator(request):
        return dict(request.headers)

    app = api.create_app()
    with app.app_context():
        yield app.test_client()


@pytest.fixture
def client_without_request_hydrator():  # pylint: disable=missing-param-doc,missing-type-doc
    """Create a Flask test client fixture

    Yields:
        flask.testing.FlaskClient: a Flask testing client
    """
    api = CometApi(hmac_secret="secret")

    @api.register_auth()
    def override():
        if g.test_authorized_for:
            return g.test_authorized_for
        return []

    @api.register_hydrator()
    def test_hydrate(issues):
        return [{"fingerprint": x.fingerprint} for x in issues]

    app = api.create_app()
    with app.app_context():
        yield app.test_client()


@pytest.fixture
def bad_client():  # pylint: disable=missing-param-doc,missing-type-doc
    """Create a bad Flask test client fixture

    Yields:
        flask.testing.FlaskClient: a Flask testing client
    """
    api = CometApi()
    app = api.create_app()
    with app.app_context():
        yield app.test_client()


def test_hello(client):  # pylint: disable=missing-param-doc,missing-type-doc,redefined-outer-name
    """Test the hello endpoint"""
    res = client.get("/")
    assert res.status == "200 OK"


# pylint: disable=missing-param-doc,missing-type-doc,redefined-outer-name
def test_get_issues(client, test_db):
    """ "Feed all test messages to the get_issues function
    to see that they get rendered correctly"""
    g.user = "testuser"
    with mock.patch("comet_core.api_v0.get_db", return_value=test_db):
        g.test_authorized_for = ["non@existant.com"]

        res = client.get("/v0/issues")
        assert res.status == "200 OK"
        assert not res.json

        g.test_authorized_for = ["test@acme.org"]

        res = client.get("/v0/issues")
        assert res.status == "200 OK"
        assert res.json, res.json

        g.test_authorized_for = Response(status=401)

        res = client.get("/v0/issues")
        assert res.status == "401 UNAUTHORIZED"
        assert not res.json


def test_get_issues_no_hydrator():
    """Test the get_issues endpoint still works while there is no hydrator"""
    app = CometApi().create_app()
    with app.app_context():
        client = app.test_client()
        assert client.get("/v0/issues")


def test_acceptrisk(client):
    """Test the accesprtrisk POST endpoint is fails if
    fingerprint/token empty or not passed"""
    g.test_authorized_for = []
    res = client.post("/v0/acceptrisk", json={"fingerprint": "", "token": ""})
    assert res.json.get("message") == "acceptrisk failed"
    res = client.post("/v0/acceptrisk", json={})
    assert res.json.get("status") == "error"


def test_snooze(client):
    """Test the snooze POST endpoint is working"""
    g.test_authorized_for = []
    res = client.post(
        "/v0/snooze",
        json={
            "fingerprint": "forseti_f0743042e3bbea4a1b163f5accd4c366",
            "token": "7ec8a1ee4308d2d07f71fd5a1c844582cfcca56e915c06fc9518ad5e22c5e718",
        },
    )
    assert res.json.get("status") != "error"


def test_snooze_error(bad_client):
    """Test the snooze endpoint fails when no data is passed"""
    res = bad_client.post("/v0/snooze")
    assert res.json
    assert res.status == "500 INTERNAL SERVER ERROR"


# args for the GET requests
get_request_args = (
    "?fp=forseti_f0743042e3bbea4a1b163f5accd4c366" "&t=7ec8a1ee4308d2d07f71fd5a1c844582cfcca56e915c06fc9518ad5e22c5e718"
)
post_json_data = {
    "fingerprint": "forseti_f0743042e3bbea4a1b163f5accd4c366",
    "token": "7ec8a1ee4308d2d07f71fd5a1c844582cfcca56e915c06fc9518ad5e22c5e718",
}


def test_resolve(client):
    """Test the resolve GET endpoint works"""
    g.test_authorized_for = []
    res = client.get("/v0/resolve" + get_request_args)
    assert "Thanks for resolving the issue!" in res.data.decode("utf-8")


def test_resolve_no_token_passed(client):
    """Test the resolve endpoint fails when
    the token is not passed in the args"""
    g.test_authorized_for = []
    res = client.get("/v0/resolve?fp=splunk_kjsdkjfskdfhskjdf")
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_resolve_post(client):
    """Test the resolve POST endpoint works"""
    g.test_authorized_for = []
    res = client.post("/v0/resolve", json=post_json_data)
    expected_response = '{"msg":"Thanks for resolving the issue!",' '"status":"ok"}'
    assert expected_response in res.data.decode("utf-8")


def test_resolve_error(bad_client):
    """Test the resolve endpoint fails when the args are missing"""
    res = bad_client.get("/v0/resolve")
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_falsepositive(client):
    """Test the falsepositive GET endpoint works"""
    g.test_authorized_for = []
    res = client.get("/v0/falsepositive" + get_request_args)
    assert "Thanks! We’ve marked this as a false positive" in res.data.decode("utf-8")


def test_falsepositive_no_token_passed(client):
    """Test the falsepositive endpoint fails when
    the token is not passed in the args"""
    g.test_authorized_for = []
    res = client.get("/v0/falsepositive?fp=splunk_82998ef6bb3db9dff3dsfdsfsdc")
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_falsepositive_post(client):
    """Test the falsepositive POST endpoint works"""
    g.test_authorized_for = []
    res = client.post("/v0/falsepositive", json=post_json_data)
    expected_response = '{"msg":"Thanks! We\\u2019ve marked this as a false positive",' '"status":"ok"}'
    assert expected_response in res.data.decode("utf-8")


def test_falsepositive_error(bad_client):
    """Test the falsepositive endpoint fails when the args are missing"""
    res = bad_client.get("/v0/falsepositive")
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_v0_root(client):
    """Test the v0 endpoint works"""
    g.test_authorized_for = []
    res = client.get("/v0/")
    assert res.data == b"Comet-API-v0"


def test_dbhealth_check(client):
    """Test the dbcheck endpoint works"""
    res = client.get("/v0/dbcheck")
    assert res.data == b"Comet-API-v0"


def test_dbhealth_check_error(client):
    """Test the dbcheck fails when the get_db function raises exception"""
    with mock.patch("comet_core.api_v0.get_db") as mock_get_db:
        mock_get_db.side_effect = Exception("XOXO")
        res = client.get("/v0/dbcheck")
    assert res.json.get("status") == "error"


def test_acknowledge(client):
    """Test the acknowledge GET endpoint works"""
    g.test_authorized_for = []
    res = client.get("/v0/acknowledge" + get_request_args)
    assert "Thanks for acknowledging!" in res.data.decode("utf-8")


def test_acknowledge_hmac_validation_failed(client):
    """Test the acknowledge endpoint fails when the fingerprint
    doesn't match the token passed"""
    res = client.get(
        "/v0/acknowledge?fp=splunk_82998ef6bb3db9dff3dsfdsfsdc" "&t=97244b15a21f45e002b2e913866ff7545510f9b08dea5241f"
    )
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_acknowledge_post(client):
    """Test the acknowledge POST endpoint works"""
    g.test_authorized_for = []
    res = client.post("/v0/acknowledge", json=post_json_data)
    assert '{"msg":"Thanks for acknowledging!","status":"ok"}' in res.data.decode("utf-8")


def test_acknowledge_error_no_fingerprint_passed(client):
    """Test the acknowledge endpoint fails when fingerprint is missing"""
    g.test_authorized_for = []
    res = client.get("/v0/acknowledge")
    assert res.status == "500 INTERNAL SERVER ERROR"


def test_escalate(client):
    """Test the escalate endpoint works"""
    g.test_authorized_for = []
    res = client.get("/v0/escalate" + get_request_args)
    assert "Thanks! This alert has been escalated" in res.data.decode("utf-8")


def test_escalate_post(client):
    """Test the POST escalate endpoint works"""
    g.test_authorized_for = []
    res = client.post("/v0/escalate", json=post_json_data)
    expected_response = '{"msg":"Thanks! This alert has been escalated.","status":"ok"}'
    assert expected_response in res.data.decode("utf-8")


def test_escalate_post_error(client):
    """Test escalation fails when the fingerprint passed is too short"""
    g.test_authorized_for = []
    res = client.post("/v0/escalate", json={"fingerprint": "splunk"})
    assert "500 INTERNAL SERVER ERROR" in res.status


def test_escalate_error(client):
    """Test escalation fails when when no fingerprint and token are missing"""
    g.test_authorized_for = []
    res = client.get("/v0/escalate")
    assert "500 INTERNAL SERVER ERROR" in res.status


def test_escalate_error_post(client):
    """Test escalation fails when the fingerprint passed contains tags"""
    g.test_authorized_for = []
    res = client.post("/v0/escalate", json={"fingerprint": "splunk_4025ad30<script>"})
    assert "500 INTERNAL SERVER ERROR" in res.status


def test_endpoint_post_request_hydrator(client):
    g.test_authorized_for = []
    res = client.post("/v0/acknowledge", json=post_json_data, headers={"slack_channel": "channel"})
    assert '{"msg":"Thanks for acknowledging!","status":"ok"}' in res.data.decode("utf-8")


def test_endpoint_get_request_hydrator(client):
    g.test_authorized_for = []
    res = client.get("/v0/acknowledge" + get_request_args)
    assert "Thanks for acknowledging!" in res.data.decode("utf-8")


def test_endpoint_post_no_request_hydrator(client_without_request_hydrator):
    """test that even if comet api doesn't have request hydrator
    the response doesn't change"""
    g.test_authorized_for = []
    res = client_without_request_hydrator.post(
        "/v0/acknowledge", json=post_json_data, headers={"slack_channel": "channel"}
    )
    assert '{"msg":"Thanks for acknowledging!","status":"ok"}' in res.data.decode("utf-8")


def test_endpoint_get_interactions(client):
    g.test_authorized_for = ["non@existant.com"]
    res = client.post("/v0/interactions", json=post_json_data)
    assert "[]" in res.data.decode("utf-8")
