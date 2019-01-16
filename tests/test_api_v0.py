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
from unittest.mock import Mock, patch

import pytest
from flask import g, Response
from unittest import mock

from comet_core.api import CometApi


@pytest.fixture
def client():  # pylint: disable=missing-param-doc,missing-type-doc
    """Create a Flask test client fixture

    Yields:
        flask.testing.FlaskClient: a Flask testing client
    """
    api = CometApi()

    @api.register_auth()
    def override():
        if g.test_authorized_for:
            return g.test_authorized_for
        return []

    @api.register_hydrator()
    def test_hydrate(issues):
        return [{'fingerprint': x.fingerprint} for x in issues]

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
    res = client.get('/')
    assert res.status == '200 OK'


# pylint: disable=missing-param-doc,missing-type-doc,redefined-outer-name
def test_get_issues(client, test_db):
    """"Feed all test messages to the get_issues function to see that they get rendered correctly"""
    g.user = 'testuser'
    with mock.patch('comet_core.api_v0.get_db', return_value=test_db):
        g.test_authorized_for = ['non@existant.com']

        res = client.get('/v0/issues')
        assert res.status == '200 OK'
        assert not res.json

        g.test_authorized_for = ['test@acme.org']

        res = client.get('/v0/issues')
        assert res.status == '200 OK'
        assert res.json, res.json

        g.test_authorized_for = Response(status=401)

        res = client.get('/v0/issues')
        assert res.status == '401 UNAUTHORIZED'
        assert not res.json


def test_get_issues_no_hydrator():
    app = CometApi().create_app()
    with app.app_context():
        client = app.test_client()
        assert client.get('/v0/issues')


def test_acceptrisk(client):
    g.test_authorized_for = []
    res = client.post('/v0/acceptrisk', json={'fingerprint': ''})
    assert res.json
    res = client.post('/v0/acceptrisk', json={})
    assert res.json.get('status') == 'error'


def test_snooze(client):
    g.test_authorized_for = []
    res = client.post('/v0/snooze', json={'fingerprint': ''})
    assert res.json


def test_snooze_error(bad_client):
    res = bad_client.post('/v0/snooze')
    assert res.json
    assert res.status == '500 INTERNAL SERVER ERROR'


def test_falsepositive(client):
    g.test_authorized_for = []
    res = client.get('/v0/falsepositive/splunk_4025ad523c2a94e5a13b1c8aef8c5730')
    assert 'Thanks! We’ve marked this as a false positive' in res.data.decode('utf-8')


def test_falsepositive_post(client):
    g.test_authorized_for = []
    res = client.post('/v0/falsepositive',
                      json={'fingerprint': 'splunk_4025ad523c2a94e5a13b1c8aef8c5730'})
    assert '{"msg":"Thanks! We\\u2019ve marked this as a false positive","status":"ok"}' \
           in res.data.decode('utf-8')


def test_falsepositive_error(bad_client):
    res = bad_client.get('/v0/falsepositive')
    assert res.status == '405 METHOD NOT ALLOWED'


def test_v0_root(client):
    g.test_authorized_for = []
    res = client.get('/v0/')
    assert res.data == b'Comet-API-v0'

def test_dbhealth_check(client):
    res = client.get('/v0/dbcheck')
    assert res.data == b'Comet-API-v0'


def test_dbhealth_check_error(client):
    with mock.patch('comet_core.api_v0.get_db') as mock_get_db:
        mock_get_db.side_effect = Exception('XOXO')
        res = client.get('/v0/dbcheck')
    assert res.json.get('status') == 'error'


def test_acknowledge(client):
    """Test the acknowledge endpoint works"""
    g.test_authorized_for = []
    res = client.get('/v0/acknowledge/splunk_4025ad523c2a94e5a13b1c8aef8c5730')
    assert 'Thanks for acknowledging!' in res.data.decode('utf-8')


def test_acknowledge_post(client):
    g.test_authorized_for = []
    res = client.post('/v0/acknowledge',
                      json={'fingerprint': 'splunk_4025ad523c2a94e5a13b1c8aef8c5730'})
    assert '{"msg":"Thanks for acknowledging!","status":"ok"}' \
           in res.data.decode('utf-8')


def test_acknowledge_error_no_fingerprint_passed(client):
    """Test the acknowledge endpoint fails when no fingerprint passes"""
    g.test_authorized_for = []
    res = client.get('/v0/acknowledge')
    assert res.status == '405 METHOD NOT ALLOWED'


def test_escalate(client):
    """Test the escalate endpoint works"""
    g.test_authorized_for = []
    res = client.get('/v0/escalate/splunk_4025ad523c2a94e5a13b1c8aef8c5730')
    assert 'Thanks! This alert has been escalated' in res.data.decode('utf-8')


def test_escalate_post(client):
    g.test_authorized_for = []
    res = client.post('/v0/escalate',
                      json={'fingerprint': 'splunk_4025ad523c2a94e5a13b1c8aef8c5730'})
    assert '{"msg":"Thanks! This alert has been escalated.","status":"ok"}' \
           in res.data.decode('utf-8')


def test_escalate_error(client):
    """Test escalation fails when when no fingerprint passes"""
    g.test_authorized_for = []
    res = client.get('/v0/escalate')
    assert '405 METHOD NOT ALLOWED' in res.status


def test_escalate_error_post(client):
    """Test escalation fails when when no fingerprint passes"""
    g.test_authorized_for = []
    res = client.post('/v0/escalate',
                     json={
                         'fingerprint': 'splunk_4025ad30<script>'})
    assert '500 INTERNAL SERVER ERROR' in res.status