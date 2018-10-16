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

import pytest
from flask import g, Response

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


def test_hello(client):  # pylint: disable=missing-param-doc,missing-type-doc,redefined-outer-name
    """Test the hello endpoint"""
    res = client.get('/')
    assert res.status == '200 OK'


# pylint: disable=missing-param-doc,missing-type-doc,redefined-outer-name
def test_get_issues(client, test_db, monkeypatch):
    """"Feed all test messages to the get_issues function to see that they get rendered correctly"""
    g.user = 'testuser'
    monkeypatch.setattr('comet_core.api_v0.get_db', lambda: test_db)
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


def test_snooze(client):
    g.test_authorized_for = []
    res = client.post('/v0/snooze', json={'fingerprint': ''})
    assert res.json


def test_falsepositive(client):
    g.test_authorized_for = []
    res = client.post('/v0/falsepositive', json={'fingerprint': ''})
    assert res.json


def test_v0_root(client):
    g.test_authorized_for = []
    res = client.get('/v0/')
    assert res.data == b'Comet-API-v0'
