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
def pubsub_output_config():
    return {'topic': 'some topic name'}


@pytest.fixture
def get_pubsub_publisher():
    pubsub_output = Mock(return_value=None)
    pubsub_output.publish_message.return_value = None
    return pubsub_output


@pytest.fixture
def app_with_specific_config(pubsub_output_config):
    api = CometApi()

    @api.register_auth()
    def override():
        if g.test_authorized_for:
            return g.test_authorized_for
        return []

    @api.register_hydrator()
    def test_hydrate(issues):
        return [{'fingerprint': x.fingerprint} for x in issues]

    api.set_config('pubsub_output', pubsub_output_config)
    app = api.create_app()
    return app


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
    res = client.post('/v0/falsepositive', json={'fingerprint': ''})
    assert res.json


def test_falsepositive_error(bad_client):
    res = bad_client.post('/v0/falsepositive')
    assert res.json
    assert res.status == '500 INTERNAL SERVER ERROR'


def test_v0_root(client):
    g.test_authorized_for = []
    res = client.get('/v0/')
    assert res.data == b'Comet-API-v0'


def test_dbhealth_check(client):
    with mock.patch('comet_core.api_v0.get_db') as mock_get_db:
        mock_get_db.side_effect = Exception('XOXO')
        res = client.get('/v0/dbcheck')
    assert res.json.get('status') == 'error'

    res = client.get('/v0/dbcheck')
    assert res.data == b'Comet-API-v0'


def test_set_config(app_with_specific_config, pubsub_output_config):
    """Test set_config method works"""
    pubsub_config = app_with_specific_config.config.get('pubsub_output')
    actual_topic = pubsub_config.get('topic')
    expected_topic = pubsub_output_config.get('topic')
    assert expected_topic == actual_topic


def test_acknowledge(client):
    """Test the acknowledge endpoint works"""
    g.test_authorized_for = []
    res = client.post('/v0/acknowledge', json={'fingerprint': ''})
    assert res.json


def test_acknowledge_error_no_fingerprint_passed(client):
    """Test the acknowledge endpoint fails when no fingerprint passes"""
    g.test_authorized_for = []
    res = client.post('/v0/acknowledge')
    assert res.json
    assert res.status == '500 INTERNAL SERVER ERROR'
    assert res.json == {'status': 'error', 'msg': 'acknowledge failed'}


@patch('comet_core.api_v0.get_db')
def test_escalate(get_db_mock, client, get_pubsub_publisher):
    """Test the escalate endpoint works"""
    g.user = 'testuser'
    with mock.patch('comet_core.api_v0.get_pubsub_publisher',
                    return_value=get_pubsub_publisher):
        g.test_authorized_for = ['non@existant.com']

        res = client.post('/v0/escalate', json={'fingerprint': ''})
        assert res.json == {'status': 'ok'}


@patch('comet_core.api_v0.get_db')
def test_escalate_error(get_db_mock, client, get_pubsub_publisher):
    """Test escalation fails when publisher fails to publish message"""
    get_pubsub_publisher.publish_message.side_effect = \
            Exception("publish message Exception")
    with mock.patch('comet_core.api_v0.get_pubsub_publisher',
                    return_value=get_pubsub_publisher):
        g.test_authorized_for = []
        res = client.post('/v0/escalate', json={'fingerprint': ''})
        assert res.json == \
               {'msg': 'escalation real time alerts failed', 'status': 'error'}
