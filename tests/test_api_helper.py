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

"""Tests for api_helper"""
from unittest import mock

import pytest

from comet_core.api import CometApi
from comet_core.api_helper import assert_valid_token, get_db, hydrate_open_issues, hydrate_with_request_headers
from comet_core.fingerprint import fingerprint_hmac


@pytest.fixture
def app_context():
    api = CometApi()
    app = api.create_app()

    yield app.app_context()


@pytest.fixture
def app_context_with_request_hydrator():
    api = CometApi()

    @api.register_request_hydrator()
    def request_hydrator(request):
        return request

    app = api.create_app()
    yield app.app_context()


def test_get_db(app_context):
    with app_context:
        assert get_db()


def test_no_hydrator():
    api = CometApi()
    with api.create_app().app_context():
        assert not hydrate_open_issues([])


def test_no_request_hydrator():
    api = CometApi()
    request_mock = mock.Mock()
    with api.create_app().app_context():
        assert not hydrate_with_request_headers(request_mock)


def test_request_hydrator(app_context_with_request_hydrator):
    request_mock = mock.Mock()
    with app_context_with_request_hydrator:
        assert hydrate_with_request_headers(request_mock) == request_mock


def test_assert_valid_token():
    fp = "test_fingerprint"
    test_hmac_secret = "secret"
    token = fingerprint_hmac(fp, test_hmac_secret)
    api = CometApi(hmac_secret=test_hmac_secret)
    with api.create_app().app_context():
        assert_valid_token(fp, token)
