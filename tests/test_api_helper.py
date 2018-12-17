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
import pytest

from comet_core.api import CometApi
from comet_core.api_helper import get_db, hydrate_open_issues, \
    get_pubsub_publisher


@pytest.fixture
def pubsub_output_config():
    return {'topic': 'some topic name'}


@pytest.fixture
def app_context(pubsub_output_config):
    api = CometApi()
    api.set_config('pubsub_output', pubsub_output_config)
    app = api.create_app()

    yield app.app_context()


def test_get_db(app_context):
    with app_context:
        assert get_db()


def test_no_hydrator():
    api = CometApi()
    with api.create_app().app_context():
        assert not hydrate_open_issues([])


def test_get_pubsub_publisher(app_context):
    with app_context:
        assert get_pubsub_publisher()