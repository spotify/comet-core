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

"""Global test fixtures"""
import os
from comet_core import Comet

import pytest


@pytest.fixture
def app():
    yield Comet({'alerts_conf_path': 'alerts_conf_path'})


@pytest.fixture
def test_db():
    """Setup a test database fixture

    Yields:
        DataStore: a sqlite backed datastore with all test data
    """
    from comet_core.data_store import DataStore
    from tests.utils import get_all_test_messages

    data_store = DataStore('sqlite://')
    for event in get_all_test_messages(parsed=True):
        data_store.add_record(event.get_record())
    yield data_store
