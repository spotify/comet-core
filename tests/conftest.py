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
from datetime import datetime
from typing import List

import pytest

from comet_core import Comet
from comet_core.app import EventContainer
from comet_core.data_store import DataStore
from comet_core.model import EventRecord

# pylint: disable=redefined-outer-name


@pytest.fixture
def app() -> Comet:
    """Returns a Comet app."""
    yield Comet()


@pytest.fixture
def messages() -> List[EventContainer]:
    """Get all test messages and their filenames as an iterator.

    Returns:
        EventContainer: some test event
    """
    event = EventContainer("test", {})
    event.set_owner("test@acme.org")
    event.set_fingerprint("test")
    return [event]


@pytest.fixture
def data_store() -> DataStore:
    """Creates a SQLite backed datastore."""
    return DataStore("sqlite://")


@pytest.fixture
def test_db(messages, data_store) -> DataStore:
    """Setup a test database fixture

    Yields:
        DataStore: a sqlite backed datastore with all test data
    """

    for event in messages:
        data_store.add_record(event.get_record())

    yield data_store


@pytest.fixture
def data_store_with_test_events(data_store) -> DataStore:
    """Creates a populated data store."""
    one = EventRecord(received_at=datetime(2018, 7, 7, 9, 0, 0), source_type="datastoretest", owner="a", data={})
    one.fingerprint = "f1"
    two = EventRecord(received_at=datetime(2018, 7, 7, 9, 30, 0), source_type="datastoretest", owner="a", data={})
    two.fingerprint = "f2"
    three = EventRecord(
        received_at=datetime(2018, 7, 7, 9, 0, 0),
        source_type="datastoretest2",  # Note that this is another source type!
        owner="b",
        data={},
    )
    three.fingerprint = "f3"

    data_store.add_record(one)
    data_store.add_record(two)
    data_store.add_record(three)

    yield data_store
