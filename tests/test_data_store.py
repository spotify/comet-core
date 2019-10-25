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

# pylint: disable=invalid-name,missing-docstring,redefined-outer-name
"""Tests the event_parser module"""

from datetime import datetime, timedelta

import pytest

from freezegun import freeze_time

import comet_core.data_store
from comet_core.data_store import remove_duplicate_events
from comet_core.model import EventRecord, IgnoreFingerprintRecord
from tests.utils import get_all_test_messages


@pytest.fixture
# pylint: disable=missing-yield-doc,missing-yield-type-doc
def data_store_with_test_events():
    data_store = comet_core.data_store.DataStore("sqlite://")

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


def test_data_store():
    data_store = comet_core.data_store.DataStore("sqlite://")
    for event in get_all_test_messages():
        data_store.add_record(event.get_record())


def test_date_sorting():
    data_store = comet_core.data_store.DataStore("sqlite://")

    old = EventRecord(
        received_at=datetime(2018, 2, 19, 0, 0, 11), source_type="datastoretest", data={"fingerprint": "same"}
    )
    new = EventRecord(
        received_at=datetime(2018, 2, 20, 0, 0, 11), source_type="datastoretest", data={"fingerprint": "same"}
    )
    old.fingerprint = "same"
    new.fingerprint = "same"

    data_store.add_record(old)
    data_store.add_record(new)

    oldest = data_store.get_oldest_event_with_fingerprint("same")
    latest = data_store.get_latest_event_with_fingerprint("same")

    assert oldest.received_at == old.received_at
    assert latest.received_at == new.received_at


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_get_unprocessed_events_batch_will_wait(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(days=1024 * 365), timedelta(days=1024 * 365), "datastoretest"
    )
    assert val == []


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring
def test_get_unprocessed_events(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=1), timedelta(minutes=1), "datastoretest"
    )
    assert len(val) == 2


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_get_unprocessed_events_max_wait(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=600), timedelta(minutes=60), "datastoretest"
    )
    assert val == []

    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=600), timedelta(minutes=59), "datastoretest"
    )
    assert len(val) == 2


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_get_unprocessed_events_wait_for_more(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=30), timedelta(minutes=120), "datastoretest"
    )
    assert val == []

    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=29), timedelta(minutes=120), "datastoretest"
    )
    assert len(val) == 2


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_update_sent_at_timestamp_to_now(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=1), timedelta(minutes=1), "datastoretest"
    )
    assert len(val) == 2
    data_store_with_test_events.update_sent_at_timestamp_to_now(val)
    record = data_store_with_test_events.get_latest_event_with_fingerprint(val[0].fingerprint)
    assert record.sent_at is not None
    assert isinstance(record.sent_at, datetime)


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_update_event_escalation_at_to_now(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=1), timedelta(minutes=1), "datastoretest"
    )
    assert len(val) == 2
    data_store_with_test_events.update_event_escalated_at_to_now(val)
    record = data_store_with_test_events.get_latest_event_with_fingerprint(val[0].fingerprint)
    assert record.escalated_at is not None
    assert isinstance(record.escalated_at, datetime)


@freeze_time("2018-07-07 10:00:00")
# pylint: disable=missing-docstring, invalid-name
def test_update_processed_at_timestamp_to_now(data_store_with_test_events):
    val = data_store_with_test_events.get_unprocessed_events_batch(
        timedelta(minutes=1), timedelta(minutes=1), "datastoretest"
    )
    assert len(val) == 2
    data_store_with_test_events.update_processed_at_timestamp_to_now(val)
    record = data_store_with_test_events.get_latest_event_with_fingerprint(val[0].fingerprint)
    assert record.processed_at is not None
    assert isinstance(record.processed_at, datetime)


def test_check_any_issue_needs_reminder():
    data_store = comet_core.data_store.DataStore("sqlite://")

    test_fingerprint1 = "f1"
    test_fingerprint2 = "f2"
    test_fingerprint3 = "f3"

    one_a = EventRecord(sent_at=datetime.utcnow() - timedelta(days=9), source_type="datastoretest")
    one_a.fingerprint = test_fingerprint1
    one_b = EventRecord(sent_at=datetime.utcnow() - timedelta(days=3), source_type="datastoretest")
    one_b.fingerprint = test_fingerprint1

    two_a = EventRecord(sent_at=datetime.utcnow() - timedelta(days=10), source_type="datastoretest")
    two_a.fingerprint = test_fingerprint2
    two_b = EventRecord(sent_at=datetime.utcnow() - timedelta(days=8), source_type="datastoretest")
    two_b.fingerprint = test_fingerprint2

    two_c = EventRecord(source_type="datastoretest")  # sent_at NULL
    two_c.fingerprint = test_fingerprint2

    three_a = EventRecord(source_type="datastoretest")  # sent_at NULL
    three_a.fingerprint = test_fingerprint3

    data_store.add_record(one_a)
    data_store.add_record(two_a)
    data_store.add_record(two_b)
    data_store.add_record(two_c)
    data_store.add_record(three_a)

    # issue \ time --->
    #   1 --------a------|-------------->
    #   2 ----a-------b--|--------------> (2c sent_at == NULL)
    #   3 ---------------|--------------> (3a sent_at == NULL)
    #                    ^
    #                 -7days
    assert data_store.check_any_issue_needs_reminder(timedelta(days=7), [one_a, two_a, three_a])

    data_store.add_record(one_b)

    # issue \ time --->
    #   1 --------a------|-----b-------->
    #   2 ----a-------b--|--------------> (2c sent_at == NULL)
    #   3 ---------------|--------------> (3a sent_at == NULL)
    #                    ^
    #                 -7days
    assert not data_store.check_any_issue_needs_reminder(timedelta(days=7), [one_a, two_a, three_a])


def test_check_needs_escalation():
    data_store = comet_core.data_store.DataStore("sqlite://")

    test_fingerprint1 = "f1"
    test_fingerprint2 = "f2"
    test_fingerprint3 = "f3"

    one = EventRecord(received_at=datetime(2018, 2, 19, 0, 0, 11), source_type="datastoretest", owner="a", data={})
    one.fingerprint = test_fingerprint1

    two = EventRecord(received_at=datetime.utcnow(), source_type="datastoretest", owner="a", data={})
    two.fingerprint = test_fingerprint1

    three = EventRecord(
        received_at=datetime.utcnow() - timedelta(hours=23), source_type="datastoretest", owner="a", data={}
    )
    three.fingerprint = test_fingerprint2

    four = EventRecord(received_at=datetime.utcnow(), source_type="datastoretest", owner="a", data={})
    four.fingerprint = test_fingerprint2

    five = EventRecord(received_at=datetime.utcnow(), source_type="datastoretest", owner="a", data={})
    four.fingerprint = test_fingerprint3

    data_store.add_record(one)
    data_store.add_record(two)
    data_store.add_record(three)
    data_store.add_record(four)
    data_store.add_record(five)

    three.fingerprint = test_fingerprint2

    assert data_store.check_needs_escalation(timedelta(days=1), two)
    assert not data_store.check_needs_escalation(timedelta(days=1), four)
    assert not data_store.check_needs_escalation(timedelta(days=1), five)


def test_check_acceptedrisk_event_fingerprint():
    data_store = comet_core.data_store.DataStore("sqlite://")

    test_fingerprint1 = "f1"

    assert not data_store.fingerprint_is_ignored(test_fingerprint1)

    data_store.ignore_event_fingerprint(test_fingerprint1, IgnoreFingerprintRecord.ACCEPT_RISK)
    assert data_store.fingerprint_is_ignored(test_fingerprint1)


def test_check_snoozed_event_fingerprint():
    data_store = comet_core.data_store.DataStore("sqlite://")

    test_fingerprint1 = "f1"
    test_fingerprint2 = "f2"

    assert not data_store.fingerprint_is_ignored(test_fingerprint1)

    data_store.ignore_event_fingerprint(
        test_fingerprint1, ignore_type=IgnoreFingerprintRecord.SNOOZE, expires_at=datetime.utcnow() + timedelta(days=30)
    )
    assert data_store.fingerprint_is_ignored(test_fingerprint1)

    test_snooze_record = IgnoreFingerprintRecord(
        fingerprint=test_fingerprint2,
        ignore_type=IgnoreFingerprintRecord.SNOOZE,
        reported_at=datetime(2018, 2, 22, 0, 0, 11),
        expires_at=datetime(2018, 2, 23, 0, 0, 11),
    )
    data_store.add_record(test_snooze_record)
    assert not data_store.fingerprint_is_ignored(test_fingerprint2)


def test_may_send_escalation():
    data_store = comet_core.data_store.DataStore("sqlite://")

    data_store.add_record(EventRecord(source_type="type1", escalated_at=None))

    assert data_store.may_send_escalation("type1", timedelta(days=7))

    data_store.add_record(EventRecord(source_type="type1", escalated_at=datetime.utcnow() - timedelta(days=8)))

    assert data_store.may_send_escalation("type1", timedelta(days=7))

    data_store.add_record(EventRecord(source_type="type1", escalated_at=datetime.utcnow() - timedelta(days=6)))

    assert not data_store.may_send_escalation("type1", timedelta(days=7))

    data_store.add_record(EventRecord(source_type="type2", escalated_at=None))

    assert data_store.may_send_escalation("type2", timedelta(days=7))


def test_check_if_previously_escalated():
    data_store = comet_core.data_store.DataStore("sqlite://")

    one = EventRecord(source_type="test_type", fingerprint="f1", escalated_at=None)
    data_store.add_record(one)

    two = EventRecord(source_type="test_type", fingerprint="f2", escalated_at=datetime.utcnow() - timedelta(days=1))
    data_store.add_record(two)

    assert not data_store.check_if_previously_escalated(one)
    assert data_store.check_if_previously_escalated(two)

    data_store.add_record(
        EventRecord(source_type="test_type", fingerprint="f1", escalated_at=datetime.utcnow() - timedelta(days=1))
    )

    assert data_store.check_if_previously_escalated(one)


def test_get_open_issues(*_):
    data_store = comet_core.data_store.DataStore("sqlite://")

    one = EventRecord(source_type="test_type", fingerprint="f1", received_at=datetime.utcnow(), owner="test")
    data_store.add_record(one)

    two = EventRecord(
        source_type="test_type", fingerprint="f2", received_at=datetime.utcnow() - timedelta(days=0.9), owner="test"
    )
    data_store.add_record(two)

    three = EventRecord(
        source_type="test_type", fingerprint="f3", received_at=datetime.utcnow() - timedelta(days=2), owner="test"
    )
    data_store.add_record(three)

    four = EventRecord(source_type="test_type", fingerprint="f4", received_at=datetime.utcnow(), owner="not_test")
    data_store.add_record(four)

    five = EventRecord(
        source_type="test_type", fingerprint="f5", received_at=datetime.utcnow() - timedelta(days=1.5), owner="not_test"
    )
    data_store.add_record(five)

    six = EventRecord(
        source_type="test_type", fingerprint="f2", received_at=datetime.utcnow() - timedelta(days=0.2), owner="test"
    )
    data_store.add_record(six)

    open_issues = data_store.get_open_issues(["test"])
    assert len(open_issues) == 2

    open_issues = data_store.get_open_issues(["test", "not_test"])
    assert len(open_issues) == 3

    test_snooze_record = IgnoreFingerprintRecord(
        fingerprint="f1",
        ignore_type=IgnoreFingerprintRecord.SNOOZE,
        reported_at=datetime(2018, 2, 22, 0, 0, 11),
        expires_at=datetime(3000, 2, 23, 0, 0, 11),
    )

    data_store.add_record(test_snooze_record)

    open_issues = data_store.get_open_issues(["test"])
    assert len(open_issues) == 1


def test_check_if_new(*_):
    data_store = comet_core.data_store.DataStore("sqlite://")

    timestamp = datetime.utcnow()
    one_a = EventRecord(source_type="test_type", fingerprint="f1", received_at=timestamp)

    timestamp = datetime.utcnow() - timedelta(days=1)
    one_b = EventRecord(source_type="test_type", fingerprint="f1", received_at=timestamp, processed_at=timestamp)

    timestamp = datetime.utcnow() - timedelta(days=8)
    one_c = EventRecord(source_type="test_type", fingerprint="f1", received_at=timestamp, processed_at=timestamp)

    assert data_store.check_if_new("f1", timedelta(days=7))

    data_store.add_record(one_a)  # processed_at is None for that one, so f1 should still be considered new
    assert data_store.check_if_new("f1", timedelta(days=7))

    data_store.add_record(one_c)  # older than 7 days, so f1 should still be considered new (kind of "new again")
    assert data_store.check_if_new("f1", timedelta(days=7))

    data_store.add_record(one_b)  # more recent than 7 days ago and processed_at is set, so f1 should not be flagged new
    assert not data_store.check_if_new("f1", timedelta(days=7))


def test_remove_duplicate_events():
    """Test the remove_duplicate_events function"""
    one = EventRecord(received_at=datetime(2018, 2, 19, 0, 0, 11), source_type="datastoretest", owner="a", data={})
    one.fingerprint = "f1"
    two = EventRecord(received_at=datetime(2018, 2, 20, 0, 0, 11), source_type="datastoretest", owner="a", data={})
    two.fingerprint = "f2"
    three = EventRecord(received_at=datetime(2018, 2, 21, 0, 0, 11), source_type="datastoretest2", owner="a", data={})
    three.fingerprint = "f1"

    records = [one, two, three]
    records = remove_duplicate_events(records)
    assert three in records
    assert one not in records
    assert len(records) == 2


@pytest.fixture
def ds_instance():
    yield comet_core.data_store.DataStore("sqlite://")


@pytest.fixture
def non_addressed_event():
    # event sent but missing in the ignore_event table to
    # indicate that it wasn't addressed by the user
    event = EventRecord(
        received_at=datetime(2018, 7, 7, 9, 0, 0),
        source_type="datastoretest",
        owner="a",
        sent_at=datetime(2018, 7, 7, 9, 0, 0),
        data={},
    )
    event.fingerprint = "f1"
    return event


@pytest.fixture
def addressed_event(ds_instance):
    # event was sent and addressed (ack by the user)
    ack_event = EventRecord(
        received_at=datetime(2018, 7, 7, 9, 30, 0),
        source_type="datastoretest",
        owner="a",
        sent_at=datetime(2018, 7, 7, 9, 30, 0),
        data={},
    )
    ack_event.fingerprint = "f2"
    ds_instance.ignore_event_fingerprint(ack_event.fingerprint, ignore_type=IgnoreFingerprintRecord.ACKNOWLEDGE)
    return ack_event


@pytest.fixture
def event_to_escalate(ds_instance):
    # event was sent and addressed (escalated by the user)
    escalated_event = EventRecord(
        received_at=datetime(2018, 7, 7, 9, 30, 0),
        source_type="datastoretest",
        owner="a",
        sent_at=datetime(2018, 7, 7, 9, 30, 0),
        data={},
    )
    escalated_event.fingerprint = "f3"
    ds_instance.ignore_event_fingerprint(
        escalated_event.fingerprint, ignore_type=IgnoreFingerprintRecord.ESCALATE_MANUALLY
    )
    return escalated_event


@pytest.fixture
def ds_with_real_time_events(ds_instance, addressed_event, non_addressed_event, event_to_escalate):
    ds_instance.add_record(addressed_event)
    ds_instance.add_record(non_addressed_event)
    ds_instance.add_record(event_to_escalate)
    return ds_instance


def test_get_real_time_events_did_not_addressed(ds_with_real_time_events, non_addressed_event):
    source_type = "datastoretest"
    non_addressed_events = ds_with_real_time_events.get_events_did_not_addressed(source_type)

    assert non_addressed_event in non_addressed_events


def test_get_real_time_events_need_escalation(ds_with_real_time_events, event_to_escalate):
    source_type = "datastoretest"
    events_to_escalate = ds_with_real_time_events.get_events_need_escalation(source_type)

    assert event_to_escalate in events_to_escalate


def test_ignore_event_fingerprint_with_metadata(ds_instance):
    fingerprint = "f1"
    record_metadata = {"slack_channel": "channel"}
    ds_instance.ignore_event_fingerprint(
        fingerprint, ignore_type=IgnoreFingerprintRecord.ESCALATE_MANUALLY, record_metadata=record_metadata
    )
    result = (
        ds_instance.session.query(IgnoreFingerprintRecord)
        .filter(IgnoreFingerprintRecord.fingerprint == fingerprint)
        .one_or_none()
    )
    assert result.record_metadata == record_metadata
