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

"""Test comet_handler"""

from datetime import datetime, timedelta
from unittest import mock
import json

from freezegun import freeze_time
from marshmallow import fields, Schema

from comet_core import Comet
from comet_core.app import EventContainer
from comet_core.model import EventRecord, IgnoreFingerprintRecord

# Helpers for test_process_unsent_events_recipient_override
RECORD_OWNER = 'not-this-one@test.com'
OVERRIDE_OWNER = 'override@test.com'


class TheTestSchema(Schema):
    """Testing schema."""
    test = fields.Str(required=True)


class TheTestType():  # pylint: disable=abstract-method
    """Testing type."""
    schema = TheTestSchema
    config = {
        'owner_reminder_cadence': timedelta(days=7)
    }

    def set_record(self, record):
        self.record = record
        self.record.owner_email = RECORD_OWNER
        self.record.fingerprint = 'test'


EVENT_RECORD_WITH_OVERRIDE = EventRecord(received_at=datetime(2018, 2, 19, 0, 0, 11),
                                         source_type='test',
                                         data={'test': 'test'})


@freeze_time('2018-05-09 09:00:00')
# pylint: disable=missing-docstring
def test_process_unprocessed_events():
    app = Comet()
    app.register_parser('datastoretest', json)
    app.register_parser('datastoretest2', json)
    app.register_parser('datastoretest3', json)

    specific_router = mock.Mock()
    router = mock.Mock()
    escalator = mock.Mock()
    app.register_router('datastoretest2', func=specific_router)
    app.register_router(func=router)
    app.register_escalator(func=escalator)

    check_user = 'an_owner'
    already_processed_user = 'already_processed_owner'

    app.data_store.add_record(
        EventRecord(id=1,
                    received_at=datetime.utcnow() - timedelta(days=5),
                    source_type='datastoretest',
                    owner=already_processed_user,
                    data={},
                    processed_at=datetime.utcnow() - timedelta(days=5),
                    fingerprint='f1'))

    app.data_store.add_record(
        EventRecord(id=2,
                    received_at=datetime.utcnow() - timedelta(days=4),
                    source_type='datastoretest',
                    owner=already_processed_user,
                    data={},
                    fingerprint='f1'))

    app.data_store.add_record(
        EventRecord(id=3,
                    received_at=datetime.utcnow() - timedelta(days=3),
                    source_type='datastoretest2',
                    owner=check_user,
                    data={},
                    fingerprint='f3'))

    app.data_store.add_record(
        EventRecord(id=4,
                    received_at=datetime.utcnow() - timedelta(days=3),
                    source_type='datastoretest3',
                    owner=check_user,
                    data={},
                    fingerprint='f4'))

    app.process_unprocessed_events()
    assert specific_router.call_count == 1
    assert router.call_count == 2
    assert router.call_args[0][2][0].owner == check_user
    assert escalator.call_count == 3


def test_event_container():
    container = EventContainer('test', {})
    container.set_owner('testowner')
    container.set_fingerprint('testfp')
    container.set_metadata({'a': 'b'})

    record = container.get_record()
    assert record.owner == 'testowner'
    assert record.fingerprint == 'testfp'
    assert 'a' in record.event_metadata


def test_message_callback(app):
    @app.register_parser('test')
    class TestParser:
        def loads(self, msg):
            ev = json.loads(msg)
            if 'a' in ev:
                return ev, None
            return None, 'fail'

    hydrator_mock = mock.Mock()
    app.register_hydrator('test', hydrator_mock)

    assert not app.message_callback('test1', '{}')
    assert not app.message_callback('test', '{ "c": "d" }')

    app.message_callback('test', '{ "a": "b" }')
    assert hydrator_mock.called


def test_register_input(app):
    assert not app.inputs

    @app.register_input(a='b', c='d')
    class TestInput:
        pass

    app.register_input(TestInput)

    assert len(app.inputs) == 2


def test_register_parser(app):
    assert not app.parsers

    @app.register_parser('test1')
    class TestParser:
        pass

    # Override existing
    app.register_parser('test1', TestParser)
    assert len(app.parsers) == 1
    app.register_parser('test2', TestParser)
    assert len(app.parsers) == 2


def test_register_hydrator(app):
    assert not app.hydrators

    @app.register_hydrator('test1')
    def test_hydrator(*args):
        pass

    # Override existing
    app.register_hydrator('test1', test_hydrator)
    assert len(app.hydrators) == 1, app.hydrators

    # Add another
    app.register_hydrator('test2', test_hydrator)
    assert len(app.hydrators) == 2, app.hydrators


def test_set_config(app):
    assert not app.specific_configs

    app.set_config('test1', {})
    app.set_config('test2', {})

    assert len(app.specific_configs) == 2


def test_register_router(app):
    assert not app.routers.func_count()

    @app.register_router()
    def test_router(*args):
        pass

    app.register_router(func=test_router)
    assert app.routers.func_count() == 2

    @app.register_router('test1')
    def test_router2(*args):
        pass

    app.register_router('test1', test_router2)
    assert len(list(app.routers.for_source_type('test1'))) == 4  # 2 global, 2 specific
    assert len(list(app.routers.for_source_type('test2'))) == 2  # 2 global, 0 specific

    app.register_router('test2', test_router2)
    assert len(list(app.routers.for_source_type('test2'))) == 3  # 2 global, 1 specific

    app.register_router(['test1', 'test2'], test_router2)
    assert len(list(app.routers.for_source_type('test1'))) == 5  # 2 global, 3 specific
    assert len(list(app.routers.for_source_type('test2'))) == 4  # 2 global, 2 specific


def test_register_escalator(app):
    assert not app.escalators.func_count()

    @app.register_escalator()
    def test_escalator(*args):
        pass

    assert app.escalators.func_count()


def test_validate_config(app):
    @app.register_parser('test1')
    class TestParser:
        pass

    assert app.parsers
    app.validate_config()
    assert not app.parsers

    app = Comet()

    app.register_parser('test1', TestParser)

    @app.register_router('test1')
    def test_router(*args):
        pass

    app.validate_config()
    assert app.parsers


def test_start_stop_inputs(app):
    class TestInput:
        __init__ = mock.Mock(return_value=None)
        stop = mock.Mock()

    app.register_input(TestInput, a='b')

    assert not TestInput.__init__.called
    app.start_inputs()
    assert TestInput.__init__.called
    assert 'a' in TestInput.__init__.call_args[1]

    app.stop()
    assert TestInput.stop.called


def test_run(app):
    def f(*args):
        app.running = False

    app.process_unprocessed_events = mock.Mock()
    with mock.patch("time.sleep") as mocked_sleep:
        mocked_sleep.side_effect = f
        app.run()
    mocked_sleep.assert_called_once()
    app.process_unprocessed_events.assert_called_once()


@freeze_time('2018-05-09 09:00:00')
# pylint: disable=missing-docstring
def test_process_unprocessed_real_time_events():
    app = Comet()
    app.register_parser('real_time_source', json)
    app.register_parser('datastoretest', json)

    app.register_real_time_source('real_time_source')

    real_time_router = mock.Mock()
    router = mock.Mock()
    escalator = mock.Mock()
    app.register_router('real_time_source', func=real_time_router)
    app.register_router(func=router)
    app.register_escalator(func=escalator)

    check_user = 'an_owner'
    already_processed_user = 'already_processed_owner'

    # already processed regular event
    app.data_store.add_record(
        EventRecord(id=1,
                    received_at=datetime.utcnow() - timedelta(days=5),
                    source_type='datastoretest',
                    owner=already_processed_user,
                    data={},
                    processed_at=datetime.utcnow() - timedelta(days=5),
                    fingerprint='f1'))

    # already processed real time event
    app.data_store.add_record(
        EventRecord(id=2,
                    received_at=datetime.utcnow() - timedelta(days=5),
                    source_type='real_time_source',
                    owner=already_processed_user,
                    processed_at=datetime.utcnow() - timedelta(days=5),
                    data={},
                    fingerprint='f2'))

    # not processed real time event
    app.data_store.add_record(
        EventRecord(id=3,
                    received_at=datetime.utcnow() - timedelta(days=3),
                    source_type='real_time_source',
                    owner=check_user,
                    data={},
                    fingerprint='f3'))

    # real time event needs escalation
    app.data_store.add_record(
        EventRecord(id=4,
                    received_at=datetime.utcnow() - timedelta(days=3),
                    source_type='real_time_source',
                    owner=check_user,
                    data={},
                    fingerprint='f4'))

    app.data_store.ignore_event_fingerprint('f4',
                                            IgnoreFingerprintRecord.ESCALATE_MANUALLY)

    app.process_unprocessed_events()
    assert real_time_router.call_count == 1
    # route real time alert with both routers.
    assert router.call_count == 1
    assert real_time_router.call_args[0][2][0].owner == check_user
    assert escalator.call_count == 1


def test_handle_non_addressed_events():
    app = Comet()
    app.register_parser('real_time_source', json)
    app.register_parser('real_time_source2', json)
    app.register_real_time_source('real_time_source')
    app.register_real_time_source('real_time_source2')

    app.set_config('real_time_source', {'alerts': {
        'alert search name':
            {
                'escalate_cadence': timedelta(minutes=45),
                'template': 'alerts_template'
            }
    }})
    app.set_config('real_time_source2', {'alerts': {
        'alert search name':
            {
                'escalate_cadence': timedelta(minutes=45),
                'template': 'alerts_template'
            }
    }})
    escalator = mock.Mock()
    escalator2 = mock.Mock()
    app.register_escalator('real_time_source', func=escalator)
    app.register_escalator('real_time_source2', func=escalator2)

    already_processed_user = 'already_processed_owner'

    # already processed real time event - needs escalation
    app.data_store.add_record(
        EventRecord(id=2,
                    received_at=datetime.utcnow() - timedelta(hours=1),
                    source_type='real_time_source',
                    owner=already_processed_user,
                    processed_at=datetime.utcnow() - timedelta(hours=1),
                    sent_at=datetime.utcnow() - timedelta(hours=1),
                    data={'search_name': 'alert search name',
                          'name': 'needs escalation'},
                    fingerprint='f2'))

    # already processed real time event - still early for escalation
    # the event sent 35 min ago.
    app.data_store.add_record(
        EventRecord(id=3,
                    received_at=datetime.utcnow() - timedelta(hours=1),
                    source_type='real_time_source2',
                    owner=already_processed_user,
                    processed_at=datetime.utcnow() - timedelta(minutes=35),
                    sent_at=datetime.utcnow() - timedelta(minutes=35),
                    data={'search_name': 'alert search name',
                          'name': 'doesnt need escalation'},
                    fingerprint='f3'))

    app.handle_non_addressed_events()
    assert escalator.call_count == 1
    assert escalator2.call_count == 0


def test_register_real_time_source(app):
    assert not app.real_time_sources

    app.register_real_time_source('test1')
    app.register_real_time_source('test2')

    assert len(app.real_time_sources) == 2
