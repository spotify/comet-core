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

"""The Comet app"""
import logging
import signal
import time
from datetime import datetime, timedelta
from comet_core.data_store import DataStore
from comet_core.model import EventRecord
from comet_core.fingerprint import comet_event_fingerprint

LOG = logging.getLogger(__name__)


class EventContainer:
    """This is the container of an event that is passed to the hydrator functions.

    Args:
        source_type (str): the source type of the message
        message (dict): the message data
    """

    def __init__(self, source_type, message):
        self.source_type = source_type
        self.message = message
        self.owner = None
        self.fingerprint = comet_event_fingerprint(data_dict=message,
                                                   prefix=source_type + '_')
        self.event_metadata = dict()

    def get_record(self):
        """Make the event container into a database record.

        Returns:
            EventRecord: the database record for this event
        """
        return EventRecord(source_type=self.source_type,
                           fingerprint=self.fingerprint, owner=self.owner,
                           event_metadata=self.event_metadata,
                           data=self.message)

    def set_owner(self, owner):
        """Set the owner of the event.

        Args:
            owner (str): the owner of the event
        """
        self.owner = owner

    def set_fingerprint(self, fingerprint):
        """Set the fingerprint of the event.

        Args:
            fingerprint (str): the fingerprint that makes this event uniquely identifiable
        """
        self.fingerprint = fingerprint

    def set_metadata(self, metadata):
        """Set optional metadata for the event.

        Args:
            metadata (dict): arbitrary metadata for the event
        """
        self.event_metadata = metadata


class SourceTypeFunction:
    """This is a collection that can register a function for one, many or all source_types."""

    def __init__(self):
        self.specific_collection = dict()
        self.global_collection = list()

    def add(self, source_types, func):
        """Adds a function for the specified source_types, or all if not specified.

        Args:
            source_types (Union[str,list,None]): which source types to register for as a str, list or None.
                Given a string, the function is registered for that source_type only.
                Given a list, the function is registered for all source_types in that list.
                Given none, the function is registered for all source_types.
            func (function): the function to register
        """
        if source_types:
            if isinstance(source_types, str):
                self.specific_collection.setdefault(source_types, []).append(
                    func)
            elif isinstance(source_types, list):
                for source_type in source_types:
                    self.specific_collection.setdefault(source_type, []).append(
                        func)
        else:
            self.global_collection.append(func)

    def for_source_type(self, source_type):
        """Get all applicable functions for a given source_type.

        Args:
            source_type (str): the source_type to get the registered functions for
        Yields:
            function: functions registered to the specified source_type
        """
        for func in self.specific_collection.get(source_type, []):
            yield func
        for func in self.global_collection:
            yield func

    def func_count(self):
        """Returns the amount of functions registered in total, useful for testing.

        Returns:
            int: the total amount of registered functions
        """
        res = 0
        for val in self.specific_collection.values():
            res += len(val)
        return res + len(self.global_collection)


# pylint: disable=too-many-instance-attributes
class Comet:
    """The main Comet class

    Args:
        database_uri (str): the database to connect to as an URI
    """

    def __init__(self, database_uri='sqlite://'):
        self.running = False
        self.data_store = DataStore(database_uri)

        self.inputs = list()
        self.instantiated_inputs = list()
        self.hydrators = dict()
        self.filters = dict()
        self.parsers = dict()
        self.routers = SourceTypeFunction()
        self.escalators = SourceTypeFunction()
        self.real_time_sources = list()
        self.real_time_config_providers = dict()

        self.database_uri = database_uri
        self.batch_config = {
            'wait_for_more': timedelta(seconds=3),
            'max_wait': timedelta(seconds=4),
            'new_threshold': timedelta(days=7),
            'owner_reminder_cadence': timedelta(days=7),
            'escalation_time': timedelta(seconds=10),
            'escalation_reminder_cadence': timedelta(days=7)
        }
        self.specific_configs = {}

    def message_callback(self, source_type, message):
        """This is the callback that inputs should call when they receive new messages

        Args:
            source_type (str): the source type of the message
            message (str): the message as a string
        Return:
            boolean: True if parsing was successful, False otherwise
        """
        LOG.info('received a message', extra={'source_type': source_type})
        parse = self.parsers.get(source_type)
        if not parse:
            LOG.warning(f'no parser found', extra={'source_type': source_type})
            return False

        try:
            message_dict = parse(message)
        except ValueError as err:
            LOG.warning(f'invalid message', extra={'source_type': source_type, 'error': str(err)})
            return False

        # Prepare an event container
        event = EventContainer(source_type, message_dict)

        # Hydrate
        hydrate = self.hydrators.get(source_type)
        if hydrate:
            hydrate(event)

        # Filter event
        filter_event = self.filters.get(source_type)
        if filter_event:
            event = filter_event(event)

        # Add to datastore
        if event:
            self.data_store.add_record(event.get_record())
        return True

    def set_config(self, source_type, config):
        """Call to override default batching and batch escalation logic.

        Args:
            source_type (str): the source type to override the configuration for
            config (dict): the config values to override
        """
        self.specific_configs[source_type] = config

    def register_input(self, clazz=None, **kwargs):
        """Register an input, with optional configuration.

        This method can be used either as a decorator or with a class passed in.

        The input will be registered but will not be instantiated until `run` is called.
        This is to ensure that we do not get messages into the pipeline before the rest of the logic, parsers,
        hydrators etc, is registered.

        Args:
            clazz (class): a class or None if used as a decorator
            kwargs (dict): optional configuration values to pass to the constructor or clazz
        Return:
            function or None: if no clazz is given returns a decorator function, otherwise None
        """
        if not clazz:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(clazz):
                self.inputs.append((clazz, kwargs))
                return clazz

            return decorator
        else:
            self.inputs.append((clazz, kwargs))

    def register_parser(self, source_type, func=None):
        """Register a parser function.

        This method can be used either as a decorator or with a parser function passed in.

        Args:
            source_type (str): the source type to register the parser for
            func (Optional[function]): a function that parse a message of type source_type, or None if used as a
                decorator
        Return:
            function or None: if no scehma is given returns a decorator function, otherwise None
        """
        if not func:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(func):
                self.parsers[source_type] = func
                return func

            return decorator
        else:
            self.parsers[source_type] = func

    def register_config_provider(self, source_type, func=None):
        """Register, per source type, a function that return config given a real time event.

        This method can be used either as a decorator or with a parser function passed in.

        Args:
            source_type (str): the source type to register the config provider for
            func (Optional[function]): a function that accepts an event and return a dictionary with configuration
        Return:
            dict: the config for the given real time event
        """
        if not func:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(func):
                self.real_time_config_providers[source_type] = func
                return func

            return decorator
        else:
            self.real_time_config_providers[source_type] = func

    def register_real_time_source(self, source_type):
        """Register real time source type
        Args:
            source_type (str): the source type to register the parser for
        """
        self.real_time_sources.append(source_type)

    def register_hydrator(self, source_type, func=None):
        """Register a hydrator.

        This method can be used either as a decorator or with a hydrator function passed in.

        Args:
            source_type (str): the source type to register the parser for
            func (Optional[function]): a function that hydrates a message of type source_type, or None if used as a
                decorator
        Return:
            function or None: if no func is given returns a decorator function, otherwise None
        """
        if not func:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(func):
                self.hydrators[source_type] = func
                return func

            return decorator
        else:
            self.hydrators[source_type] = func

    def register_filter(self, source_type, func=None):
        """Register a filter function to filter events before saving them to the db.

        This method can be used either as a decorator or with a filter function passed in.

        Args:
            source_type (str): the source type to register the filter for
            func (Optional[function]): a function that filter a message of type source_type, or None if used as a
                decorator
        Return:
            function or None: if no func is given returns a decorator function, otherwise None
        """
        if not func:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(func):
                self.filters[source_type] = func
                return func

            return decorator
        else:
            self.filters[source_type] = func

    def register_router(self, source_types=None, func=None):
        """Register a router.

        This method can be used either as a decorator or with a routing function passed in.

        Args:
            source_types (Optional[Union[str,list]]): a source type or multiple source types (in a list) to route, or
                None to route all source types
            func (Optional[function]): a function that routes batched messages, or None if used as a decorator
        Return:
            function or None: if no func is given returns a decorator function, otherwise None
        """
        if not func:
            # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
            def decorator(func):
                self.routers.add(source_types, func)
                return func

            return decorator
        self.routers.add(source_types, func)

    def register_escalator(self, source_types=None, func=None):
        """Register a escalator.

        This method can be used either as a decorator or with a escalator function passed in.

        Args:
            source_types (Optional[Union[str,list]]): a source type or multiple source types (in a list) to escalate,
                or None to route all source types
            func (Optional[function]): a function that escalates messages, or None if used as a decorator

        Return:
            function or None: if no func is given returns a decorator function, otherwise None
        """
        # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
        if not func:
            def decorator(func):
                self.escalators.add(source_types, func)
                return func

            return decorator
        self.escalators.add(source_types, func)

    #  pylint: disable=too-many-branches
    def process_unprocessed_events(self):
        """Checks the database for unprocessed events and processes them.

        Processing means: group by source-type and owner, check if for each source-type/owner set there is at least
        one new event or one old event that the owner needs to be reminded about, and sends notification if that's the
        case. It also will checks for all events if escalation is needed and send it if no escalation was sent to the
        same escalation recipient recently. All ignored events will be skipped for the above, but marked as processed.

        Config options we care about:
            source_type_config['owner_reminder_cadence']:
            source_type_config['notifications_send_emails']
            source_type_config['escalation_time'],
            source_type_config['escalation_reminder_cadence']
            source_type_config['recipient_override']
            source_type_config['email_subject']:
            source_type_config['wait_for_more']:
            source_type_config['max_wait']:
        """
        LOG.debug('Processing unprocessed events')

        # pylint: disable=consider-iterating-dictionary
        for source_type in self.parsers.keys():
            source_type_config = self.batch_config
            if source_type in self.specific_configs:
                source_type_config.update(self.specific_configs[source_type])

            batch_events = self.data_store.get_unprocessed_events_batch(
                source_type_config['wait_for_more'],
                source_type_config['max_wait'],
                source_type)

            events_by_owner = {}
            ignored_events = []
            need_escalation_events = []

            if source_type in self.real_time_sources:
                real_time_events_by_owner = {}
                for event in batch_events:
                    real_time_events_by_owner.setdefault(event.owner,
                                                         []).append(event)
                # handle unprocessed real_time alerts
                self._handle_real_time_alerts(real_time_events_by_owner,
                                              source_type)
                # check if real time alerts need escalation
                events_to_escalate = \
                    self.data_store.get_events_need_escalation(
                        source_type)
                self._handle_events_need_escalation(source_type,
                                                    events_to_escalate)

            else:
                # Group events by owner and mark them as new or seen before
                for event in batch_events:
                    if self.data_store.fingerprint_is_ignored(event.fingerprint):
                        ignored_events.append(event)
                    else:
                        event.new = self.data_store.check_if_new(event.fingerprint,
                                                                 source_type_config['new_threshold'])
                        event.needs_escalation = False
                        if self.data_store.check_needs_escalation(
                                source_type_config['escalation_time'], event):
                            event.needs_escalation = True
                            event.first_escalation = not self.data_store.check_if_previously_escalated(
                                event)
                            need_escalation_events.append(event)
                        events_by_owner.setdefault(event.owner, []).append(event)

            if ignored_events:
                self.data_store.update_processed_at_timestamp_to_now(
                    ignored_events)
                LOG.info('events-ignored',
                         extra={'events': len(ignored_events)})

            # Determine if we should send an email to the system owner
            # This happens if there are events that..
            #  * ..has not been seen before
            #  * ..was last sent to the owner X days ago
            # (where X is `owner_reminder_cadence`, default 7 days)
            for owner, events in events_by_owner.items():
                owner_reminder_cadence = \
                    source_type_config['owner_reminder_cadence']
                if any([event.new for event in events]) \
                        or self.data_store.\
                        check_any_issue_needs_reminder(owner_reminder_cadence,
                                                       events):
                    self._route_events(owner, events, source_type)

                self.data_store.update_processed_at_timestamp_to_now(events)
                LOG.info('events-processed', extra={
                    'events': len(events),
                    'source-type': source_type,
                    'owner': owner
                })

            # Check if any of the events for this source_type needs
            # escalation and if we may send an escalation
            if need_escalation_events and self.data_store. \
                    may_send_escalation(source_type, source_type_config['escalation_reminder_cadence']):
                self._handle_events_need_escalation(source_type,
                                                    need_escalation_events)

    def handle_non_addressed_events(self):
        """Check if there are real time events sent to a user that were not addressed.

        Each event has escalate_cadence parameter which is used as the earliest time to escalate if the user did
        not address the alert.
        """
        for source_type in self.real_time_sources:
            non_addressed_events = self.data_store.get_events_did_not_addressed(source_type)
            events_needs_escalation = []
            for event in non_addressed_events:
                # load configuration for event, using batch settings as default
                event_config = {}
                if source_type in self.real_time_config_providers:
                    event_config = self.real_time_config_providers[source_type](event)

                escalate_cadence = event_config.get('escalate_cadence', timedelta(hours=36))

                event_sent_at = event.sent_at
                # when is earliest time to escalate the specific event
                if event_sent_at <= datetime.utcnow() - escalate_cadence:
                    events_needs_escalation.append(event)

            self._handle_events_need_escalation(source_type,
                                                events_needs_escalation)

    def _route_events(self, owner, events, source_type):
        """route events need routing by getting the route function
           function from the source type and route the events.
        Args:
            owner (str): the owner of the events
            events (list(EventRecord)): events to route
            source_type (str): source type to get escalator functions.
        """
        routers = list(self.routers.for_source_type(source_type))
        if not routers:
            LOG.warning('no-router', extra={'source-type': source_type})
        for route_func in routers:
            route_func(source_type, owner, events)

        self.data_store.update_sent_at_timestamp_to_now(events)

        LOG.info('event-notification-sent', extra={
            'events': len(events),
            'source-type': source_type,
            'owner': owner
        })

    def _handle_real_time_alerts(self, real_time_events_by_owner, source_type):
        """Handle real time alerts by sending the alerts to the owner
            without any checks
        Args:
            real_time_events_by_owner (dict): events by owner
            source_type (str): source type to get the specific router
        """
        if real_time_events_by_owner:
            for owner, events in real_time_events_by_owner.items():
                self._route_events(owner, events, source_type)
                self.data_store.update_processed_at_timestamp_to_now(events)

    def _handle_events_need_escalation(self, source_type,
                                       needs_escalation_events):
        """Handle events need escalation by getting the escalate
           function from the source type and escalate.
        Args:
            source_type (str): source type to get escalator functions.
            needs_escalation_events (list(EventRecord)): events need escalation
        """
        if needs_escalation_events:
            did_escalate = False
            for escalator_func in self.escalators.for_source_type(source_type):
                did_escalate = True
                escalator_func(source_type, needs_escalation_events)

                LOG.info('event-escalated', extra={
                    'events': len(needs_escalation_events),
                    'source_type': source_type
                })

            if not did_escalate:
                LOG.warning('event-not-esclated', extra={
                    'events': len(needs_escalation_events),
                    'source_type': source_type
                })

            self.data_store.update_event_escalated_at_to_now(
                needs_escalation_events)

    # pylint: disable=unused-argument
    def stop(self, *args):
        """Stops all inputs.

        Args:
            *args (list): dummy args to allow function to be called from a signal
        """
        for instance in self.instantiated_inputs:
            instance.stop()
        self.running = False

    def validate_config(self):
        """Validates that every parser has a router"""
        for source_type in list(self.parsers):
            if not list(self.routers.for_source_type(source_type)):
                LOG.warning('no router found',
                            extra={'source_type': source_type})
                del self.parsers[source_type]

    def start_inputs(self):
        """Helper used to instantiate all registered inputs"""
        self.instantiated_inputs = [clazz(self.message_callback, **kwargs)
                                    for clazz, kwargs in self.inputs]

    def prepare_run(self):
        """Prepare the run for both normal running and staging"""
        self.validate_config()
        self.start_inputs()
        # Run
        self.running = True
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def staging(self):
        """For starting staging env"""
        self.prepare_run()
        timeout = time.time() + 60  # this is to wait 1 minute
        while self.running:
            self.process_unprocessed_events()
            self.handle_non_addressed_events()
            time.sleep(0.1)
            if time.time() > timeout:
                self.stop()

    def run(self):
        """Start the Comet app"""
        self.prepare_run()
        while self.running:
            self.process_unprocessed_events()
            self.handle_non_addressed_events()
            time.sleep(0.1)
