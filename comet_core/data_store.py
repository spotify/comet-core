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

"""Data Store module - interface to database."""

from datetime import datetime, timedelta
from itertools import tee, islice, chain

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func

from comet_core.model import BaseRecord, EventRecord, IgnoreFingerprintRecord


Session = sessionmaker(autocommit=True)  # pylint: disable=invalid-name


def now_and_next(some_iterable):
    """
    get an iterator in a nice itertools way so we can act on the list as we remove args
    Args:
        some_iterable (iterable): any iterable list

    Returns:
        tuple: tuple of two items in a row
    """
    items, nexts = tee(some_iterable, 2)
    nexts = chain(islice(nexts, 1, None), [None])
    return zip(items, nexts)


def remove_duplicate_events(event_record_list):
    """
    This removes duplicates based on fingerprint and chooses the newest issue
    Args:
        event_record_list (list): list of EventRecords
    Returns:
        list: of EventRecords with extra fingerprints removed
    """
    event_record_list = sorted(event_record_list, key=lambda x: (x.fingerprint, x.received_at))
    for issue_1, issue_2 in now_and_next(event_record_list):
        if issue_1 and issue_2 and issue_1.fingerprint == issue_2.fingerprint:
            event_record_list.remove(issue_1)
    return event_record_list


class DataStore:
    """Abstraction of the comet storage layer.

    Args:
        database_uri (str): the database to use
    """
    def __init__(self, database_uri):
        self.engine = create_engine(database_uri)
        self.connection = self.engine.connect()

        Session.configure(bind=self.engine)
        self.session = Session()

        BaseRecord.metadata.create_all(self.engine)

    def add_event(self, event):
        """Add an event to the data store.

        Args:
            event (PluginBase): a typed event to add to the database.
        """
        self.add_record(event.record)

    def add_record(self, record):
        """Store a record in the data store.
        Args:
            record (EventRecord): the record object to store
        """
        self.session.add(record)

    def get_unprocessed_events_batch(self, wait_for_more, max_wait, source_type):
        """Get all unprocessed events of the given source_type but only if the latest event is older than
        `wait_for_more` or the oldest event is older than `max_wait`.

        Metrics emitted:
            events-hit-max-wait

        Args:
            wait_for_more (datetime.timedelta): the amount of time to wait since the latest event
            max_wait (datetime.timedelta): the amount of time to wait since the earliest event
            source_type (str): source type of the events to look for

        Returns:
            list: list of `EventRecord`s, or empty list if there is nothing to return
        """

        # https://explainextended.com/2009/09/18/not-in-vs-not-exists-vs-left-join-is-null-mysql/
        events = self.session.query(EventRecord). \
            filter((EventRecord.processed_at.is_(None)) & (EventRecord.source_type == source_type)). \
            order_by(EventRecord.received_at.asc()).all()

        now = datetime.utcnow()

        if events and events[-1].received_at < now - wait_for_more:
            return events
        if events and events[0].received_at < now - max_wait:
            # METRIC_RELAY.emit('events-hit-max-wait', 1,
            #                   {'source-type': source_type})
            return events

        return []

    def get_events_did_not_addressed(self, source_type):
        """Get all events who we sent to the user +
           the event haven't escalated already
           and do not exist in IgnoreFingerprintRecord database.
           That means that the user didn't addressed those events.
        Args:
            source_type (str): source type to filter the search by.
        Returns:
            list: list of `EventRecord`s not addressed,
            or empty list if there is nothing to return
        """
        non_addressed_events = self.session.query(EventRecord). \
            filter((EventRecord.sent_at.isnot(None)) &
                   (EventRecord.escalated_at.is_(None)) &
                   (EventRecord.source_type == source_type)). \
            outerjoin(IgnoreFingerprintRecord,
                      EventRecord.fingerprint ==
                      IgnoreFingerprintRecord.fingerprint). \
            filter(IgnoreFingerprintRecord.fingerprint.is_(None)).all()

        return non_addressed_events

    def check_any_issue_needs_reminder(self, search_timedelta, records):
        """Checks if the issue among the provided ones with the most recent sent_at value has that value older than the
        `search_timedelta`, that is, a reminder should be sent for the issue.
        NOTE: if all database records for a fingerprint given in the `records` list have the sent_at values set to Null,
              then this fingerprint will be treated as NOT needing a reminder, which might be unintuitive.
        Args:
            search_timedelta (datetime.timedelta): reminder interval
            records (list): list of EventRecord objects to check
        Returns:
            bool: True if any of the provided records represents an issue that needs to be reminded about
        """
        fingerprints = [record.fingerprint for record in records]
        timestamps = self.session.query(func.max(EventRecord.sent_at)). \
            filter(EventRecord.fingerprint.in_(fingerprints) & EventRecord.sent_at.isnot(None)). \
            group_by(EventRecord.fingerprint).all()
        if timestamps:
            return max(timestamps)[0] <= datetime.utcnow() - search_timedelta
        return False

    def update_timestamp_column_to_now(self, records, column_name):
        """Update the `column_name` of the provided `EventRecord`s to datetime now

        Args:
            records (list): `EventRecord`s to update the `column_name` for
            column_name (str): the name of the datebase column to update
        """
        time_now = datetime.utcnow()
        updates = [{'id': r.id, column_name: time_now} for r in records]

        self.session.bulk_update_mappings(EventRecord, updates)

    def update_processed_at_timestamp_to_now(self, records):  # pylint: disable=invalid-name
        """Update the processed_at timestamp for the given records to now.

        Args:
            records (list): `EventRecord`s to update the processed for
        """
        self.update_timestamp_column_to_now(records, 'processed_at')

    def update_sent_at_timestamp_to_now(self, records):
        """Update the sent_at timestamp for the given records to now.

        Args:
            records (list): `EventRecord`s to update the sent_at for
        """
        self.update_timestamp_column_to_now(records, 'sent_at')

    def update_event_escalated_at_to_now(self, records):  # pylint: disable=invalid-name
        """Update the escalated_at timestamp for the given records to now.
        Args:
            records (list): `EventRecord`s to update
        """
        self.update_timestamp_column_to_now(records, 'escalated_at')

    def get_oldest_event_with_fingerprint(self, fingerprint):  # pylint: disable=invalid-name
        """
        Returns the oldest (first occurrence) event with the provided fingerprint.

        Args:
            fingerprint (str): fingerprint to look for
        Returns:
            EventRecord: oldest EventRecord with the given fingerprint
        """
        return self.session.query(EventRecord). \
            filter(EventRecord.fingerprint == fingerprint). \
            order_by(EventRecord.received_at.asc()). \
            limit(1). \
            one_or_none()

    def get_latest_event_with_fingerprint(self, fingerprint):  # pylint: disable=invalid-name
        """
        Returns the latest (in other words: the newest, closest to now) event with the provided fingerprint.

        Args:
            fingerprint (str): fingerprint to look for
        Returns:
            EventRecord: latest EventRecord with the given fingerprint
        """
        return self.session.query(EventRecord). \
            filter(EventRecord.fingerprint == fingerprint). \
            order_by(EventRecord.received_at.desc()). \
            limit(1). \
            one_or_none()

    def check_needs_escalation(self, escalation_time, event):
        """Checks if the event needs to be escalated. Returns True if the first occurrence of an event with the same
        fingerprint is older than the escalation time.
        Args:
            escalation_time (datetime.timedelta): time to delay escalation
            event (EventRecord): EventRecord to check
        Returns:
            bool: True if the event should be escalated
        """
        oldest_event = self.get_oldest_event_with_fingerprint(event.fingerprint)

        if not oldest_event:
            return False

        return oldest_event.received_at <= datetime.utcnow() - escalation_time

    def ignore_event_fingerprint(self, fingerprint, ignore_type, expires_at=None):
        """Add a fingerprint to the list of ignored events
        Args:
            fingerprint (str): fingerprint of the event to ignore
            ignore_type (str): the type (reason) for ignoring, for example IgnoreFingerprintRecord.SNOOZE
            expires_at (datetime.datetime): specify the time of the ignore expiration
        """
        new_record = IgnoreFingerprintRecord(fingerprint=fingerprint, ignore_type=ignore_type, expires_at=expires_at)
        self.session.begin()
        self.session.add(new_record)
        self.session.commit()

    def fingerprint_is_ignored(self, fingerprint):
        """Check if a fingerprint is marked as ignored (whitelisted or snoozed)
        Args:
            fingerprint (str): fingerprint of the event
        Returns:
            bool: True if whitelisted
        """
        return self.session.query(IgnoreFingerprintRecord). \
            filter(IgnoreFingerprintRecord.fingerprint == fingerprint). \
            filter((IgnoreFingerprintRecord.expires_at > datetime.utcnow()) |
                   (IgnoreFingerprintRecord.expires_at.is_(None))). \
            count() >= 1

    def may_send_escalation(self, source_type, escalation_reminder_cadence):
        """Check if we are allowed to send another esclation notification to the source_type escalation recipient.
        Returns false if there was an escalation sent to them within `escalation_reminder_cadence`.

        Args:
            source_type (str): source type of the events
            escalation_reminder_cadence (datetime.timedelta): time to wait before sending next escalation

        Returns:
            bool: True if an escalation may be sent, False otherwise
        """
        last_escalated = self.session.query(EventRecord.escalated_at). \
            filter(EventRecord.source_type == source_type). \
            order_by(EventRecord.escalated_at.desc()). \
            limit(1).one_or_none()

        if not last_escalated[0]:
            return True

        return last_escalated[0] <= datetime.utcnow() - escalation_reminder_cadence

    def check_if_previously_escalated(self, event):
        """Checks if the issue was escalated before. This looks for previous escalations sent for events with the same
        fingerprint.

        Args:
            event (EventRecord): one event of the issue to check

        Returns:
            bool: True if any previous event with the same fingerprint was escalated, False otherwise
        """
        return self.session.query(EventRecord). \
            filter(EventRecord.fingerprint == event.fingerprint). \
            filter(EventRecord.escalated_at.isnot(None)). \
            count() >= 1

    def get_open_issues(self, owners):
        """Return a list of open (newer than 24h), not whitelisted or snoozed issues for the given owners.
        Args:
            owners (list): list of strings, containing owners
        Returns:
            list: list of EventRecord, representing open, non-ignored issues for the given owners
        """
        open_issues = self.session.query(EventRecord). \
            filter(EventRecord.owner.in_(owners)). \
            filter(EventRecord.received_at >= datetime.utcnow() - timedelta(days=1)). \
            all()

        open_issues = remove_duplicate_events(open_issues)

        open_issues_fps = [issue.fingerprint for issue in open_issues]

        ignored_issues_fps_tuples = self.session.query(IgnoreFingerprintRecord.fingerprint). \
            filter(IgnoreFingerprintRecord.fingerprint.in_(open_issues_fps)). \
            filter((IgnoreFingerprintRecord.expires_at > datetime.utcnow()) |
                   (IgnoreFingerprintRecord.expires_at.is_(None))).all()

        ignored_issues_fps = [t[0] for t in ignored_issues_fps_tuples]

        return [issue for issue in open_issues if issue.fingerprint not in ignored_issues_fps]

    def check_if_new(self, fingerprint, new_threshold):
        """Check if an issue is new. An issue is treated as new if there are no events with the same fingerprint OR
        if there are older events with the same fingerprint but the most recent one of them is older than
        `new_threshold`. The idea with the second condition is to flag regressions as new issues, but allow for some
        flakyness (eg a scanner not running a day should not flag all old open issues as new when it runs the day
        after again).

        Args:
            fingerprint (str): fingerprint of the issue to evaluate
            new_threshold (datetime.timedelta): time after which an issue should be considered new again, even if it was
            seen before
        Returns:
            bool: True if the issue is new, False if it is old.
        """

        most_recent_processed = self.session.query(EventRecord.received_at). \
            filter(EventRecord.fingerprint == fingerprint). \
            filter(EventRecord.processed_at.isnot(None)). \
            order_by(EventRecord.received_at.desc()). \
            limit(1).one_or_none()

        if not most_recent_processed:
            return True

        return most_recent_processed[0] <= datetime.utcnow() - new_threshold

    def get_events_need_escalation(self, source_type):
        """
        Get all the events that the end user escalate manually
        and weren't escalated already by comet.
        Args:
            source_type (str): source type to filter the search by.
        Returns:
            list: list of `EventRecord`s to escalate.
        """
        events_to_escalate = self.session.query(EventRecord). \
            filter((EventRecord.sent_at.isnot(None)) &
                   (EventRecord.escalated_at.is_(None)) &
                   (EventRecord.source_type == source_type)). \
            outerjoin(IgnoreFingerprintRecord,
                      EventRecord.fingerprint ==
                      IgnoreFingerprintRecord.fingerprint). \
            filter(IgnoreFingerprintRecord.ignore_type ==
                   IgnoreFingerprintRecord.ESCALATE_MANUALLY).all()
        return events_to_escalate
