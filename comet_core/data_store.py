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
from typing import Dict, List, Optional

import sqlalchemy
import sqlalchemy.orm

from comet_core.model import BaseRecord, EventRecord, IgnoreFingerprintRecord


def remove_duplicate_events(event_record_list: List[EventRecord]) -> List[EventRecord]:
    """Removes duplicates based on fingerprint and chooses the newest issue.

    Args:
        event_record_list: list of EventRecords
    Returns:
        list: of EventRecords with extra fingerprints removed
    """
    events_hash_table: Dict[Optional[str], EventRecord] = {}
    for e in event_record_list:
        if e.fingerprint in events_hash_table:
            if events_hash_table[e.fingerprint].received_at < e.received_at:
                events_hash_table[e.fingerprint] = e
        else:
            events_hash_table[e.fingerprint] = e
    return list(events_hash_table.values())


class DataStore:
    """Abstraction of the Comet storage layer.

    Args:
        database_uri: Database URL to connect to. Will be passed to sqlalchemy.create_engine, refer to that
        documentation for formats.
    """

    def __init__(self, database_uri: str) -> None:
        """Creates a new DataStore instance

        Args:
            database_uri (str): Database URL to connect to. Will be passed to sqlalchemy.create_engine, refer to that
            documentation for formats.
        """
        # Setting "future" for 2.0 syntax
        engine = sqlalchemy.create_engine(database_uri, future=True)
        # expire_on_commit needs to be false due to https://docs.sqlalchemy.org/en/14/errors.html#error-bhk3
        self.session = sqlalchemy.orm.sessionmaker(engine, future=True, expire_on_commit=False)

        BaseRecord.metadata.create_all(engine)

    def add_record(self, record: EventRecord) -> None:
        """Store a record in the data store.

        Args:
            record: the record object to store
        """
        with self.session.begin() as session:
            session.add(record)

    def get_unprocessed_events_batch(
        self, wait_for_more: timedelta, max_wait: timedelta, source_type: str
    ) -> List[EventRecord]:
        """Get all unprocessed events of the given source_type but only if the latest event is older than
        `wait_for_more` or the oldest event is older than `max_wait`.

        Args:
            wait_for_more: the amount of time to wait since the latest event
            max_wait: the amount of time to wait since the earliest event
            source_type: source type of the events to look for

        Returns:
            list: list of `EventRecord`s, or empty list if there is nothing to return
        """

        # https://explainextended.com/2009/09/18/not-in-vs-not-exists-vs-left-join-is-null-mysql/
        with self.session.begin() as session:
            events: List[EventRecord] = (
                session.query(EventRecord)
                .filter((EventRecord.processed_at.is_(None)) & (EventRecord.source_type == source_type))
                .order_by(EventRecord.received_at.asc())
                .all()
            )

        now = datetime.utcnow()

        if events and events[-1].received_at < now - wait_for_more:
            return events
        if events and events[0].received_at < now - max_wait:
            return events

        return []

    def get_events_did_not_addressed(self, source_type: str) -> List[EventRecord]:
        """Get all non-escalated and non-ignored events sent to the user.

        Events that haven't escalated already and do not exist in IgnoreFingerprintRecord database are events that have
        not been addressed by the user.

        Args:
            source_type: source type to filter the search by.
        Returns:
            list: list of records not addressed, or empty list if there is nothing to return
        """
        with self.session.begin() as session:
            non_addressed_events: List[EventRecord] = (
                session.query(EventRecord)
                .filter(
                    (EventRecord.sent_at.isnot(None))
                    & (EventRecord.escalated_at.is_(None))
                    & (EventRecord.source_type == source_type)
                )
                .outerjoin(IgnoreFingerprintRecord, EventRecord.fingerprint == IgnoreFingerprintRecord.fingerprint)
                .filter(IgnoreFingerprintRecord.fingerprint.is_(None))
                .all()
            )

        return non_addressed_events

    def check_any_issue_needs_reminder(self, search_timedelta: datetime, records: List[EventRecord]) -> bool:
        """Checks if a reminder should be sent by issue by comparingsent_at value with search_timedelta.

        Check if the issue among the provided ones with the most recent sent_at value has that value older than the
        `search_timedelta`, that is, a reminder should be sent for the issue.

        NOTE: if all database records for a fingerprint given in the `records` list have the sent_at values set to Null,
        then this fingerprint will be treated as NOT needing a reminder, which might be unintuitive.

        Args:
            search_timedelta: reminder interval
            records: records to look at.
        Returns:
            bool: True if any of the provided records represents an issue that needs to be reminded about
        """
        fingerprints = [record.fingerprint for record in records]
        with self.session.begin() as session:
            timestamps: List[datetime] = (
                session.query(sqlalchemy.sql.expression.func.max(EventRecord.sent_at))
                .filter(EventRecord.fingerprint.in_(fingerprints) & EventRecord.sent_at.isnot(None))
                .group_by(EventRecord.fingerprint)
                .all()
            )
        if timestamps:
            return max(timestamps)[0] <= datetime.utcnow() - search_timedelta

        return False

    def get_any_issues_need_reminder(self, search_timedelta: timedelta, records: List[EventRecord]) -> List[str]:
        """Returns all the `fingerprints` having corresponding `event` table entries with the latest `sent_at`
        more then search_timedelta ago.
        NOTE: if all database records for a fingerprint given in the `records` list have the sent_at values set to Null,
              then this fingerprint will be treated as NOT needing a reminder, which might be unintuitive.
        Args:
            search_timedelta: reminder interval
            records: list of EventRecord objects to check
        Returns:
            list: list of fingerprints that represent issues that need to be reminded about
        """
        fingerprints = [record.fingerprint for record in records]
        with self.session.begin() as session:
            fingerprints_to_remind = (
                session.query(
                    sqlalchemy.sql.expression.func.max(EventRecord.sent_at).label("sent_at"), EventRecord.fingerprint
                )
                .filter(EventRecord.fingerprint.in_(fingerprints) & EventRecord.sent_at.isnot(None))
                .group_by(EventRecord.fingerprint)
                .all()
            )
        result = []
        deltat = datetime.utcnow() - search_timedelta
        for f in fingerprints_to_remind:
            if f.sent_at <= deltat:
                result.append(f.fingerprint)

        return result

    def update_timestamp_column_to_now(self, records: List[EventRecord], column_name: str) -> None:
        """Update the `column_name` of the provided records to now

        Args:
            records: records to update the `column_name` for
            column_name: the name of the datebase column to update
        """
        time_now = datetime.utcnow()
        updates = [{"id": r.id, column_name: time_now} for r in records]

        with self.session.begin() as session:
            session.bulk_update_mappings(EventRecord, updates)

    def update_processed_at_timestamp_to_now(self, records: List[EventRecord]) -> None:  # pylint: disable=invalid-name
        """Update the processed_at timestamp for to now.

        Args:
            records: records to update the processed_at field
        """
        self.update_timestamp_column_to_now(records, "processed_at")

    def update_sent_at_timestamp_to_now(self, records: List[EventRecord]) -> None:
        """Update the sent_at timestamp to now.

        Args:
            records: records to update the sent_at field
        """
        self.update_timestamp_column_to_now(records, "sent_at")

    def update_event_escalated_at_to_now(self, records: List[EventRecord]) -> None:  # pylint: disable=invalid-name
        """Update the escalated_at timestamp to now.

        Args:
            records: records to update the escalated_at field
        """
        self.update_timestamp_column_to_now(records, "escalated_at")

    def get_oldest_event_with_fingerprint(self, fingerprint: str) -> EventRecord:  # pylint: disable=invalid-name
        """
        Returns the oldest (first occurrence) event with the provided fingerprint.

        Args:
            fingerprint: fingerprint to look for
        Returns:
            EventRecord: oldest EventRecord with the given fingerprint
        """
        with self.session.begin() as session:
            return (
                session.query(EventRecord)
                .filter(EventRecord.fingerprint == fingerprint)
                .order_by(EventRecord.received_at.asc())
                .limit(1)
                .one_or_none()
            )

    def get_latest_event_with_fingerprint(self, fingerprint: str) -> EventRecord:  # pylint: disable=invalid-name
        """
        Returns the latest (in other words: the newest, closest to now) event with the provided fingerprint.

        Args:
            fingerprint: fingerprint to look for
        Returns:
            EventRecord: latest EventRecord with the given fingerprint
        """
        with self.session.begin() as session:
            return (
                session.query(EventRecord)
                .filter(EventRecord.fingerprint == fingerprint)
                .order_by(EventRecord.received_at.desc())
                .limit(1)
                .one_or_none()
            )

    def check_needs_escalation(self, escalation_time: timedelta, event: EventRecord) -> bool:
        """Checks if the event needs to be escalated.

        Returns True if the first occurrence of an event with the same fingerprint is older than the escalation time.

        Args:
            escalation_time: time to delay escalation
            event: EventRecord to check
        Returns:
            bool: True if the event should be escalated
        """
        oldest_event = self.get_oldest_event_with_fingerprint(event.fingerprint)

        if not oldest_event:
            return False

        return oldest_event.received_at <= datetime.utcnow() - escalation_time

    def ignore_event_fingerprint(
        self,
        fingerprint: str,
        ignore_type: str,
        expires_at: Optional[datetime] = None,
        reported_at: Optional[datetime] = None,
        record_metadata: Optional[datetime] = None,
    ) -> None:
        """Add a fingerprint to the list of ignored events.

        Args:
            fingerprint: fingerprint of the event to ignore
            ignore_type: the type (reason) for ignoring, for example IgnoreFingerprintRecord.SNOOZE
            expires_at: specify the time of the ignore expiration
            reported_at: specify the time of the reported date
            record_metadata: metadata to hydrate the record with.
        """
        new_record = IgnoreFingerprintRecord(
            fingerprint=fingerprint,
            ignore_type=ignore_type,
            expires_at=expires_at,
            reported_at=reported_at,
            record_metadata=record_metadata,
        )
        with self.session.begin() as session:
            session.add(new_record)

    def fingerprint_is_ignored(self, fingerprint: str) -> bool:
        """Check if a fingerprint is marked as ignored.

        Args:
            fingerprint (str): fingerprint of the event
        Returns:
            bool: True if whitelisted or snoozed
        """
        with self.session.begin() as session:
            return (
                session.query(IgnoreFingerprintRecord)
                .filter(IgnoreFingerprintRecord.fingerprint == fingerprint)
                .filter(
                    (IgnoreFingerprintRecord.expires_at > datetime.utcnow())
                    | (IgnoreFingerprintRecord.expires_at.is_(None))
                )
                .count()
                >= 1
            )

    def may_send_escalation(self, source_type: str, escalation_reminder_cadence: timedelta) -> bool:
        """Check if another escalation notification is allowed to the source_type escalation recipient.

        Returns false if there was an escalation sent to them within `escalation_reminder_cadence`.

        Args:
            source_type: source type of the events
            escalation_reminder_cadence: time to wait before sending next escalation

        Returns:
            bool: True if an escalation may be sent, False otherwise
        """
        with self.session.begin() as session:
            last_escalated = (
                session.query(EventRecord.escalated_at)
                .filter(EventRecord.source_type == source_type)
                .order_by(EventRecord.escalated_at.desc())
                .limit(1)
                .one_or_none()
            )

        if not last_escalated[0]:
            return True

        return last_escalated[0] <= datetime.utcnow() - escalation_reminder_cadence

    def check_if_previously_escalated(self, event: EventRecord) -> None:
        """Checks if the issue was escalated before.

        This looks for previous escalations sent for events with the same fingerprint.

        Args:
            event: one event of the issue to check

        Returns:
            bool: True if any previous event with the same fingerprint was escalated, False otherwise
        """
        with self.session.begin() as session:
            return (
                session.query(EventRecord)
                .filter(EventRecord.fingerprint == event.fingerprint)
                .filter(EventRecord.escalated_at.isnot(None))
                .count()
                >= 1
            )

    def get_open_issues(self, owners: List[str]) -> List[EventRecord]:
        """Return a list of open (newer than 24h), not whitelisted or snoozed issues for the given owners.

        Args:
            owners: list of strings, containing owners
        Returns:
            list: list of EventRecord, representing open, non-ignored issues for the given owners
        """
        with self.session.begin() as session:
            open_issues = (
                session.query(EventRecord)
                .filter(EventRecord.owner.in_(owners))
                .filter(EventRecord.received_at >= datetime.utcnow() - timedelta(days=1))
                .all()
            )

            open_issues = remove_duplicate_events(open_issues)

            open_issues_fps = [issue.fingerprint for issue in open_issues]

            ignored_issues_fps_tuples = (
                session.query(IgnoreFingerprintRecord.fingerprint)
                .filter(IgnoreFingerprintRecord.fingerprint.in_(open_issues_fps))
                .filter(
                    (IgnoreFingerprintRecord.expires_at > datetime.utcnow())
                    | (IgnoreFingerprintRecord.expires_at.is_(None))
                )
                .all()
            )

        ignored_issues_fps = [t[0] for t in ignored_issues_fps_tuples]

        return [issue for issue in open_issues if issue.fingerprint not in ignored_issues_fps]

    def check_if_new(self, fingerprint: str, new_threshold: timedelta) -> bool:
        """Check if an issue is new.

        An issue is treated as new if there are no events with the same fingerprint OR if there are older events with
        the same fingerprint but the most recent one of them is older than `new_threshold`.
        The idea with the second condition is to flag regressions as new issues, but allow for some flakyness (e.g., a
        scanner not running a day should not flag all old open issues as new when it runs the day after again).

        Args:
            fingerprint: fingerprint of the issue to evaluate
            new_threshold: time after which an issue should be considered new again, even if it was seen before
        Returns:
            bool: True if the issue is new, False if it is old.
        """

        with self.session.begin() as session:
            most_recent_processed = (
                session.query(EventRecord.received_at)
                .filter(EventRecord.fingerprint == fingerprint)
                .filter(EventRecord.processed_at.isnot(None))
                .order_by(EventRecord.received_at.desc())
                .limit(1)
                .one_or_none()
            )

        if not most_recent_processed:
            return True

        return most_recent_processed[0] <= datetime.utcnow() - new_threshold

    def get_events_need_escalation(self, source_type: str) -> List[EventRecord]:
        """Get all the events that the end user escalate manually and weren't escalated already by Comet.

        Args:
            source_type: source type to filter the search by.
        Returns:
            list: list of `EventRecord`s to escalate.
        """
        with self.session.begin() as session:
            events_to_escalate = (
                session.query(EventRecord)
                .filter(
                    (EventRecord.sent_at.isnot(None))
                    & (EventRecord.escalated_at.is_(None))
                    & (EventRecord.source_type == source_type)
                )
                .outerjoin(IgnoreFingerprintRecord, EventRecord.fingerprint == IgnoreFingerprintRecord.fingerprint)
                .filter(IgnoreFingerprintRecord.ignore_type == IgnoreFingerprintRecord.ESCALATE_MANUALLY)
                .all()
            )
            return events_to_escalate

    def get_interactions_fingerprint(self, fingerprint: str) -> List[IgnoreFingerprintRecord]:
        """Return the list of all interactions associated with a fingerprint.

        Args:
            fingerprint: the fingerprint of the issue
        Returns:
            list: list of IgnoreFingerprintRecord for the specified fingerprint
        """

        with self.session.begin() as session:
            interactions = (
                session.query(IgnoreFingerprintRecord).filter(IgnoreFingerprintRecord.fingerprint == fingerprint).all()
            )
            return [
                {
                    "id": t.id,
                    "fingerprint": t.fingerprint,
                    "ignore_type": t.ignore_type,
                    "reported_at": t.reported_at,
                    "expires_at": t.expires_at,
                }
                for t in interactions
            ]
