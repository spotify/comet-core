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

"""Model module - hosting database models."""
import json
from datetime import datetime

import sqlalchemy
import sqlalchemy.orm

BaseRecord = sqlalchemy.orm.declarative_base()


class JSONType(sqlalchemy.types.TypeDecorator):  # pylint: disable=abstract-method
    """This is for testing purposes, to make the JSON type work with sqlite."""

    impl = sqlalchemy.UnicodeText

    cache_ok = True

    def load_dialect_impl(self, dialect):
        """This is an end-user override hook that can be used to provide
        differing types depending on the given dialect.

        Args:
            dialect (object): SQLAlchemy dialect object
        Returns:
            object: if dialect name is 'mysql' it will override the type descriptor to JSON()
        """
        if dialect.name == "mysql":
            return dialect.type_descriptor(sqlalchemy.JSON())
        return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value, dialect):
        """This is an end-user override hook that is used to support JSON conversion is sqlite
        when binding parameters.

        Args:
            value (object): a JSON dumpable object
            dialect (object): the dialect object
        Returns:
            json: the processed value
        """
        if dialect.name == "mysql":
            return value
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        """This is an end-user override hook that is used to support JSON conversion is sqlite
        when processing retrieved values.

        Args:
            value (json): a JSON dumpable object
            dialect (object): the dialect object
        Returns:
            str: the processed value
        """
        if dialect.name == "mysql":
            return value
        if value is not None:
            value = json.loads(value)
        return value


class EventRecord(BaseRecord):
    """Event model.
    Args:
        args (list) : arguments list passed to the BaseRecord constructor
        kwargs (dict) : arguments dict passed to the BaseRecord constructor
    """

    __tablename__ = "event"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    source_type = sqlalchemy.Column(sqlalchemy.String(250), nullable=False)
    fingerprint = sqlalchemy.Column(sqlalchemy.String(250))
    owner = sqlalchemy.Column(sqlalchemy.String(250))
    event_metadata = sqlalchemy.Column(JSONType())
    data = sqlalchemy.Column(JSONType())

    received_at = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    sent_at = sqlalchemy.Column(sqlalchemy.DateTime, default=None)
    escalated_at = sqlalchemy.Column(sqlalchemy.DateTime, default=None)
    processed_at = sqlalchemy.Column(sqlalchemy.DateTime, default=None)

    def __init__(self, *args, **kwargs):
        self.new = False
        self.owner_email_overridden = False
        super().__init__(*args, **kwargs)

    def update_metadata(self, metadata):
        """Update optional metadata for the event.

        Args:
            metadata (dict): arbitrary metadata for the event
        """
        if self.event_metadata:
            self.event_metadata.update(metadata)
        else:
            self.event_metadata = metadata

    def __repr__(self):
        return f"EventRecord(id={self.id!r}, source_type={self.source_type!r}, fingerprint={self.fingerprint!r})"


class IgnoreFingerprintRecord(BaseRecord):
    """Acceptedrisk model."""

    __tablename__ = "ignore_fingerprint"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    fingerprint = sqlalchemy.Column(sqlalchemy.String(250))
    ignore_type = sqlalchemy.Column(sqlalchemy.String(50))
    reported_at = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.utcnow)
    expires_at = sqlalchemy.Column(sqlalchemy.DateTime, default=None)
    record_metadata = sqlalchemy.Column(JSONType())

    SNOOZE = "snooze"
    ACCEPT_RISK = "acceptrisk"
    FALSE_POSITIVE = "falsepositive"
    ACKNOWLEDGE = "acknowledge"
    ESCALATE_MANUALLY = "escalate_manually"
    RESOLVED = "resolved"

    def __repr__(self):
        return f"IgnoreFingerPrintRecord(id={self.id!r}, fingerprint={self.fingerprint!r}, ignore_type={self.ignore_type!r})"
