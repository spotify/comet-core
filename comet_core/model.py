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

from sqlalchemy import Column, DateTime, Integer, JSON, String, UnicodeText, types
from sqlalchemy.ext.declarative import declarative_base


class BaseRecordRepr(object):
    """
    This class can be used by declarative_base, to add an automatic
    __repr__ method to *all* subclasses of BaseRecord.
    """
    def __repr__(self):
        """Return a representation of this object as a string.

        Returns:
            str: a representation of the object.
        """
        return f'{self.__class__.__name__}: ' + \
               ' '.join(
                   [f'{k}={self.__getattribute__(k)}'
                    for k, v in self.__class__.__dict__.items()
                    if hasattr(v, '__set__')])


BaseRecord = declarative_base(cls=BaseRecordRepr)  # pylint: disable=invalid-name


class JSONType(types.TypeDecorator):  # pylint: disable=abstract-method
    """This is for testing purposes, to make the JSON type work with sqlite."""
    impl = UnicodeText

    def load_dialect_impl(self, dialect):
        """This is an end-user override hook that can be used to provide
        differing types depending on the given dialect.

        Args:
            dialect (object): SQLAlchemy dialect object
        Returns:
            object: if dialect name is 'mysql' it will override the type descriptor to JSON()
        """
        if dialect.name == 'mysql':
            return dialect.type_descriptor(JSON())
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
        if dialect.name == 'mysql':
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
        if dialect.name == 'mysql':
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
    __tablename__ = 'event'
    id = Column(Integer, primary_key=True)
    source_type = Column(String(250), nullable=False)
    fingerprint = Column(String(250))
    owner = Column(String(250))
    event_metadata = Column(JSONType())
    data = Column(JSONType())

    received_at = Column(DateTime, default=datetime.utcnow)
    sent_at = Column(DateTime, default=None)
    escalated_at = Column(DateTime, default=None)
    processed_at = Column(DateTime, default=None)

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


class IgnoreFingerprintRecord(BaseRecord):
    """Acceptedrisk model.
    """
    __tablename__ = 'ignore_fingerprint'
    id = Column(Integer, primary_key=True)
    fingerprint = Column(String(250))
    ignore_type = Column(String(50))
    reported_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, default=None)

    SNOOZE = 'snooze'
    ACCEPT_RISK = 'acceptrisk'
    FALSE_POSITIVE = 'falsepositive'
    ACKNOWLEDGE = 'acknowledge'
    ESCALATE_MANUALLY = 'escalate_manually'
