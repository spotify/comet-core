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

"""Comet Legacy API.

Used for compatability with email buttons."""

from datetime import timedelta, datetime

from flask import Blueprint, escape

from comet_core.api_helper import get_db
from comet_core.model import IgnoreFingerprintRecord

bp = Blueprint('legacy', __name__, url_prefix='/')  # pylint: disable=invalid-name


@bp.route('/acceptrisk/<fingerprint>')
def acceptrisk(fingerprint):
    """Accept risk for the given fingerprint

    Args:
        fingerprint (str): the fingerprint to accept risk for

    Returns:
        str: the HTTP response string
    """
    get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.ACCEPT_RISK)
    return f'The issue was labeled as an accepted risk. You will not receive any' + \
           f' more notifications about it. (fp {escape(fingerprint)})'


@bp.route('/snooze/<fingerprint>')
def snooze(fingerprint):
    """snooze the given fingerprint

    Args:
        fingerprint (str): the fingerprint to snooze

    Returns:
        str: the HTTP response string
    """
    expires_at = datetime.utcnow() + timedelta(days=30)
    get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.SNOOZE, expires_at=expires_at)
    return f'The issue was snoozed. You will not receive any more notifications for 30 days! (fp {escape(fingerprint)})'


@bp.route('/falsepositive/<fingerprint>')
def falsepositive(fingerprint):
    """Mark the given fingerprint as falsepositive
    Args:
        fingerprint (str): the fingerprint to mark as falsepositive

    Returns:
        str: the HTTP response string
    """
    get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.FALSE_POSITIVE)
    return f'The issue was reported as false positive. You will not receive any more notifications about it. ' + \
           f'Please reach out to #security if you have anything to tell us. (fp {escape(fingerprint)})'
