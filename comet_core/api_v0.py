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

"""Comet v0 API.

Can be used by GET response links in email messages or form POST requests from a web ui."""

import logging
import re
from datetime import timedelta, datetime

from flask import Blueprint, g, jsonify, request, render_template_string

from comet_core.api_helper import hydrate_open_issues, get_db, \
    requires_auth
from comet_core.model import IgnoreFingerprintRecord

bp = Blueprint('v0', __name__, url_prefix='/v0')  # pylint: disable=invalid-name
LOG = logging.getLogger(__name__)


def action_succeeded(message=None, status_code=200):
    """Generate html (for GET request) or json (for POST requests) response with a custom success message.

    Args:
        message (str): custom success message
        status_code (int): the http status code to return
    Returns:
        Tuple[Union[str,flask.Response],int]: rendered html code or json Response object and http code
                                              with a success message
    """
    if request.method == 'POST':
        response = {'status': 'ok'}
        if message:
            response['msg'] = message
        return jsonify(response), status_code

    template = '<h2>{{ message }}</h2> ' \
               '<p>Note: This feature is still early in development, ' \
               'please reach out to Security if you have any feedback.</p>'
    return render_template_string(template, message=message), status_code


def action_failed(message=None, status_code=500):
    """Generate html (for GET request) or json (for POST requests) response with a custom failure message.

    Args:
        message (str): custom failure message
        status_code (int): the http status code to return
    Returns:
        Tuple[Union[str,flask.Response],int]: rendered html code or json Response object and http code
                                              with an error message
    """
    if request.method == 'POST':
        response = {'status': 'error'}
        if message:
            response['message'] = message
        return jsonify(response), status_code

    template = '<h2>Something went wrong: {{ message }}</h2> ' \
               '<p>Please complete the action by emailing to Security.</p>' \
               '<p>Note: This feature is still early in development, ' \
               'please reach out to Security if you have any feedback.</p>'
    return render_template_string(template, message=message), status_code


def assert_fingerprint_syntax(fingerprint):
    """Checks if a fingerprint string looks valid and raises exceptions otherwise.

    Checks if it is of valid length and only contains valid characters.

    Args:
        fingerprint (str): the fingerprint string to check
    Raises:
        ValueError: if the fingerprint is empty, too long, too short or contains invalid characters
    """
    if not fingerprint:
        raise ValueError('fingerprint invalid: None/empty')

    if len(fingerprint) < 8:
        raise ValueError('fingerprint invalid: shorter than 8 characters')
    if len(fingerprint) > 1024:
        raise ValueError('fingerprint invalid: longer than 1024 characters')

    pattern = re.compile('[a-zA-Z0-9._-]*')
    if not pattern.fullmatch(fingerprint):
        raise ValueError('fingerprint invalid: contains invalid characters')


def get_and_check_fingerprint(fingerprint):
    """Reads the fingerprint from a POST request json data if it was a POST request, also checks its syntax.

    Args:
        fingerprint (str): the fingerprint from a get request (will be returned if it was no POST request)
    Raises:
        ValueError: if the POST request did not contain json data, or if the json data did not contain a fingerprint
    Returns:
        str: fingerprint
    """
    if request.method == 'POST':
        request_json = request.get_json()
        if not request_json:
            raise ValueError('No json data in post request.')
        if 'fingerprint' not in request_json:
            raise ValueError('No fingerprint parameter in json data.')
        fingerprint = request_json['fingerprint']

    assert_fingerprint_syntax(fingerprint)

    return fingerprint


@bp.route('/')
def health_check():
    """Can be called by e.g. Kubernetes to verify that the API is up

     Returns:
        str: the static string "Comet-API", could be anything
    """
    return 'Comet-API-v0'


@bp.route('/dbcheck')
def dbhealth_check():
    """Can be called by e.g. Kubernetes to verify that the API is up and is able to query DB

     Returns:
        str: the static string "Comet-API", could be anything
    """
    try:
        get_db().get_latest_event_with_fingerprint('xxx')
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on dbhealth_check')
        return jsonify({'status': 'error', 'msg': 'dbhealth_check failed'}), 500

    return 'Comet-API-v0'


@bp.route('/acceptrisk/<path:fingerprint>')
@bp.route('/acceptrisk', methods=['POST'])
@requires_auth
def acceptrisk(fingerprint=None):
    """Accept risk for alerts with the given fingerprint (silence them).

    Args:
        fingerprint (str): the fingerprint to mark as acceptrisk
    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint(fingerprint)
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.ACCEPT_RISK)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on acceptrisk')
        return action_failed('acceptrisk failed')

    return action_succeeded('Alert successfully marked as accept risk.')


@bp.route('/snooze/<path:fingerprint>')
@bp.route('/snooze', methods=['POST'])
@requires_auth
def snooze(fingerprint=None):
    """Snooze alerts with the given fingerprint for 30 days (silence them for 30 days).

    Args:
        fingerprint (str): the fingerprint to  snooze
    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint(fingerprint)
        expires_at = datetime.utcnow() + timedelta(days=30)
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.SNOOZE, expires_at=expires_at)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on snooze')
        return action_failed('snooze failed')

    return action_succeeded('Alert successfully snoozed.')


@bp.route('/falsepositive/<path:fingerprint>')
@bp.route('/falsepositive', methods=['POST'])
@requires_auth
def falsepositive(fingerprint=None):
    """Mark alerts with the given fingerprint as falsepositive (silence them).

    Args:
        fingerprint (str): the fingerprint to mark as falsepositive

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint(fingerprint)
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.FALSE_POSITIVE)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on falsepositive')
        return action_failed('Reporting as false positive failed.')

    return action_succeeded('Thanks! We’ve marked this as a false positive')


@bp.route('/acknowledge/<path:fingerprint>')
@bp.route('/acknowledge', methods=['POST'])
@requires_auth
def acknowledge(fingerprint=None):
    """Mark the alert with the given fingerprint as acknowledged (applies to real-time alerts only).

    Args:
        fingerprint (str): the fingerprint to acknowledge

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint(fingerprint)
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.ACKNOWLEDGE)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on acknowledge')
        return action_failed('acknowledgement failed for some reason')

    return action_succeeded('Thanks for acknowledging!')


@bp.route('/escalate/<path:fingerprint>')
@bp.route('/escalate', methods=['POST'])
@requires_auth
def escalate(fingerprint=None):
    """Mark the given fingerprint as manually escalated (applied to real-time alerts only).

    Args:
        fingerprint (str): the fingerprint to mark as escalated

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint(fingerprint)
        # indication that the user addressed the alert and escalate.
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.ESCALATE_MANUALLY)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on escalate real time alert')
        return action_failed('Escalation failed for some reason')

    return action_succeeded('Thanks! This alert has been escalated.')


@bp.route('/issues')
@requires_auth
def get_issues():
    """Return a list of issues for the user that authenticated.

    Returns:
        str: json list, containing one json dictionary for each issue
    """
    try:
        raw_issues = get_db().get_open_issues(g.authorized_for)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on get_issues.get_db().get_open_issues')
        return jsonify({'status': 'error', 'msg': 'get_open_issues failed'}), 500

    try:
        hydrated_issues = hydrate_open_issues(raw_issues)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on get_issues.hydrate_open_issues')
        return jsonify({'status': 'error', 'msg': 'hydrate_open_issues failed'}), 500

    return jsonify(hydrated_issues)
