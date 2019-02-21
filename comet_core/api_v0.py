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

from comet_core.api_helper import hydrate_open_issues, get_db, requires_auth, assert_valid_token
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


def get_and_check_fingerprint():
    """Reads the fingerprint from a POST request json data if it
    was a POST request, also checks its syntax.
    Also validate the token passed in the request

    Raises:
        ValueError: if the POST request did not contain json data,
        or if the json data did not contain a fingerprint

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

    if request.method == 'GET':
        if 'fp' not in request.args:
            raise ValueError('No fingerprint parameter in URL.')
        if 't' not in request.args:
            raise ValueError('No token parameter in URL.')

        fingerprint = request.args['fp']
        token = request.args['t']

        assert_fingerprint_syntax(fingerprint)

        assert_valid_token(fingerprint, token)

    return fingerprint


def acceptrisk():
    """Accept risk for alerts with the given fingerprint (silence them).

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint()
        get_db().ignore_event_fingerprint(fingerprint,
                                          IgnoreFingerprintRecord.ACCEPT_RISK)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on acceptrisk')
        return action_failed('acceptrisk failed')

    return action_succeeded('Alert successfully marked as accept risk.')


def snooze():
    """Snooze alerts with the given fingerprint for 30 days (silence them for 30 days).

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint()
        expires_at = datetime.utcnow() + timedelta(days=30)
        get_db().ignore_event_fingerprint(fingerprint,
                                          IgnoreFingerprintRecord.SNOOZE,
                                          expires_at=expires_at)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on snooze')
        return action_failed('snooze failed')

    return action_succeeded('Alert successfully snoozed.')


def acknowledge():
    """Mark the alert with the given fingerprint as acknowledged (applies to real-time alerts only).

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint()
        get_db().ignore_event_fingerprint(fingerprint,
                                          IgnoreFingerprintRecord.ACKNOWLEDGE)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on acknowledge')
        return action_failed('acknowledgement failed for some reason')

    return action_succeeded('Thanks for acknowledging!')


def falsepositive():
    """Mark alerts with the given fingerprint as falsepositive (silence them).

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint()
        get_db().ignore_event_fingerprint(fingerprint,
                                          IgnoreFingerprintRecord.FALSE_POSITIVE)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on falsepositive')
        return action_failed('Reporting as false positive failed.')

    return action_succeeded('Thanks! We’ve marked this as a false positive')


def escalate():
    """Mark the given fingerprint as manually escalated (applied to real-time alerts only).

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = get_and_check_fingerprint()
        # indication that the user addressed the alert and escalate.
        get_db().ignore_event_fingerprint(fingerprint,
                                          IgnoreFingerprintRecord.ESCALATE_MANUALLY)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on escalate real time alert')
        return action_failed('Escalation failed for some reason')

    return action_succeeded('Thanks! This alert has been escalated.')


# API ENDPOINTS


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


@bp.route('/acceptrisk', methods=['GET'])
def acceptrisk_get():
    """This endpoint expose the acceptrisk functionality via GET request.
    Doesn't required authentication because we are
    handling the auth by validating the token passed in the request.
    For details on the acceptrisk function see :func:`~comet_core.api_v0.acceptrisk`

    Returns:
        str: The response from the acceptrisk function
    """
    return acceptrisk()


@bp.route('/acceptrisk', methods=['POST'])
@requires_auth
def acceptrisk_post():
    """This endpoint expose the acceptrisk functionality via POST request.
    For details on the acceptrisk function see :func:`~comet_core.api_v0.acceptrisk`

    Returns:
        str: The response from the acceptrisk function
    """
    return acceptrisk()


@bp.route('/snooze', methods=['GET'])
def snooze_get():
    """This endpoint expose the snooze functionality via GET request.
    Doesn't required authentication because we are
    handling the auth by validating the token passed in the request.
    For details on the snooze function see :func:`~comet_core.api_v0.snooze`

    Returns:
        str: The response from the snooze function
    """
    return snooze()


@bp.route('/snooze', methods=['POST'])
@requires_auth
def snooze_post():
    """This endpoint expose the snooze functionality via POST request.
    For details on the snooze function see :func:`~comet_core.api_v0.snooze`

    Returns:
        str: The response from the snooze function
    """
    return snooze()


@bp.route('/falsepositive', methods=['GET'])
def falsepositive_get():
    """This endpoint expose the falsepositive functionality via GET request.
    Doesn't required authentication because we are
    handling the auth by validating the token passed in the request.
    For details on the falsepositive function see :func:`~comet_core.api_v0.falsepositive`

    Returns:
        str: The response from the falsepositive function
    """
    return falsepositive()


@bp.route('/falsepositive', methods=['POST'])
@requires_auth
def falsepositive_post():
    """This endpoint expose the falsepositive functionality via POST request.
    For details on the falsepositive function see :func:`~comet_core.api_v0.falsepositive`

    Returns:
        str: The response from the falsepositive function
    """
    return falsepositive()


@bp.route('/acknowledge', methods=['GET'])
def acknowledge_get():
    """This endpoint expose the acknowledge functionality via GET request.
    Doesn't required authentication because we are
    handling the auth by validating the token passed in the request.
    For details on the acknowledge function see :func:`~comet_core.api_v0.acknowledge`

    Returns:
        str: The response from the acknowledge function
    """
    return acknowledge()


@bp.route('/acknowledge', methods=['POST'])
@requires_auth
def acknowledge_post():
    """This endpoint expose the acknowledge functionality via POST request.
    For details on the acknowledge function see :func:`~comet_core.api_v0.acknowledge`

    Returns:
        str: The response from the acknowledge function
    """
    return acknowledge()


@bp.route('/escalate', methods=['GET'])
def escalate_get():
    """This endpoint expose the escalate functionality via GET request.
    Doesn't required authentication because we are
    handling the auth by validating the token passed in the request.
    For details on the escalate function see :func:`~comet_core.api_v0.escalate`

    Returns:
        str: The response from the escalate function
    """
    return escalate()


@bp.route('/escalate', methods=['POST'])
@requires_auth
def escalate_post():
    """This endpoint expose the escalate functionality via POST request.
    For details on the escalate function see :func:`~comet_core.api_v0.escalate`

    Returns:
        str: The response from the escalate function
    """
    return escalate()


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
