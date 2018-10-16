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

Used by System-Z"""

import logging

from datetime import timedelta, datetime

from flask import Blueprint, g, jsonify, request

from comet_core.api_helper import hydrate_open_issues, get_db, requires_auth
from comet_core.model import IgnoreFingerprintRecord


bp = Blueprint('v0', __name__, url_prefix='/v0')  # pylint: disable=invalid-name
LOG = logging.getLogger(__name__)


def ok():  # pylint: disable=invalid-name
    """Generate an "ok" response

    Returns:
        Response: a 200 OK response with JSON payload
    """
    return jsonify({'status': 'ok'})


@bp.route('/acceptrisk', methods=('POST',))
@requires_auth
def acceptrisk():
    """Accept risk for the given fingerprint

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = request.get_json()['fingerprint']
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.ACCEPT_RISK)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on acceptrisk')
        return jsonify({'status': 'error', 'msg': 'acceptrisk failed'}), 500

    return ok()


@bp.route('/snooze', methods=('POST',))
@requires_auth
def snooze():
    """snooze the given fingerprint

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = request.get_json()['fingerprint']
        expires_at = datetime.utcnow() + timedelta(days=30)
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.SNOOZE, expires_at=expires_at)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on snooze')
        return jsonify({'status': 'error', 'msg': 'snooze failed'}), 500

    return ok()


@bp.route('/falsepositive', methods=('POST',))
@requires_auth
def falsepositive():
    """Mark the given fingerprint as falsepositive

    Returns:
        str: the HTTP response string
    """
    try:
        fingerprint = request.get_json()['fingerprint']
        get_db().ignore_event_fingerprint(fingerprint, IgnoreFingerprintRecord.FALSE_POSITIVE)
    except Exception as _:  # pylint: disable=broad-except
        LOG.exception('Got exception on falsepositive')
        return jsonify({'status': 'error', 'msg': 'falsepositive failed'}), 500

    return ok()


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
