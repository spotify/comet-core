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

"""API helpers"""
import hmac
import logging
from functools import wraps

from flask import g, Response, current_app

from comet_core.data_store import DataStore
from comet_core.fingerprint import fingerprint_hmac

LOG = logging.getLogger(__name__)


def hydrate_open_issues(raw_issues):
    """Return a list of hydrated issues (json dicts) for the given raw issues.
    Each `EventRecord` in the `raw_issues` is hydrated with more readable fields using the templates defined in
    the plugin configs.
    Args:
        raw_issues (list): list of `EventRecord`s to hydrate
    Returns:
        str: json list, containing one json dictionary for each issue
    """
    hydrator_func = current_app.config.get('hydrator_func')
    if hydrator_func:
        return hydrator_func(raw_issues)
    LOG.warning('No API hydrator registered!')
    return False


def get_db():
    """Get or initialize the request-scoped datastore instance

    Returns:
        DataStore: a request-scoped datastore instance
    """
    if 'db' not in g:
        g.db = DataStore(current_app.config.get('database_uri'))
    return g.db


# pylint: disable=missing-return-doc,missing-return-type-doc,missing-param-doc,missing-type-doc
def requires_auth(f):
    """Decorator for requiring auth in functions"""

    @wraps(f)
    # pylint: disable=missing-docstring,missing-return-doc,missing-return-type-doc
    def decorated(*args, **kwargs):
        auth_func = current_app.config.get('auth_func')
        g.authorized_for = []
        if auth_func:
            res = auth_func()
            if isinstance(res, Response):
                return res
            g.authorized_for = res
            return f(*args, **kwargs)
        LOG.warning('no auth function specified')
        return f(*args, **kwargs)

    return decorated


def assert_valid_token(fingerprint, token):
    """
    Check if the token given in the request is valid by comparing to the calculated API token.

    Args:
        fingerprint (str): the fingerprint to compute the API token with
        token (str): the token to validate

    Raises:
        ValueError: if the token is not valid
    """
    expected_token = fingerprint_hmac(fingerprint, current_app.config['hmac_secret'])
    if not hmac.compare_digest(bytes(expected_token, 'utf-8'), bytes(token, 'utf-8')):
        raise ValueError('Invalid token for the given fingerprint.')
