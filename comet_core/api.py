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

"""Comet API."""

import logging

from flask import Flask
from flask_cors import CORS

from comet_core import api_v0

LOG = logging.getLogger(__name__)


class CometApi():
    """The Comet API

    Args:
        cors_origins (List[str]): the CORS origins to allow
        database_uri (str): the database URI to use
        host (str): the IP to bind to
        port (int): the port to bind to
        hmac_secret (str): the server secret to use in GET requests auth
    """

    def __init__(self,
                 cors_origins=None,
                 database_uri='sqlite://',
                 host='0.0.0.0',
                 port=5000,
                 hmac_secret=''):
        self.cors_origins = cors_origins if cors_origins is not None else []
        self.database_uri = database_uri
        self.host = host
        self.port = port
        self.auth_func = None
        self.hydrator_func = None
        self.hmac_secret = hmac_secret

    def register_auth(self):
        """Used as a decorator to register an auth function

        Returns:
            function: the original function
        """

        # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
        def decorator(func):
            self.auth_func = func
            return func

        return decorator

    def register_hydrator(self):
        """Used as a decorator to register an hydrator function

        Returns:
            function: the original function
        """

        # pylint: disable=missing-docstring, missing-return-doc, missing-return-type-doc
        def decorator(func):
            self.hydrator_func = func
            return func

        return decorator

    def create_app(self):
        """Create a Flask app from the configured instance

        Returns:
            Flask: the Flask (uwsgi) app
        """
        app = Flask(__name__)

        app.config['auth_func'] = self.auth_func
        app.config['hydrator_func'] = self.hydrator_func
        app.config['database_uri'] = self.database_uri
        app.config['hmac_secret'] = self.hmac_secret

        cors = CORS()
        cors.init_app(app, resources={
            r'/*': {
                'origins': self.cors_origins,
                'supports_credentials': True
            }
        })

        app.register_blueprint(api_v0.bp)

        @app.route('/')
        def health_check():  # pylint: disable=unused-variable
            """Can be called by e.g. Kubernetes to verify that the API is up

            Returns:
                str: the static string "Comet-API", could be anything
            """
            return 'Comet-API'

        return app

    def run(self, **kwargs):
        """Run the API, can be used for debugging.

        We recommend to use a uwsgi server in front by calling create_app for production.

        Args:
            **kwargs (dict): optional configuration to the Flask `run` method.
        """
        app = self.create_app()

        app.run(host=self.host, port=self.port, **kwargs)
