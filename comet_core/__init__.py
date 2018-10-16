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

"""comet - Comet Distributed Security Notification System"""

__author__ = 'Spotify Security Wasabi team <wasabi@spotify.com>'
__all__ = ['Comet']

import logging
from pythonjsonlogger import jsonlogger

from comet_core.app import Comet

# pylint: disable=invalid-name

root_logger = logging.getLogger()

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s",
                                                     "%Y-%m-%dT%H:%M:%S"))
root_logger.addHandler(stderr_handler)
