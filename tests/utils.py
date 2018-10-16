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

"""Utilities and Mocked Data for tests"""

from comet_core.app import EventContainer


def get_all_test_messages(parsed=False):
    """Get all test messages and their filenames as an iterator.

    Args:
        parsed (bool): returns Event objects if true otherwise strings
    Yields:
        EventContainer: some test event
    """
    event = EventContainer('test', {})
    event.set_owner('test@acme.org')
    event.set_fingerprint('test')
    return [event]
