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

"""Interface for plugins"""


class CometInput:
    """Defines the interface for all Input plugins

    Args:
        message_callback (func): callback function that is expeceted to be called once there is message
    """

    def __init__(self, message_callback):
        self.message_callback = message_callback

    def stop(self):
        """Optional function called when Comet is shut down to gracefully shutdown plugins"""
        pass
