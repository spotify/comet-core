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

"""Alert Configuration Factory class"""

import json
from collections import namedtuple


class AlertConfigurationFactory(object):
    """
    Factory class to create AlertConfiguration object

    Args:
        alerts_conf_path (str): where all the alerts conf files live.
        source_type (str): the source type folder name
        to construct the path to the conf file.
        subtype (str): subtype of the alert to construct the path to the conf file.
    """
    def __init__(self, alerts_conf_path, source_type, subtype):
        self.alerts_conf_path = alerts_conf_path
        self.source_type = source_type
        self.subtype = subtype

    def get_conf(self):
        """
        Read conf file stored in comet env and
        return AlertConfiguration object populated with the content inside.

        Returns:
            AlertConfiguration: configuration object
        """
        conf_path = f'{self.alerts_conf_path}/{self.source_type}/' + self.subtype + '.json'

        with open(conf_path, 'r') as f:
            conf_data = f.read()
            data = json.loads(conf_data)
            data['source_type'] = self.source_type

        return namedtuple("AlertConfiguration", data.keys())(*data.values())


def get_event_conf(alerts_conf_path, event):
    """
    Return event conf object to the event only if
    it has a conf_name in the message.
    configuration files work for real time events.

    Args:
        alerts_conf_path (str): the path where all the alerts conf files live.
        event (EventRecord): the event to extract the conf file name from.
    Returns:
        AlertConfiguration: configuration object for the event
    """
    subtype = event.data.get('subtype')
    if subtype:
        conf = AlertConfigurationFactory(alerts_conf_path, event.source_type, subtype).get_conf()
        return conf
    return None
