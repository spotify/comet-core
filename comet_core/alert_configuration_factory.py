import json
import os
from collections import namedtuple
import pkg_resources


class AlertConfigurationFactory(object):
    def __init__(self, conf_name, source_type):
        self.conf_name = conf_name
        self.source_type = source_type

    def get_conf(self):
        conf_path = f'alerts_conf_files/{self.source_type}/' + self.conf_name + '.json'
        conf_data = pkg_resources.resource_stream('comet_spotify', conf_path).read()

        d = json.loads(conf_data)
        return namedtuple("AlertConfiguration", d.keys())(*d.values())


def get_event_conf(event):
    conf_name = event.data.get('conf_name')
    alert_conf = AlertConfigurationFactory(conf_name, event.source_type).get_conf()
    return alert_conf
