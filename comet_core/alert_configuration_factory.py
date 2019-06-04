import json
from collections import namedtuple
import pkg_resources


class AlertConfigurationFactory(object):
    """
    Factory class to create AlertConfiguration object
    """
    def __init__(self, conf_name, source_type):
        self.conf_name = conf_name
        self.source_type = source_type

    def get_conf(self):
        """
        Read conf file stored in comet env and
        return AlertConfiguration object populated with the content inside.
        :return (AlertConfiguration): configuration object
        """
        conf_path = f'alerts_conf_files/{self.source_type}/' + self.conf_name + '.json'
        conf_data = pkg_resources.resource_stream('comet_spotify', conf_path).read()

        d = json.loads(conf_data)
        return namedtuple("AlertConfiguration", d.keys())(*d.values())


def get_event_conf(event):
    """
    Return event conf object to the event only if
    it has a conf_name in the message.
    configuration files work for real time events.
    :return (AlertConfiguration): configuration object for the event
    """
    conf_name = event.data.get('conf_name')
    if conf_name:
        conf = AlertConfigurationFactory(conf_name, event.source_type).get_conf()
        return conf
    return None
