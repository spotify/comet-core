import json
from collections import namedtuple
import pkg_resources


class AlertConfigurationFactory(object):
    """
    Factory class to create AlertConfiguration object

    Args:
        conf_file_name (str): the configuration file name
        source_type (str): the source type folder name
        to construct the path to the conf file.
    """
    def __init__(self, conf_file_name, source_type):
        self.conf_file_name = conf_file_name
        self.source_type = source_type

    def get_conf(self):
        """
        Read conf file stored in comet env and
        return AlertConfiguration object populated with the content inside.

        Returns:
            AlertConfiguration: configuration object
        """
        conf_path = f'alerts_conf_files/{self.source_type}/' + self.conf_file_name + '.json'
        conf_data = pkg_resources.resource_stream('comet_spotify', conf_path).read()

        data = json.loads(conf_data)
        return namedtuple("AlertConfiguration", data.keys())(*data.values())


def get_event_conf(event):
    """
    Return event conf object to the event only if
    it has a conf_name in the message.
    configuration files work for real time events.

    Args:
        event (EventRecord): the event to extract the conf file name from.
    Returns:
        AlertConfiguration: configuration object for the event
    """
    conf_name = event.data.get('conf_name')
    if conf_name:
        conf = AlertConfigurationFactory(conf_name, event.source_type).get_conf()
        return conf
    return None
