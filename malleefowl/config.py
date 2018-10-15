import os
import tempfile
from pywps import configuration

import logging
LOGGER = logging.getLogger("PYWPS")

DEFAULT_NODE = 'default'
DKRZ_NODE = 'dkrz'
IPSL_NODE = 'ipsl'


def wps_url():
    return configuration.get_config_value("server", "url")


def cache_path():
    cache_path = configuration.get_config_value("data", "cache_path")
    if not cache_path:
        LOGGER.warn("No cache path configured. Using default value.")
        cache_path = os.path.join(configuration.get_config_value("server", "outputpath"), "cache")
    return cache_path


def archive_root():
    value = configuration.get_config_value("data", "archive_root")
    if value:
        path_list = [path.strip() for path in value.split(':')]
        LOGGER.debug("using archive root %s", path_list)
    else:
        path_list = []
    return path_list


def archive_node():
    node = configuration.get_config_value("data", "archive_node")
    node = node or 'default'
    node = node.lower()
    return node
