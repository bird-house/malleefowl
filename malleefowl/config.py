import os
from pywps import configuration as wpsconfig

import logging
logger = logging.getLogger(__name__)


def wps_url():
    return wpsconfig.get_config_value("server", "url")


def cache_path():
    mypath = wpsconfig.get_config_value("cache", "cache_path")
    if mypath is None or len(mypath) == 0:
        mypath = os.path.join(os.sep, "tmp", "cache")
    if not os.path.exists(mypath):
        try:
            os.makedirs(mypath)
        except OSError:
            pass
    return mypath


def archive_root():
    archive_root = None
    try:
        archive_root = wpsconfig.get_config_value("cache", "archive_root")
    except Exception:
        archive_root = os.environ.get('ESGF_ARCHIVE_ROOT')
        logger.warn("archive root not configured. Using environment variable ESGF_ARCHIVE_ROOT: %s", archive_root)
    if archive_root is None:
        archive_root = []
    else:
        archive_root = [path.strip() for path in archive_root.split(':')]
    return archive_root


def timeout():
    try:
        t = int(getConfigValue("malleefowl", "timeout"))
    except Exception:
        t = 3600
        pass
    return t
