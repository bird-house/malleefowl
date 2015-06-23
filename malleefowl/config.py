import os
from pywps import config as wpsconfig

from malleefowl.exceptions import ConfigurationException

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)

def getConfigValue(*args):
    try:
        value = wpsconfig.getConfigValue(*args)
    except Exception:
        value = None
        logger.exception("Could not get config value for %s", args)
    return value

def wps_url():
    return wpsconfig.getConfigValue("wps", "serveraddress")

def thredds_url():
    return wpsconfig.getConfigValue("malleefowl", "thredds_url")
    
def timeout():
    # default 1 day, in secs, 0 means for ever
    timeout = 86400
    try:
        timeout = int(wpsconfig.getConfigValue("malleefowl", "timeout"))
    except Exception:
        logger.warn("timeout not configured ... using default %s", url)
    return timeout

def cache_path():
    mypath = getConfigValue("cache", "cache_path")
    if mypath is None or len(mypath) == 0:
        mypath = os.path.join(os.sep, "tmp", "cache")
    mkdir(mypath)
    return mypath

def cache_url():
    return getConfigValue("cache", "cache_url")

def mako_cache():
    mako_cache = os.path.join(cache_path(), 'mako')
    mkdir(mako_cache)
    return mako_cache

def archive_root():
    archive_root = None
    try:
        archive_root = wpsconfig.getConfigValue("malleefowl", "archive_root")
    except Exception:
        archive_root = os.environ.get('ESGF_ARCHIVE_ROOT')
        logger.warn("archive root not configured. Using environment variable ESGF_ARCHIVE_ROOT: %s", archive_root)
    if archive_root is None:
        archive_root = []
    else:
        archive_root = [path.strip() for path in archive_root.split(':')]
    return archive_root


