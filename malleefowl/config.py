import os
from pywps import config as wpsconfig

from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def getConfigValue(*args):
    value = ''
    try:
        value = wpsconfig.getConfigValue(*args)
    except Exception:
        logger.exception("Could not get config value for %s", args)
    return value

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
    if len(mypath) == 0:
        mypath = os.path.join(os.sep, "tmp", "cache")
    utils.mkdir(mypath)
    return mypath

def cache_url():
    return getConfigValue("cache", "cache_url")

def archive_root():
    archive_root = None
    try:
        archive_root = wpsconfig.getConfigValue("malleefowl", "archive_root")
        archive_root = [path.strip() for path in archive_root.split(',')]
    except Exception:
        archive_root = []
        logger.warn("archive root not configured")
    return archive_root

def mako_cache():
    mako_cache = getConfigValue("malleefowl", "mako_cache")
    utils.mkdir(mako_cache)
    return mako_cache

