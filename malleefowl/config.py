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
        logger.exception("Could not get config value for")
    return value

def thredds_url():
    url = "http://localhost:8080/thredds"
    try:
        url = wpsconfig.getConfigValue("malleefowl", "threddsUrl")
    except Exception:
        logger.warn("threddsUrl not configured ... using default %s", url)
    return url

def timeout():
    # default 1 day, in secs, 0 means for ever
    timeout = 86400
    try:
        timeout = int(wpsconfig.getConfigValue("malleefowl", "timeout"))
    except Exception:
        logger.warn("timeout not configured ... using default %s", url)
    return timeout

def cache_path():
    mypath = os.path.join(getConfigValue("server","outputPath"), "cache")
    utils.mkdir(mypath)
    return mypath

def mako_cache():
    mako_cache = getConfigValue("malleefowl", "mako_cache")
    utils.mkdir(mako_cache)
    return mako_cache

