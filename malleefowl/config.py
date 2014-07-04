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

def mongodb_url():
    url = "mongodb://localhost"
    try:
        url = wpsconfig.getConfigValue("malleefowl", "mongodbUrl")
    except Exception:
        logger.exception("mongodbUrl not configured ... using default %s", url)
    return url

def thredds_url():
    url = "http://localhost:8080/thredds"
    try:
        url = wpsconfig.getConfigValue("malleefowl", "threddsUrl")
    except Exception:
        logger.exception("threddsUrl not configured ... using default %s", url)
    return url

def timeout():
    # default 1 day, in secs, 0 means for ever
    timeout = 86400
    try:
        timeout = int(wpsconfig.getConfigValue("malleefowl", "timeout"))
    except Exception:
        logger.exception("timeout not configured ... using default %s", url)
    return timeout

def sys_token():
    token = change_me_in_custom.cfg
    try:
        token = wpsconfig.getConfigValue("malleefowl", "sysToken")
    except Exception:
        logger.exception("sysToken not configured ... using default %s", url)
    return token
    
def cache_path():
    mypath = os.path.join(getConfigValue("server","outputPath"), "cache")
    utils.mkdir(mypath)
    return mypath

def files_path():
    mypath = os.path.join(getConfigValue("server","outputPath"), "files")
    utils.mkdir(mypath)
    return mypath
