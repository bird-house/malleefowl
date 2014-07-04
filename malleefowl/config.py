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
        logger.exception("mongodburl not configured ... using default %s", url)
    return url

@property
def cache_path():
    mypath = os.path.join(getConfigValue("server","outputPath"), "cache")
    utils.mkdir(mypath)
    return mypath

@property
def files_path():
    mypath = os.path.join(getConfigValue("server","outputPath"), "files")
    utils.mkdir(path)
    return mypath
