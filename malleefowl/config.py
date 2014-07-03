import os
from pywps import config as wpsconfig

from malleefowl import utils

def getConfigValue(*args):
    return wpsconfig.getConfigValue(*args)

@property
def cache_path(self):
    mypath = os.path.join(getConfigValue("server","outputPath"), "cache")
    utils.mkdir(mypath)
    return mypath

@property
def files_path(self):
    mypath = os.path.join(getConfigValue("server","outputPath"), "files")
    utils.mkdir(path)
    return mypath
