import os
from pywps import config
from malleefowl import tokenmgr, utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

root_path = config.getConfigValue("malleefowl", "filesPath")

def list_files(token, filter):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
        
    files_path = os.path.join(root_path, userid)
    utils.mkdir(files_path)

    files = [f for f in os.listdir(files_path) if filter in f]

    return files

def get_files(token, file_id):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

    logger.debug('get files for userid=%s' % (userid))
        
    files_path = os.path.join(root_path, userid)
    utils.mkdir(files_path)

    files = [f for f in os.listdir(files_path) if file_id in f]
    file_path = os.path.join(files_path, files[0])

    logger.debug('found file=%s' % (file_path))

    return file_path


