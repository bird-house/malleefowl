from irods import *

from pywps import config
from malleefowl import tokenmgr, utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def list_files(token, filter, collection):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

    logger.debug('userid=%s, filter=%s, collection=%s' % (userid, filter, collection))

    import os
    logger.debug("home=%s", os.environ['HOME'])

    status, my_env = getRodsEnv()
    logger.debug('host=%s', my_env.rodsHost)
    conn, errMsg = rcConnect(my_env.rodsHost, my_env.rodsPort, my_env.rodsUserName, my_env.rodsZone)
    
    logger.debug("got connection, errMsg=%s", err_msg)
    status = clientLogin(conn)

    logger.debug("client login, status=%s", status)

    # Open the current working directory
    col = irodsCollection(conn)
    col.openCollection(collection)

    logger.debug('collection name=%s, num objs=%s', col.getCollName(), col.getLenObjects())
    
    objects = col.getObjects()
    conn.disconnect()

    files = [obj[0] for obj in objects]
    return files
