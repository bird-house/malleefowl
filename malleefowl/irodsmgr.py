from irods import *

from pywps import config
from malleefowl import tokenmgr, utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def list_files(token, filter, collection):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

    logger.debug('userid=%s, filter=%s, collection=%s' % (userid, filter, collection))

    status, myEnv = getRodsEnv()
    conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
    status = clientLogin(conn)

    # Open the current working directory
    col = irodsCollection(conn)
    col.openCollection(collection)

    logger.debug('collection name=%s, num objs=%s', col.getCollName(), col.getLenObjects())
    
    objects = col.getObjects()
    conn.disconnect()

    files = [obj[0] for obj in objects]
    return files
