from irods import *

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def list_files(collection):
    logger.debug('irods collection=%s' % (collection))

    import os
    logger.debug("home=%s", os.environ['HOME'])

    status, my_env = getRodsEnv()
    logger.debug('host=%s', my_env.rodsHost)
    conn, err_msg = rcConnect(my_env.rodsHost, my_env.rodsPort, my_env.rodsUserName, my_env.rodsZone)
    
    logger.debug("got connection, error msg=%s", err_msg)
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

def rsync(src, dest):
    logger.debug('rsync src=%s dest=%s', src, dest)

    from subprocess import check_output, STDOUT, CalledProcessError
    try:
        check_output(["irsync", "-r", src, dest], stderr=STDOUT)
    except CalledProcessError as e:
        logger.error('irods rsync failed. Ouput=%s', e.output )
        raise
    except Exception as e:
        logger.error('irods rsync failed. Message=%s', e.message )
        raise

    
    

    
