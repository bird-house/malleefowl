import os

from irods import *

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def ls(collection):
    logger.debug('irods collection=%s' % (collection))

    logger.debug("home=%s", os.environ['HOME'])

    status, my_env = getRodsEnv()
    logger.debug('host=%s, status=%s', my_env.rodsHost, status)
    conn, err_msg = rcConnect(my_env.rodsHost, my_env.rodsPort, my_env.rodsUserName, my_env.rodsZone)
    
    logger.debug("got connection, conn=%s, error msg=%s", conn, err_msg)
    status = clientLogin(conn)

    logger.debug("client login, status=%s", status)

    # Open the current working directory
    c = irodsCollection(conn)
    c.openCollection(collection)

    logger.debug('collection name=%s, num objs=%s, num sub coll=%s',
                 c.getCollName(),
                 c.getLenObjects(),
                 c.getLenSubCollections())
    
    files = [obj[0] for obj in c.getObjects()]
    subcolls = [c for c in c.getSubCollections()]
    conn.disconnect()
  
    return (files, subcolls)

def rsync(src, dest):
    logger.debug('rsync src=%s dest=%s', src, dest)

    # TODO: show files that have been synced (option -l)

    from subprocess import check_output, STDOUT, CalledProcessError
    try:
        check_output(["irsync", "-rvs", src, dest], stderr=STDOUT)
    except CalledProcessError as e:
        logger.error('irods rsync failed. Ouput=%s', e.output )
        raise
    except Exception as e:
        logger.error('irods rsync failed. Message=%s', e.message )
        raise

    # TODO: fix permissions
    os.chmod(dest , 0o755)
    for root,dirs,_ in os.walk(dest):
        for d in dirs:
            logger.debug('fix permission: dir=%s', d)
            os.chmod(os.path.join(root,d) , 0o755)

    
    

    
