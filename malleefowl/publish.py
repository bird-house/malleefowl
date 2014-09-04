## This modules handles publishing results

import os

from malleefowl import utils, config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def link_to_local_store(filename, newname=None, userid=None):
    """
    Link filename to local store. If newname is given then rename file to this name.
    """ 
    logger.debug("publish to local store, userid=%s", userid)
    
    basedir = config.files_path()
    outdir = os.path.join(basedir, userid)
    utils.mkdir(outdir)

    if newname is None or newname.strip() == '' or newname == '<colander.null>':
        newname = os.path.basename(filename)
    if not newname.endswith('.nc'):
        newname = newname + '.nc'
    outfile = os.path.join(outdir, newname)
    success = False
    try:
        if not os.path.exists(outfile):
            os.link(filename, outfile)
        success = True
    except:
        logger.exception("publishing of %s failed", filename)
    return (newname, success)
