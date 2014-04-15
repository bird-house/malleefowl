## This modules handles publishing results

import os

from pywps import config

from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def link_to_local_store(files=[], basename=None, userid=None):
    logger.debug("publish to local store, userid=%s", userid)
    
    basedir = config.getConfigValue( "malleefowl", "filesPath" )
    outdir = os.path.join(basedir, userid)
    utils.mkdir(outdir)

    results = []
    for f in files:
        outfile = os.path.basename(f)
        if basename is not None:
            outfile = basename + '-' + outfile
        outfile = os.path.join(outdir, outfile)
        success = False
        try:
            if not os.path.exists(outfile):
                os.link(f, outfile)
        except:
            logger.error("publishing of %s failed", f)
        results.append( (outfile, success) )
    return results
