## This modules handles publishing results

import os

from pywps import config

from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def mv_to_local_store(files=[], basename='base', userid='test@malleefowl.org'):
    logger.debug("publish to local store, userid=%s", userid)
    
    basedir = config.getConfigValue( "malleefowl", "filesPath" )
    outdir = os.path.join(basedir, userid)
    utils.mkdir(outdir)

    results = []
    for f in files:
        outfile = os.path.join(outdir,
                               basename + "-" +
                               os.path.basename(f) + ".nc")
        success = False
        try:
            os.link(os.path.abspath(f), outfile)
            success = True
        except:
            logger.error("publishing of %s failed", f)
        results.append( (outfile, success) )
    logger.debug("publish done, num results=%s", len(results))
    return results
    

def link_to_local_store(files=[], userid='test@malleefowl.org'):
    logger.debug("publish to local store, userid=%s", userid)
    
    basedir = config.getConfigValue( "malleefowl", "filesPath" )
    outdir = os.path.join(basedir, userid)
    utils.mkdir(outdir)

    results = []
    for f in files:
        outfile = os.path.join(outdir, os.path.basename(f))
        success = False
        try:
            if not os.path.lexists(outfile):
                os.symlink(f, outfile)
        except:
            logger.error("publishing of %s failed", f)
        results.append( (outfile, success) )
    return results
