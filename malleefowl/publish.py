## This modules handles publishing results

from pywps import config

from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def to_local_store(files=[], basename='base', userid='test@malleefowl.org'):
    import os

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
            logger.error("publishing of %s failed", nc_file)
        results.append( (outfile, success) )
    logger.debug("publish done, num results=%s", len(results))
    return results
    
