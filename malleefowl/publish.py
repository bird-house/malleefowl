## This modules handles publishing results

from pywps import config

from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def publish_local(files=[], basename='base', userid='test@malleefowl.org'):
    import os
    basedir = config.getConfigValue( "malleefowl", "filesPath" )
    outdir = os.path.join(basedir, userid)
    utils.mkdir(outdir)

    result = []
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
        result.append( (outfile, success) )
    return result
    
