from pywps.Process import WPSProcess as PyWPSProcess

import os
import tempfile
import types

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


@property
def working_dir():
    return os.path.abspath(os.curdir)


def mktempfile(suffix='.txt'):
    (_, filename) = tempfile.mkstemp(dir=os.curdir, suffix=suffix)
    return filename


def show_status(proc, msg, percent_done):
    logger.info('STATUS (%d/100) - %s: %s', percent_done, proc.identifier, msg)
    proc.status.set(msg=msg, percentDone=percent_done, propagate=True)

    
def getInputValues(proc, identifier):
    values = proc.getInputValue(identifier)
    if values is None:
        values = []
    elif type(values) != types.ListType:
        values = [values]
    return values

class WPSProcess(PyWPSProcess):
    """This is the base class for wps processes with some convenient functions."""
    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        PyWPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            abstract = abstract,
            metadata = metadata,
            storeSupported = "true",  # async
            statusSupported = "true"  # retrieve status, needs to be true for async
            )

     
    @property
    def working_dir(self):
        return working_dir

    
    def mktempfile(self, suffix='.txt'):
        return mktempfile()

    
    def show_status(self, msg, percent_done=0):
        logger.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)

        
    def getInputValues(self, identifier):
        return getInputValues(self, identifier)





    


