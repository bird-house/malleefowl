from pywps.Process import WPSProcess as PyWPSProcess

import types
import logging

def show_status(proc, msg, percent_done):
    logger.info('STATUS (%d/100) - %s: %s', percent_done, proc.identifier, msg)
    proc.status.set(msg=msg, percentDone=percent_done, propagate=True)

    
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
            storeSupported = True,  # async
            statusSupported = True  # retrieve status, needs to be true for async
            )

    def show_status(self, msg, percent_done=0):
        logging.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)






    


