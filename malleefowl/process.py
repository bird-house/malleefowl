from pywps.Process import WPSProcess as PyWPSProcess

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class WPSProcess(PyWPSProcess):
    """This is the base class for wps processes with some convenient functions."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        PyWPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            metadata = metadata,
            abstract = abstract,
            grassLocation = False)

    @property
    def working_dir(self):
        import os
        return os.path.abspath(os.curdir)

    def mktempfile(self, suffix='.txt'):
        import tempfile
        (_, filename) = tempfile.mkstemp(dir=self.working_dir, suffix=suffix)
        return filename

    def show_status(self, msg, percent_done):
        logger.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)

    def getInputValues(self, identifier):
        import types
        values = self.getInputValue(identifier)
        if values is None:
            values = []
        elif type(values) != types.ListType:
            values = [values]
        return values





    


