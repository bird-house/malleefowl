import os
import types
import tempfile

from pywps.Process import WPSProcess as PyWPSProcess

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class WPSProcess(PyWPSProcess):
    """This is the base class for all climdaps wps processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):

        # TODO: fix metadata appending
        #metadata.append({"title":"Hardworking Bird Malleefowl", "href":"http://en.wikipedia.org/wiki/Malleefowl"})

        PyWPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            metadata = metadata,
            abstract=abstract,
            grassLocation = False)

    @property
    def files_path(self):
        return config.files_path()

    @property
    def working_dir(self):
        return os.path.abspath(os.curdir)

    def mktempfile(self, suffix='.txt'):
        (_, filename) = tempfile.mkstemp(dir=self.working_dir, suffix=suffix)
        return filename

    def sleep(self, secs):
        import time
        time.sleep(secs)

    def show_status(self, msg, percent_done):
        logger.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)

    def getInputValues(self, identifier):
        values = self.getInputValue(identifier)
        if values is None:
            values = []
        elif type(values) != types.ListType:
            values = [values]
        return values

class SourceProcess(WPSProcess):
     """This is the base class for all source processes."""

     def __init__(self, identifier, title, version, metadata=[], abstract=""):
        
        WPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract = abstract)

        self.file_identifier = self.addLiteralInput(
            identifier="file_identifier",
            title="File Identifier",
            abstract="URL, keyword, ...",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

class WorkerProcess(WPSProcess):
    """This is the base class for all worker processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        WPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract = abstract)
        
        # complex input
        # -------------

        # TODO: needs some work ...
        self.netcdf_url_in = self.addComplexInput(
            identifier="file_identifier",
            title="NetCDF File",
            abstract="NetCDF File",
            minOccurs=1,
            maxOccurs=100,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

    def get_nc_files(self):
        nc_files = []
        value = self.netcdf_url_in.getValue()
        if value != None:
            if type(value) == types.ListType:
                nc_files = value
            else:
                nc_files = [value]

        nc_files = map(os.path.abspath, nc_files)
        return nc_files




    


