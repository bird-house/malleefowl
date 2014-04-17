"""
Processes for ClimDaPs WPS

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import types
import tempfile

from pywps.Process import WPSProcess as PyWPSProcess
from pywps import config

from malleefowl import database

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class WPSProcess(PyWPSProcess):
    """This is the base class for all climdaps wps processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract="", extra_metadata={}):

        self.extra_metadata=extra_metadata
        database.register_process_metadata(identifier, extra_metadata)
        metadata.append(
            {"title":"Hardworking Bird Malleefowl", "href":"http://en.wikipedia.org/wiki/Malleefowl"}
            )

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

    def addComplexInput(self, identifier, title, abstract=None,
                        metadata=[], minOccurs=1, maxOccurs=1,
                        formats=[{"mimeType":None}], maxmegabites=None,
                        upload=False):
        if upload:
            logger.debug('update extra metadata for complex input: identifier=%s', identifier)
            uploads = self.extra_metadata.get('uploads', [])
            if not identifier in uploads:
                uploads.append(identifier)
            self.extra_metadata['uploads'] = uploads
            logger.debug('extra metadata = %s', uploads)
            database.register_process_metadata(self.identifier, self.extra_metadata)
        return PyWPSProcess.addComplexInput(
            self,
            identifier=identifier,
            title=title,
            abstract=abstract,
            metadata=metadata,
            minOccurs=minOccurs,
            maxOccurs=maxOccurs,
            formats=formats,
            maxmegabites=maxmegabites)

    @property
    def cache_path(self):
        return config.getConfigValue("malleefowl","cachePath")

    @property
    def files_path(self):
        return config.getConfigValue("malleefowl","filesPath")

    @property
    def files_url(self):
        return config.getConfigValue("malleefowl","filesUrl")

    @property
    def working_dir(self):
        return os.path.abspath(os.curdir)

    @property
    def thredds_url(self):
        return config.getConfigValue("malleefowl", "threddsUrl")

    def irods_home(self):
        import yaml
        value = config.getConfigValue("irods", "home")
        homes = yaml.load(value)
        logger.debug('homes = %s', homes.keys())
        return homes

    @property
    def timeout(self):
        timeout = 0
        try:
            value = config.getConfigValue("malleefowl", "timeout")
            logger.debug("timeout = %s" % (value))
            timeout = int(value)
        except:
            logger.warn('invalid malleefowl:timeout parameter in pywps config')
        return timeout

    def mktempfile(self, suffix='.txt'):
        (_, filename) = tempfile.mkstemp(dir=self.working_dir, suffix=suffix)
        return filename

    def sleep(self, secs):
        import time
        time.sleep(secs)

    def show_status(self, msg, percent_done):
        logger.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)

class SourceProcess(WPSProcess):
     """This is the base class for all source processes."""

     def __init__(self, identifier, title, version, metadata=[], abstract="", extra_metadata={}):
        wf_identifier = identifier + ".source"
        
        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract = abstract,
            extra_metadata = extra_metadata)

        # input: source filter
        # --------------------
        
        self.file_identifier = self.addLiteralInput(
            identifier="file_identifier",
            title="File Identifier",
            abstract="URL, keyword, ...",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        # netcdf output
        # -------------

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

    def __init__(self, identifier, title, version, metadata=[], abstract="",
                 extra_metadata={
                     'esgfilter': '',
                     'esgquery': '*'}):
        wf_identifier = identifier + '.worker'
        
        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract = abstract,
            extra_metadata = extra_metadata)

        
        # complex input
        # -------------

        # TODO: needs some work ...
        self.netcdf_url_in = self.addComplexInput(
            identifier="file_identifier",
            title="NetCDF File",
            abstract="NetCDF File",
            metadata=[],
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




    


