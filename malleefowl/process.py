"""
Processes for ClimDaPs WPS

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import types
import tempfile

from pywps.Process import WPSProcess as PyWPSProcess
from pywps import config

import utils

class WPSProcess(PyWPSProcess):
    """This is the base class for all climdaps wps processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        metadata.append(
            {"title":"ClimDaPs", "href":"http://www.dkrz.de"}
            )
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

    @property
    def cache_path(self):
        return config.getConfigValue("server","cachePath")

    @property
    def files_path(self):
        return config.getConfigValue("server","filesPath")

    @property
    def files_url(self):
        return config.getConfigValue("server","filesUrl")

    @property
    def working_dir(self):
        return os.path.abspath(os.curdir)

    def mktempfile(self, suffix='.txt'):
        (_, filename) = tempfile.mkstemp(dir=self.working_dir, suffix=suffix)
        return filename

class SourceProcess(WPSProcess):
     """This is the base class for all source processes."""

     def __init__(self, identifier, title, version, metadata=[], abstract=""):
        wf_identifier = identifier + ".source"
        metadata.append(
            {"title":"C3Grid", "href":"http://www.c3grid.de"},
            )

        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract=abstract)

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
                     'esgfilter': 'institute:MPI-M,variable:tas,experiment:esmHistorical,ensemble:r1i1p1,time_frequency:day',
                     'esgquery': '*'}):
        wf_identifier = identifier + '.worker'
        metadata.append(
            {"title":"C3Grid", "href":"http://www.c3grid.de"},
            )

        utils.register_process_metadata(wf_identifier, extra_metadata)
        
        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract=abstract)

        
        # complex input
        # -------------

        # TODO: needs some work ...
        self.netcdf_url_in = self.addComplexInput(
            identifier="file_identifier",
            title="NetCDF File",
            abstract="NetCDF File",
            metadata=[],
            minOccurs=0,
            maxOccurs=10,
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




    


