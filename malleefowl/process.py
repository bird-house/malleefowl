"""
Processes for ClimDaPs WPS

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""


from pywps.Process import WPSProcess as PyWPSProcess

class WPSProcess(PyWPSProcess):
    """This is the base class for all climdaps wps processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        # TODO: what can i do with this?
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

class WorkflowProcess(WPSProcess):
    """This is the base class for all workflow processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        wf_identifier = identifier + '_workflow'
        metadata.append(
            {"title":"workflow", "href":"http://www.c3grid.de"}
            )
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
        self.input = self.addComplexInput(
            identifier="input",
            title="Input NetCDF File",
            abstract="NetCDF File from ESGF data node",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )


    


