"""
Processes for testing cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import types

from pywps.Process import WPSProcess


class CDOInfo(WPSProcess):
    """This process calls cdo sinfo for given netcdf file"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "de.dkrz.test.cdo.sinfo",
            title = "Testing cdo sinfo call",
            version = "0.1",
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            # TODO: what can i do with this?
            metadata=[
                {"title":"CDO","href":"https://code.zmaw.de/projects/cdo"},
                ],
            abstract="Just testing cdo sinfo ...",
            grassLocation = False)

        # complex input
        # -------------

        # TODO: needs some work ...
        self.netcdf_in = self.addComplexInput(
            identifier="input",
            title="Input NetCDF File",
            abstract="Upload NetCDF File ...",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"application/x-netcdf"}],
            maxmegabites=4
            )

        # complex output
        # -------------

        self.text_out = self.addComplexOutput(
            identifier="output",
            title="CDO sinfo result",
            abstract="CDO sinfo result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        self.message(msg='exec cdo sinfo', force=True)

        # complex
        # write cdo result
        self.message(msg='cdo result', force=True)
        
        from os import curdir, path
        nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        result = self.cmd(cmd=["cdo", "sinfo", nc_filename], stdout=True)

        with open('/tmp/cdo-result.txt', 'w') as fp:
            fp.write(result)
            fp.close()
            self.text_out.setValue( fp.name )
