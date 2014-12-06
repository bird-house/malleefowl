"""
Processes with cdo commands
"""

from malleefowl.process import WPSProcess

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class Dummy(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "dummy",
            title = "Dummy",
            version = "0.1",
            metadata=[],
            abstract="Dummy process for testing.",
            )

        self.netcdf_file = self.addComplexInput(
            identifier="resource",
            title="Resource",
            abstract="NetCDF File",
            minOccurs=0,
            maxOccurs=100,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="result",
            abstract="result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("starting ...", 0)

        nc_files = self.getInputValues(identifier='resource')

        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            import os
            fp.write('PYTHONPATH=%s' % (os.environ.get('PYTHONPATH')))
            for nc_file in nc_files:
                fp.write('%s\n', nc_file)

        self.show_status("done", 100)

        self.output.setValue( outfile )
