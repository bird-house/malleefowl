from malleefowl.process import WPSProcess
from malleefowl import publish, tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Publish(WPSProcess):
    """Publish netcdf file to thredds server."""
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.publish",
            title = "Publish to Thredds",
            version = "0.3",
            metadata=[
                ],
            abstract="Publish netcdf file to Thredds server...",
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to publish data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.newname = self.addLiteralInput(
            identifier="newname",
            title="New Filename",
            abstract="For Example: tas_test_01.nc",
            type=type(''),
            default='',
            minOccurs=0,
            maxOccurs=1,
            )

        self.nc_file = self.addComplexInput(
            identifier="nc_file",
            title="NetCDF File",
            abstract="NetCDF File",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Publisher result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
        
    def execute(self):
        self.show_status("Publishing ...", 10)

        token = self.token.getValue()
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

        result = publish.link_to_local_store(filename=self.nc_file.getValue(),
                                             newname=self.newname.getValue(),
                                             userid=userid)
    
        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
             import json
             json.dump(result, fp, indent=True)
        self.output.setValue( outfile )

        self.show_status("Publishing ... done", 90)
