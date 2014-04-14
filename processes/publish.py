from malleefowl.process import WorkerProcess
from malleefowl import publish, tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Publish(WorkerProcess):
    """Publish netcdf files to thredds server"""
    def __init__(self):
        WorkerProcess.__init__(
            self,
            identifier = "org.malleefowl.publish",
            title = "Publish NetCDF Files to Thredds Server",
            version = "0.2",
            metadata=[
                ],
            abstract="Publish netcdf files to Thredds server...",
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to publish data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.basename = self.addLiteralInput(
            identifier="basename",
            title="Basename",
            abstract="Basename of files",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Publisher result",
            abstract="Publisher result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
        
    def execute(self):
        self.show_status("publishing ...", 10)

        token = self.token.getValue()
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

        result = publish.mv_to_local_store(files=self.get_nc_files(),
                                           basename=self.basename.getValue(),
                                           userid=userid)
    
        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
             import json
             json.dump(result, fp, indent=True)
        self.output.setValue( outfile )

        self.show_status("publishing ... done", 90)
