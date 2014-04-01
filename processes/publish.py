from malleefowl.process import WorkerProcess
from malleefowl import utils, tokenmgr
import os

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
        self.show_status("starting publisher", 10)

        token = self.token.getValue()
        nc_files = self.get_nc_files()

        result = "Published files to thredds server\n"

        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
        
        outdir = os.path.join(self.files_path, userid)
        utils.mkdir(outdir)
        
        count = 0
        for nc_file in nc_files:
            outfile = os.path.join(outdir,
                                   self.basename.getValue() + "-" +
                                   os.path.basename(nc_file) + ".nc")
            result = result + outfile + "\n"
            try:
                os.link(os.path.abspath(nc_file), outfile)
                result = result + "success\n"
            except:
                logger.error("publishing of %s failed", nc_file)
                result = result + "failed\n"
            count = count + 1
            percent_done = int(20 + 70.0 / len(nc_files) * count)
            self.show_status("%d file(s) published" % count, percent_done)
        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.output.setValue( out_filename )

        self.show_status("publisher done", 90)
