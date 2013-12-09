#from malleefowl.process import WorkerProcess
import malleefowl.process

import os

class Publish(malleefowl.process.WorkerProcess):
    """Publish netcdf files to thredds server"""
    def __init__(self):
        malleefowl.process.WorkerProcess.__init__(
            self,
            identifier = "de.dkrz.publish",
            title = "Publish NetCDF Files to Thredds Server",
            version = "0.1",
            metadata=[
                {"title":"ClimDaPs","href":"https://redmine.dkrz.de/collaboration/projects/climdaps"},
                ],
            abstract="Publish netcdf files to Thredds server...",
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
        self.status.set(msg="starting publisher", percentDone=10, propagate=True)

        nc_files = self.get_nc_files()

        result = "Published files to thredds server\n"
        
        count = 0
        for nc_file in nc_files:
            outfile = os.path.join(self.files_path,
                                   self.basename.getValue() + "-" +
                                   os.path.basename(nc_file) + ".nc")
            result = result + outfile + "\n"
            try:
                os.link(os.path.abspath(nc_file), outfile)
                result = result + "success\n"
            except:
                self.message(msg="publish failed", force=True)
                result = result + "failed\n"
            count = count + 1
            percent_done = int(10 + 80.0 / len(nc_files) * count)
            self.status.set(msg="%d file(s) published" % count,
                            percentDone=percent_done, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.output.setValue( out_filename )

        self.status.set(msg="publisher done", percentDone=90, propagate=True)
