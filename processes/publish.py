from malleefowl.process import WorkerProcess

import os

class Publish(WorkerProcess):
    """Publish netcdf files to thredds server"""
    def __init__(self):
        WorkerProcess.__init__(
            self,
            identifier = "org.malleefowl.publish",
            title = "Publish NetCDF Files to Thredds Server",
            version = "0.1",
            metadata=[
                {"title":"ClimDaPs","href":"https://redmine.dkrz.de/collaboration/projects/climdaps"},
                ],
            abstract="Publish netcdf files to Thredds server...",
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

        count = 0
        for nc_file in nc_files:
            outfile = os.path.join(self.files_path, os.path.basename(nc_file) + ".nc")
            os.link(os.path.abspath(nc_file), outfile)
            count = count + 1
            percent_done = int(10 + 80.0 / len(nc_files) * count)
            self.status.set(msg="%d file(s) published" % count,
                            percentDone=percent_done, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write('all files written ...')
            fp.close()
            self.output.setValue( out_filename )

        self.status.set(msg="publisher done", percentDone=90, propagate=True)
