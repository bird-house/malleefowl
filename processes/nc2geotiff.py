"""
Processes with cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

#from malleefowl.process import WorkerProcess
import malleefowl.process

class nc2geotiff(malleefowl.process.WorkerProcess):
    """This process calls cdo with operation on netcdf file"""
    def __init__(self):
        malleefowl.process.WorkerProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.nc2geotiff",
            title = "Transform netCDF to geoTiff",
            version = "0.1",
            metadata=[
                {"title":"CDO","href":"https://code.zmaw.de/projects/cdo"},
                ],
            abstract="Transform netCDF format to geoTiff format",
            )

        # complex output
        # -------------

        self.output = self.addComplexOutput(
            identifier="geotiff",
            title="geoTiff Output",
            abstract="geoTiff Output",
            metadata=[],
            formats=[{"mimeType":"image/tiff"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting gdal operator", percentDone=10, propagate=True)

        nc_file = self.get_nc_files()
 
        out_filename = self.mktempfile(suffix='.tif')
        try:
            cmd = ["gdal_translate -of GTiff", nc_file , out_filename]
            self.cmd(cmd=cmd, stdout=True)
        except:
            self.message(msg='transformation failed', force=True)

        self.status.set(msg="gdal transforamtion done", percentDone=90, propagate=True)
        self.output.setValue( nc_file[1] )

