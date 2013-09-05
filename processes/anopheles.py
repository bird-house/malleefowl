"""
Processes for Anopheles Gambiae population dynamics 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
import tempfile
import subprocess

from malleefowl.process import WorkerProcess

class AnophelesProcess(WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        WorkerProcess.__init__(self, 
            identifier = "de.csc.esgf.anopheles",
            title="Population dynamics of Anopheles Gambiae",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to calculate the Population dynamics of Anopheles Gambiae",
            extra_metadata={
                  'esgfilter': 'variable:tas,variable:evspsbl,variable:hurs,variable:pr',  #institute:MPI-M, ,time_frequency:day
                  'esgquery': 'variable:tas AND variable:evspsbl AND variable:hurs AND variable:pr' # institute:MPI-M AND time_frequency:day 
                  },
            )
            
            
        # Literal Input Data
        # ------------------

       
        self.output = self.addComplexOutput(
            identifier="output",
            title="anopheles",
            abstract="Calculated population dynamics of adult Anopheles Gambiae ",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path

        # guess var names of files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = nc_file
            elif "hurs" in ds.variables.keys():
                nc_hurs = nc_file
            elif "pr" in ds.variables.keys():
                nc_pr = nc_file
            elif "evspsbl" in ds.variables.keys():
                nc_evspsbl = nc_file    
            else:
                raise Exception("input netcdf file has not variable tas|hurs|pr|evspsbl")
                     
        nc_anopheles = path.join(path.abspath(curdir), "nc_anopheles.nc")
        script_anophels =  path.join(path.dirname(__file__),"anopheles.R")
        
        self.cmd(cmd=["R", "--vanilla", "--args", nc_tas, nc_hurs, nc_pr, nc_evspsbl, nc_anopheles, "<", script_anophels ], stdout=True)
        self.status.set(msg="anopheles done", percentDone=90, propagate=True)
        self.output.setValue( nc_anopheles )
        