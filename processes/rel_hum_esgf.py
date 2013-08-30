"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
from malleefowl.process import WorkflowProcess
import subprocess

class RelHumProcessEsgf(WorkflowProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkflowProcess.__init__(self, 
            identifier = "de.csc.relhum_esgf",
            title="Specific to relative humidity",
            version = "0.1",
            #storeSupported = "true",   # async
            #statusSupported = "true",  # retrieve status, needs to be true for async 
            ## TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Just testing a nice script to calculate the relative humidity ...",
            )

        # Literal Input Data
        # ------------------

       
        self.file_out = self.addComplexOutput(
            identifier="file_out",
            title="Out File",
            abstract="out file",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path
        
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = nc_file
            elif "huss" in ds.variables.keys():
                nc_huss = nc_file
            elif "ps" in ds.variables.keys():
                nc_ps = nc_file
            else:
                raise Exeption("missing something")
                
        nc_hurs = path.join(path.abspath(curdir), "hurs_test.nc")
        rel_hum_command = path.join(path.dirname(__file__),"rel_hum_esgf.sh")
        self.cmd(cmd=["bash", rel_hum_command , nc_tas, nc_huss, nc_ps, nc_hurs ], stdout=True)
        
        self.status.set(msg="cdo sinfo done", percentDone=90, propagate=True)
        self.file_out.setValue( nc_hurs )
        
        
        
