"""
Processes for Penman Monteith relation 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
import tempfile
import subprocess

from malleefowl.process import WorkerProcess

class PenmanProcess(WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        WorkerProcess.__init__(self, 
            identifier = "de.csc.esgf.penman",
            title="evapotranspiration following the Penman Monteith equation",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to calculate the Penman Monteith equation...",
            extra_metadata={
                  'esgfilter': 'variable:sfcwind,variable:rlds,variable:rsds,
                           variable:rlus,variable:rsus,variable:ps,variable:huss,
                           variable:pr,variable:tas,time_frequency:day',  #institute:MPI-M,
                  'esgquery': 'variable:sfcwind 
                           AND variable:rlds
                           AND variable:rsds
                           AND variable:rlus
                           AND variable:rsus
                           AND variable:ps
                           AND variable:huss
                           AND variable:pr
                           AND variable:tas
                           AND time_frequency:day'
                           # institute:MPI-M 
                  },
            )

            
        # Literal Input Data
        # ------------------

       
        self.output = self.addComplexOutput(
            identifier="output",
            title="Evapotranspiration",
            abstract="Calculated Evapotranspiration following Penman Monteith relation ",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path

        # default var names
        huss = 'huss'
        ps = 'ps'
   
        # guess var names of files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = nc_file
            elif "sfcwind" in ds.variables.keys():
                nc_sfcwind = nc_file
            elif "rlds" in ds.variables.keys():
                nc_rlds = nc_file
            elif "rsds" in ds.variables.keys():
                nc_rsds = nc_file
            elif "rlus" in ds.variables.keys():
                nc_rlus = nc_file
            elif "rsus" in ds.variables.keys():
                nc_rsus = nc_file
            elif "ps" in ds.variables.keys():
                nc_ps = nc_file
            elif "huss" in ds.variables.keys():
                nc_huss = nc_file
            elif "pr" in ds.variables.keys():
                nc_pr = nc_file
            else:
                raise Exception("input netcdf file has not variable tas|huss|ps")
                
                
                
        def execute(self):
        
        result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/penman.sh")
        
        # literals
        # subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])
        
        self.file_out.setValue("/home/main/wps_data/hurs_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc")
   