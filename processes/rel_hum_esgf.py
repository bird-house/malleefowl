"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
import tempfile
import subprocess

from malleefowl.process import WorkflowProcess

class EsgfRelHumProcess(WorkflowProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkflowProcess.__init__(self, 
            identifier = "de.csc.esgf.relhum",
            title="Specific to relative humidity (with ESGF data source)",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to calculate the relative humidity ...",
            )

        # Literal Input Data
        # ------------------

       
        self.output = self.addComplexOutput(
            identifier="output",
            title="Relative Humidity",
            abstract="Calculated Relative Humidity",
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
            elif "huss" in ds.variables.keys():
                nc_huss = nc_file
            elif "hus" in ds.variables.keys():
                nc_huss = nc_file
                huss = 'hus'
            elif "ps" in ds.variables.keys():
                nc_ps = nc_file
            elif "psl" in ds.variables.keys():
                nc_ps = nc_file
                ps = 'psl'
            else:
                raise Exception("input netcdf file has not variable tas|huss|ps")
                
        # merge ps and huss
        (_, merged_ps_huss) = tempfile.mkstemp(suffix='.nc')
        cmd = ['cdo', '-O', 'merge', nc_ps, nc_huss, merged_ps_huss]
        self.cmd(cmd=cmd, stdout=True)

        self.status.set(msg="relhum merged", percentDone=20, propagate=True)
        
        # ps * huss
        (_, nc_e) = tempfile.mkstemp(suffix='.nc')
        expr = "expr,\'e=((%s*%s)/62.2)\'" % (ps, huss)
        cmd = ['cdo', expr, merged_ps_huss, nc_e]
        self.cmd(cmd=cmd, stdout=True)

        self.status.set(msg="relhum ps*hus", percentDone=40, propagate=True)
        
        # partial vapour pressure using Magnus-Formula over water
        # cdo expr,'es=6.1078*10^(7.5*(tas-273.16)/(237.3+(tas-273.16)))' ../in/tas_$filename  ../out/es_$filename
        (_, nc_es) = tempfile.mkstemp(suffix='.nc')
        cmd = ['cdo', "expr,\'es=6.1078*exp(17.08085*(tas-273.16)/(234.175+(tas-273.16)))\'", nc_tas, nc_es]
        self.cmd(cmd=cmd, stdout=True)

        self.status.set(msg="relhum ps*hus", percentDone=60, propagate=True)
        
        # calculate relative humidity
        nc_hurs = path.join(path.abspath(curdir), "hurs_test.nc")
        cmd = ['cdo', '-div', nc_e, nc_es, nc_hurs]
        self.cmd(cmd=cmd, stdout=True)
        
        self.status.set(msg="relhum done", percentDone=90, propagate=True)
        self.output.setValue( nc_hurs )
        
        
        
