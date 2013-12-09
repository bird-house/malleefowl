"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
#from malleefowl.process import WorkerProcess
import malleefowl.process 
import subprocess

class GamProcess(malleefowl.process.WorkerProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        malleefowl.process.WorkerProcess.__init__(self, 
            identifier = "de.csc.gam",
            title="Species distribution modell ",
            version = "0.1",
            #storeSupported = "true",   # async
            #statusSupported = "true",  # retrieve status, needs to be true for async 
            ## TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Calculation of species distribution",
            extra_metadata={
                  'esgquery': 'variable:tas OR variable:pr ', # institute:MPI-M 
                  'esgfilter': 'project:CORDEX,time_frequency:mon'  #institute:MPI-M,
                  },
           )

        # Literal Input Data
        # ------------------

        self.period_in = self.addLiteralInput(
            identifier="period",
            title="Select period",
            abstract="Select between reference or projection period",
            default="reference",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['reference','projection']
            )
        
        self.R_in = self.addComplexInput(
            identifier="R_in",
            title="PA-Data or RData",
            abstract="example: http://localhost:8090/files/ICP_DATA_Fsylv_Pabie_2.csv",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=10,
            formats=[{"mimeType":"text/csv"}],
            )
            
        self.climin1 = self.addLiteralInput(
            identifier="climin1",
            title="mean temperature June to August",
            abstract="Kappa Value (choose 0 if Indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )
            
        self.climin2 = self.addLiteralInput(
            identifier="climin2",
            title="mean temperature May to September",
            abstract="Kappa Value (choose 0 if indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )
            
        self.climin3 = self.addLiteralInput(
            identifier="climin3",
            title="precipitation sum June to August",
            abstract="Kappa Value (choose 0 if indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )

        self.climin4 = self.addLiteralInput(
            identifier="climin4",
            title="precipitation sum May to September",
            abstract="Kappa Value (choose 0 if indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )
            
        self.climin5 = self.addLiteralInput(
            identifier="climin5",
            title="Coldest month",
            abstract="Kappa Value (choose 0 if indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )
            
        self.climin6 = self.addLiteralInput(
            identifier="climin6",
            title="dyest month",
            abstract="Kappa Value (choose 0 if indice should not be used)",
            default="0",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['0','1','2','3','4','5','6','7','8','9','10','11','12']
            )
  
        self.output = self.addComplexOutput(
            identifier="output",
            title="Indices Output tar",
            abstract="Indices Output file",
            metadata=[],
            formats=[{"mimeType":"application/x-tar"}],
            asReference=True,
            )
         
    def execute(self):

        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path, system
        import numpy as np
        from cdo import *
        import datetime
        import string
        
        cdo = Cdo()
        
        # get the appropriate files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                tasFilePath = nc_file
            elif "pr" in ds.variables.keys():
                prFilePath = nc_file
            else:
                raise Exception("input netcdf file has not variable tas|pr")

        #tasFilePath = '/home/main/sandbox/climdaps/parts/files/tas_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc'       
        #prFilePath = '/home/main/sandbox/climdaps/parts/files/pr_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc'        

        tasFile = NetCDFFile(tasFilePath , 'r')        
        prFile = NetCDFFile(prFilePath ,'r')
        
        c_files = list()
        c_kappa = list()
        c_names = list()

        if int(self.climin1.getValue()) > 0 :  
            c1_file = self.mktempfile(suffix='.nc')
            c1_temp = self.mktempfile(suffix='.nc')
            c_files.append(c1_file)
            c_kappa.append(self.climin1.getValue())
            c_names.append('c1')
            cdo.selmon('6,7,8' , input= tasFilePath, options='-f nc', output = c1_temp)
            cdo.yearmean(input= c1_temp, options='-f nc', output = c1_file)
            self.status.set(msg="c1 done", percentDone=10, propagate=True)

        if int(self.climin2.getValue()) > 0 :  
            c2_file = self.mktempfile(suffix='.nc')
            c2_temp = self.mktempfile(suffix='.nc')
            c_files.append(c2_file)
            c_kappa.append(self.climin2.getValue())
            c_names.append('c2')
            cdo.selmon('5,6,7,8,9' , input= tasFilePath, options='-f nc', output = c2_temp)
            cdo.yearmean(input= c2_temp, options='-f nc', output = c2_file)
            self.status.set(msg="c2 done", percentDone=20, propagate=True)

        if int(self.climin3.getValue()) > 0 :  
            c3_file = self.mktempfile(suffix='.nc')
            c3_temp = self.mktempfile(suffix='.nc')
            c_files.append(c3_file)
            c_kappa.append(self.climin3.getValue())
            c_names.append('c3')
            cdo.selmon('6,7,8' , input= prFilePath, options='-f nc', output = c3_temp)
            cdo.yearsum(input= c3_temp, options='-f nc', output = c3_file)
            self.status.set(msg="c3 done", percentDone=30, propagate=True)

        if int(self.climin4.getValue()) > 0 :  
            c4_file = self.mktempfile(suffix='.nc')
            c4_temp = self.mktempfile(suffix='.nc')
            c_files.append(c4_file)
            c_kappa.append(self.climin4.getValue())
            c_names.append('c4')
            cdo.selmon('5,6,7,8,9' , input= prFilePath, options='-f nc', output = c4_temp)
            cdo.yearsum(input= c4_temp, options='-f nc', output = c4_file)
            self.status.set(msg="c4 done", percentDone=40, propagate=True)
            
        if int(self.climin5.getValue()) > 0 :  
            c5_file = self.mktempfile(suffix='.nc')
            c_files.append(c5_file)
            c_kappa.append(self.climin5.getValue())
            c_names.append('c5')
            cdo.yearmin(input= tasFilePath, options='-f nc', output = c5_file)
            self.status.set(msg="c5 done", percentDone=50, propagate=True)

        if int(self.climin6.getValue()) > 0 :  
            c6_file = self.mktempfile(suffix='.nc')
            c_files.append(c6_file)
            c_kappa.append(self.climin6.getValue())
            c_names.append('c6')
            cdo.yearmin(input= prFilePath, options='-f nc', output = c6_file)
            self.status.set(msg="c6 done", percentDone=50, propagate=True)
        
        if self.period_in.getValue() == 'reference' :
            rworkspace = self.mktempfile(suffix='.RData')
            system("R --vanilla --args %s %s %s %s %s %s < /home/main/sandbox/climdaps/src/Malleefowl/processes/gam_reference.r " % (rworkspace, self.R_in.getValue(), str(len(c_files)), string.join(c_names," "), string.join(c_files," "), string.join(c_kappa," ")))
            self.output.setValue( rworkspace )
        
        
        if self.period_in.getValue() == 'projection' :
            summary = self.mktempfile(suffix='.pdf')
            system("R --vanilla --args %s %s %s %s %s %s  < /home/main/sandbox/climdaps/src/Malleefowl/processes/gam_projection.r > ./gam_projection.log" % (summary, self.R_in.getValue(), str(len(c_files)), string.join(c_names," "), string.join(c_files," "), string.join(c_kappa," ")))
            self.output.setValue( summary )
        
        # from os import curdir, path
        # nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        #result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/dkrz/gam_job.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(), self.end_date_in.getValue()], stdout=True)
        ## literals
        # subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])
        #self.file_out_ref.setValue("/home/main/wps_data/gam_ref.nc")
        #self.file_out_pred.setValue("/home/main/wps_data/gam_pred.nc")
        #self.output.setValue( c6_file )
        
        
