from malleefowl.process import WorkerProcess
import subprocess


class IndicesProcess(WorkerProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkerProcess.__init__(self, 
            identifier = "de.csc.indices",
            title="Calculation of climate indices",
            version = "0.1",
            #storeSupported = "true",   # async
            #statusSupported = "true",  # retrieve status, needs to be true for async 
            ## TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Just testing a python script to  ...",
            extra_metadata={
                  'esgfilter': 'variable:tas,variable:pr,time_frequency:day',  #institute:MPI-M,
                  'esgquery': 'variable:tas AND variable:pr AND time_frequency:day' # institute:MPI-M 
                  },
            )

        # Literal Input Data
        # ------------------
        
        self.floatIn = self.addLiteralInput(
            identifier="float",
            title="Base temperature",
            abstract="Threshold for termal vegetation period",
            default="5.6",
            type=type(0.1),
            minOccurs=1,
            maxOccurs=1,
            )

        self.tas_yearmean = self.addLiteralInput(
            identifier="tas_yearmean",
            title="annual mean temperature (K)",
            abstract="annual mean temperature",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.pr_yearsum = self.addLiteralInput(
            identifier="pr_yearsum",
            title="annual precipitation sum (mm)",
            abstract="annual precipitation sum",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
    def execute(self):
        
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path
        import numpy as np
        from cdo import *
        import datetime 
        cdo = Cdo()
        
        # guess var names of files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = nc_file
            elif "pr" in ds.variables.keys():
                nc_pr = nc_file
            else:
                raise Exception("input netcdf file has not variable tas|pr")


        tasFilePath = '/home/main/sandbox/climdaps/parts/files/tas_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc'       
        prFilePath = '/home/main/sandbox/climdaps/parts/files/pr_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc'        

        tasFile = NetCDFFile(tasFilePath , 'r')        
        prFile = NetCDFFile(prFilePath ,'r')

        # get the dimensions
        # dimNames = tasFile.dimensions.keys()

        # simple precesses realized by cdo commands:
        tas_yearmean = np.squeeze(cdo.yearmean(input=tasFilePath, options='-f nc', returnMaArray='tas'))
        tas_yearmeanFile = NetCDFFile('/home/main/sandbox/climdaps/parts/files/tas_yearmeanFile.nc', 'w')
        tas_yearmeanTimeDim = tas_yearmeanFile.createDimension('time', tas_yearmean.shape[0])
        tas_yearmeanLatDim = tas_yearmeanFile.createDimension('lat', tas_yearmean.shape[1])
        tas_yearmeanLonDim = tas_yearmeanFile.createDimension('lon', tas_yearmean.shape[2])
        dims = ('time','lat','lon')
        tas_yearmeanVar = tas_yearmeanFile.createVariable('tas_yearmean', 'f', dims)
        tas_yearmeanVar.assignValue(tas_yearmean)
        tas_yearmeanFile.close()

        pr_yearsum = np.squeeze(cdo.yearsum(input=prFilePath, options='-f nc', returnMaArray='pr'))
        pr_yearsum = pr_yearsum * 60 * 60 * 24 # convert flux to amount 
        pr_yearsumFile = NetCDFFile('/home/main/sandbox/climdaps/parts/files/pr_yearsumFile.nc', 'w')
        pr_yearsumTimeDim = pr_yearsumFile.createDimension('time', pr_yearsum.shape[0])
        pr_yearsumLatDim = pr_yearsumFile.createDimension('lat', pr_yearsum.shape[1])
        pr_yearsumLonDim = pr_yearsumFile.createDimension('lon', pr_yearsum.shape[2])
        dims = ('time','lat','lon')
        pr_yearsumVar = pr_yearsumFile.createVariable('pr_yearsum', 'f', dims)
        pr_yearsumVar.assignValue(pr_yearsum)
        pr_yearsumFile.close()

        # more sufesticated processes
        # get the raw values into memory: 

        dates = str(cdo.showdate(input=tasFilePath)).replace("'","").replace(']','').replace('[','').split('  ')
        for xx,date in enumerate(dates):
            date = date.split('-')
            dates[xx] = datetime.date(int(date[0]),int(date[1]),int(date[2]))

        tas = np.squeeze(tasFile.variables["tas"])
        tas = tas - 273.15
        pr = np.squeeze(prFile.variables["pr"])
        pr = pr  * 60 * 60 * 24

        # close inFiles
        tasFile.close()
        prFile.close() 


        # from os import curdir, path
        # nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        #result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/clim_in", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()], stdout=True)
        ## literals
        ## subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])

        #nc_hurs = path.join(path.abspath(curdir), "clim_in.nc")
        #self.output.setValue( nc_hurs )
