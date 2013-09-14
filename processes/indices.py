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
            maxOccurs=0,
            )

        self.tas_yearmean = self.addLiteralInput(
            identifier="tas_yearmean",
            title="annual mean temperature (K)",
            abstract="annual mean temperature",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        self.pr_yearsum = self.addLiteralInput(
            identifier="pr_yearsum",
            title="annual precipitation sum (mm)",
            abstract="annual precipitation sum",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        self.tas_5to9mean = self.addLiteralInput(
            identifier="tas_5to9mean",
            title="tas_5to9mean",
            abstract="mean temperature (K) form Mai to September",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )

        self.tas_6to8mean = self.addLiteralInput(
            identifier="tas_6to8mean",
            title="tas_6to8mean",
            abstract="mean temperature (K) form Juni to August",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        self.pr_5to9sum = self.addLiteralInput(
            identifier="pr_5to9sum",
            title="pr_5to9sum",
            abstract="precipitation sum (mm) form Mai to September",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        self.pr_6to8sum = self.addLiteralInput(
            identifier="pr_6to8sum",
            title="pr_6to8sum",
            abstract="precipitation sum (mm) form Juni to August",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        # defined in WorkflowProcess ...

        # complex output
        # -------------

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
        from os import curdir, path
        import numpy as np
        from cdo import *
        import datetime 
        import tarfile

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
        output_files =  list()
        
        # get the dimensions
        # dimNames = tasFile.dimensions.keys()

        # simple precesses realized by cdo commands:
        if self.tas_yearmean.getValue() == True :  
            tas_yearmean = np.squeeze(cdo.yearmean(input=tasFilePath, options='-f nc', returnMaArray='tas'))
            tas_yearmean_filename = self.mktempfile(suffix='_tas_yearmean_.nc')
            output_files.append(tas_yearmean_filename)
            tas_yearmeanFile = NetCDFFile(tas_yearmean_filename , 'w')
            tas_yearmeanTimeDim = tas_yearmeanFile.createDimension('time', tas_yearmean.shape[0])
            tas_yearmeanLatDim = tas_yearmeanFile.createDimension('lat', tas_yearmean.shape[1])
            tas_yearmeanLonDim = tas_yearmeanFile.createDimension('lon', tas_yearmean.shape[2])
            dims = ('time','lat','lon')
            tas_yearmeanVar = tas_yearmeanFile.createVariable('tas_yearmean', 'f', dims)
            tas_yearmeanVar.assignValue(tas_yearmean)
            tas_yearmeanFile.close()
        
        if self.pr_yearsum.getValue() == True :
            pr_yearsum = np.squeeze(cdo.yearsum(input=prFilePath, options='-f nc', returnMaArray='pr'))
            pr_yearsum = pr_yearsum * 60 * 60 * 24 # convert flux to amount
            pr_yearsum_filename = self.mktempfile(suffix='_pr_yearsum_.nc')
            output_files.append(pr_yearsum_filename)
            pr_yearsumFile = NetCDFFile(pr_yearsum_filename , 'w')
            pr_yearsumTimeDim = pr_yearsumFile.createDimension('time', pr_yearsum.shape[0])
            pr_yearsumLatDim = pr_yearsumFile.createDimension('lat', pr_yearsum.shape[1])
            pr_yearsumLonDim = pr_yearsumFile.createDimension('lon', pr_yearsum.shape[2])
            dims = ('time','lat','lon')
            pr_yearsumVar = pr_yearsumFile.createVariable('pr_yearsum', 'f', dims)
            pr_yearsumVar.assignValue(pr_yearsum)
            pr_yearsumFile.close()

        if self.tas_5to9mean.getValue() == True :
            tas_5to9mean = np.squeeze(cdo.yearsum(input = " ".join([cdo.selmon('5,6,7,8,9',input = tasFilePath)]), options='-f nc', returnMaArray='tas'))  
            
            tas_5to9mean_filename = self.mktempfile(suffix='_tas_5to9mean_.nc')
            output_files.append(tas_5to9mean_filename)
            tas_5to9meanFile = NetCDFFile(tas_5to9mean_filename , 'w')
            tas_5to9meanTimeDim = tas_5to9meanFile.createDimension('time', tas_5to9mean.shape[0])
            tas_5to9meanLatDim = tas_5to9meanFile.createDimension('lat', tas_5to9mean.shape[1])
            tas_5to9meanLonDim = tas_5to9meanFile.createDimension('lon', tas_5to9mean.shape[2])
            dims = ('time','lat','lon')
            tas_5to9meanVar = tas_5to9meanFile.createVariable('tas_5to9mean', 'f', dims)
            tas_5to9meanVar.assignValue(tas_5to9mean)
            tas_5to9meanFile.close()

        if self.tas_6to8mean.getValue() == True :
            tas_6to8mean = np.squeeze(cdo.yearsum(input  =  " ".join([cdo.selmon('6,7,8',input  =  tasFilePath)]), options='-f nc', returnMaArray='tas'))  #python
            
            tas_6to8mean_filename = self.mktempfile(suffix='_tas_6to8mean_.nc')
            output_files.append(tas_6to8mean_filename)
            tas_6to8meanFile = NetCDFFile(tas_6to8mean_filename , 'w')
            tas_6to8meanTimeDim = tas_6to8meanFile.createDimension('time', tas_6to8mean.shape[0])
            tas_6to8meanLatDim = tas_6to8meanFile.createDimension('lat', tas_6to8mean.shape[1])
            tas_6to8meanLonDim = tas_6to8meanFile.createDimension('lon', tas_6to8mean.shape[2])
            dims = ('time','lat','lon')
            tas_6to8meanVar = tas_6to8meanFile.createVariable('tas_6to8mean', 'f', dims)
            tas_6to8meanVar.assignValue(tas_6to8mean)
            tas_6to8meanFile.close()
            
        if self.pr_5to9sum.getValue() == True :
            pr_5to9sum = np.squeeze(cdo.yearsum(input  =  " ".join([cdo.selmon('5,6,7,8,9',input  =  prFilePath)]), options='-f nc', returnMaArray='pr'))  #python
            pr_5to9sum = pr_5to9sum * 60 * 60 * 24 # convert flux to amount
            
            pr_5to9sum_filename = self.mktempfile(suffix='_pr_5to9sum_.nc')
            output_files.append(pr_5to9sum_filename)
            pr_5to9sumFile = NetCDFFile(pr_5to9sum_filename , 'w')
            pr_5to9sumTimeDim = pr_5to9sumFile.createDimension('time', pr_5to9sum.shape[0])
            pr_5to9sumLatDim = pr_5to9sumFile.createDimension('lat', pr_5to9sum.shape[1])
            pr_5to9sumLonDim = pr_5to9sumFile.createDimension('lon', pr_5to9sum.shape[2])
            dims = ('time','lat','lon')
            pr_5to9sumVar = pr_5to9sumFile.createVariable('pr_5to9sum', 'f', dims)
            pr_5to9sumVar.assignValue(pr_5to9sum)
            pr_5to9sumFile.close()

        if self.pr_6to8sum.getValue() == True :
            pr_6to8sum = np.squeeze(cdo.yearsum(input  =  " ".join([cdo.selmon('6,7,8',input  =  prFilePath)] ), options='-f nc', returnMaArray='pr'))  #python
            pr_6to8sum = pr_6to8sum * 60 * 60 * 24 # convert flux to amount
            
            pr_6to8sum_filename = self.mktempfile(suffix='_pr_6to8sum_.nc')
            output_files.append(pr_6to8sum_filename)
            pr_6to8sumFile = NetCDFFile(pr_6to8sum_filename , 'w')
            pr_6to8sumTimeDim = pr_6to8sumFile.createDimension('time', pr_6to8sum.shape[0])
            pr_6to8sumLatDim = pr_6to8sumFile.createDimension('lat', pr_6to8sum.shape[1])
            pr_6to8sumLonDim = pr_6to8sumFile.createDimension('lon', pr_6to8sum.shape[2])
            dims = ('time','lat','lon')
            pr_6to8sumVar = pr_6to8sumFile.createVariable('pr_6to8sum', 'f', dims)
            pr_6to8sumVar.assignValue(pr_6to8sum)
            pr_6to8sumFile.close()
        
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
        
        # calculation of running mean 
        tas_runmean = np.squeeze(cdo.runmean(11,input=tasFilePath, options='-f nc', returnMaArray='tas'))
        
        
        
        # close inFiles
        tasFile.close()
        prFile.close() 

        # make tar archive
        tar_archive = self.mktempfile(suffix='.tar')
        tar = tarfile.open(tar_archive, "w")
        for name in output_files:
            tar.add(name, recursive=False)
        tar.close()
        
        # output
        self.status.set(msg="processing done", percentDone=90, propagate=True)
        self.output.setValue( tar_archive )
