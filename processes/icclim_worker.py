#from malleefowl.process import WorkerProcess
import malleefowl.process 
import subprocess
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class IndicesProcess(malleefowl.process.WorkerProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        malleefowl.process.WorkerProcess.__init__(self, 
            identifier = "de.csc.icclim_worker",
            title="Calculation of climate indices based on icclim",
            version = "0.1",
            #storeSupported = "true",   # async
            #statusSupported = "true",  # retrieve status, needs to be true for async 
            ## TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Just testing a python script to test icclim",
            #extra_metadata={
                  #'esgfilter': 'variable:tas, variable:evspsblpot, variable:huss, variable:ps, variable:pr, variable:sftlf, time_frequency:day', 
                  #'esgquery': 'data_node:esg-dn1.nsc.liu.se' 
                  #},
            extra_metadata={
                  'esgfilter': 'variable:tasmax',  #institute:MPI-M ,variable:pr,
                  'esgquery': 'time_frequency:day AND project:CMIP5' # institute:MPI-M AND project:CMPI5
                  },
            )

        # Literal Input Data
        # ------------------
        

        self.icclim_SU = self.addLiteralInput(
            identifier="icclim_SU",
            title="summer days",
            abstract="input data is a datafile with daily tasmax",
            type=type(False),
            minOccurs=1,
            maxOccurs=0,
            )
            
        # defined in WorkflowProcess ...

        # complex output
        # -------------

        self.icclim_output = self.addComplexOutput(
            identifier="icclim_output",
            title="Indices Output tar",
            abstract="Indices Output file",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )
            
    def execute(self):
        
        from os import curdir, path
        import datetime
        import ocgis
        import datetime
        import icclim

        self.show_status('starting calcualtion of icclim indices', 5)
        
        # get the appropriate files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = Dataset(nc_file)
            if "tasmax" in ds.variables.keys():
                tasmaxFilePath = nc_file
            else:
                raise Exception("input netcdf file has not variable tasmax")
        
        self.show_status('get files ...', 7)
        
        
        def sel_path():
            # result location
            ocgis.env.DIR_OUTPUT = path.abspath(curdir)
            ocgis.env.OVERWRITE = True
        sel_path()

        
        self.show_status('created output file initially', 7)

        # simple precesses realized by cdo commands:
        if self.icclim_SU.getValue() == True :
            #dt1 = datetime.datetime(2077,01,01)
            #dt2 = datetime.datetime(2078,12,31)
            rd = ocgis.RequestDataset(tasmaxFilePath, 'tasmax') # time_range=[dt1, dt2]
            group = ['year']
            calc_icclim = [{'func':'icclim_SU','name':'SU'}]
            SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix='my_test_SU', output_format='nc', add_auxiliary_files=False).execute()
        
        print res 
        
        self.show_status("all indices has been calculated", 50)
        self.show_status("current directory is %s" % path.abspath(curdir), 50)
        
        # make tar archive
        #tar_archive = self.mktempfile(suffix='.tar')
        #tar = tarfile.open(tar_archive, "w")
        #for name in output_files:
            #tar.add(name, arcname = name.replace(self.working_dir, ""))
        #tar.close()
        
        self.show_status("make tar archive ... done", 50)
        
        
        #mystring.replace('\r\n', '')
        
        # output
        self.show_status("processing done", 52)
        self.icclim_output.setValue( SU_file )
        logger.debug('tar archive = %s' %( tas_yearmean_filename))
