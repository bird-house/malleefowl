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
            identifier = "de.csc.icclim",
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

        self.output = self.addComplexOutput(
            identifier="output",
            title="Indices Output tar",
            abstract="Indices Output file",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )
            
    def execute(self):
        
       from malleefowl import cscenv
        
        self.show_status('starting calcualtion of icclim indices', 5)        
  
        self.output.setValue( cscenv.indices(self.get_nc_files(), self.icclim_SU.getValue()) )
 
        self.show_status("processing done", 72)
