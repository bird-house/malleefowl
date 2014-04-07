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
                  'esgfilter': 'variable:tasmax , variable:tasmin , time_frequency:day project:CMIP5 , project:CORDEX', 
                  'esgquery': '' 
                  },
            )

        # Literal Input Data
        # ------------------
        

        self.SU = self.addLiteralInput(
            identifier="SU",
            title="Nr of summer days",
            abstract="max day temperatur >= 25 C'\n'nput data is a datafile with daily tasmax",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )
            
        self.FD = self.addLiteralInput(
            identifier="FD",
            title="Nr of frost days",
            abstract="min day temperatur > 0 C'\n'input data is a datafile with daily tasmin",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )
            
            
        # defined in WorkflowProcess ...

        # complex output
        # -------------
        self.output = self.addComplexOutput(
            identifier="output",
            title="indice log",
            abstract="indice log",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
            
    def execute(self):
        
        from malleefowl import cscenv, utils
        import os

        self.show_status('starting calcualtion of icclim indices', 5)        
        
        self.output.setValue( cscenv.indices(self.get_nc_files(), self.SU.getValue())
        
        self.show_status("processing done", 100)