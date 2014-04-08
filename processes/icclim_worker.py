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
            title="Calculation of climate indices (ECA)",
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
                  'esgfilter': 'variable:tasmax, variable:tasmin, variable:tas, project:CMIP5, project:CORDEX',  
                  'esgquery': ' time_frequency:day' 
                  },
            )

        # Literal Input Data
        # ------------------
        
    
        self.TG = self.addLiteralInput(
            identifier="TG",
            title="TG",
            abstract="Mean of mean temperatur (tas files as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )
            
        self.TX = self.addLiteralInput(
            identifier="TX",
            title="TX",
            abstract="mean of max temperatur (tasmax files as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )
            
        self.TN = self.addLiteralInput(
            identifier="TN",
            title="TN",
            abstract="Mean over min temperatur (tasmin files as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )

        self.SU = self.addLiteralInput(
            identifier="SU",
            title="SU",
            abstract="Nr of summer days (tasmax files as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )

            
        self.ETR = self.addLiteralInput(
            identifier="ETR",
            title="ETR",
            abstract="Extrem temperture range (tasmax and tasmin as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )

        self.DTR = self.addLiteralInput(
            identifier="DTR",
            title="DTR",
            abstract="Mean of diurnal temperaure range (tasmax and tasmin as input files)",
            type=type(False),
            minOccurs=0,
            maxOccurs=0,
            )

        self.HI = self.addLiteralInput(
            identifier="HI",
            title="HI",
            abstract="Huglin Index (tas and tasmax as input files)",
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
        
        self.output.setValue( cscenv.indices(self.get_nc_files(), self.TG.getValue(), self.TN.getValue(), self.TX.getValue(), self.SU.getValue(), self.DTR.getValue(), self.ETR.getValue(), self.HI.getValue() )) 
        
        #  
        
        self.show_status("processing done", 100)