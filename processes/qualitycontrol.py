
"""
Quality Control process. It calls the Quality Control scripts and returns the 
log file for now.
Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
import datetime
from malleefowl.process import WPSProcess
class Process(WPSProcess):
    def __init__(self):
        # init process
        abstractML =("The process takes in configuration files and a link"
                     +" to a to check file. The output is the log file for now.")

        WPSProcess.__init__(self,
            identifier = "QualityControl", 
            title="Quality Control",
            version = "0.1",
	    metadata=[],
	    abstract=abstractML)
             
        self.configs = self.addLiteralInput(identifier = "config",
                                           title = "Configuration files", 
                                           default="configuration file here",
                                           type=type(""),
                                           minOccurs = 1,#required
                                           #maxOccurs = 1,
                                           )
        self.files = self.addLiteralInput(identifier="file links", 
                                           title="Links to files", 
                                           default="Links here",
                                           type=types.StringType,
                                           minOccurs = 1,#required
                                           maxOccurs = 1,
                                           )
        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="Quality check result log.",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
        self.allOk = self.addLiteralOutput(identifier="allOk",
                                           title ="Everything is ok",
                                           abstract ="True if the Quality Check did not find errors.",
                                           default=False,
                                           type = type(True),
                                          )
    def execute(self):
        
        self.status.set(msg="Creating new file", percentDone=50, propagate=True)
        #filename=str(datetime.datetime.now()).replace(":","-").replace(" ","_").replace(".","_")
        output_ext = "log"
        #filename+=output_ext
        filename = self.mktempfile(suffix='.' + output_ext)
        file1 = file(filename,"w")
        self.status.set(msg="Writing to file", percentDone=80, propagate=True)
        file1.write("The file name is:"+filename)
        file1.write("Some random text")
        self.output.setValue(file1)
        self.allOk.setValue(self.evaluateResults())
        return #If execute() returns anything but null, it is considered as error and exception is called

    """ Return true if the Quality Check finds no errors.
        TODO: Implement it
    """
    def evaluateResults(self):
        return True
	    
	         
