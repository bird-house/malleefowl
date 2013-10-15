
"""
Quality Control process. It calls the Quality Control scripts and returns the 
log file for now.
Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
#import datetime
import os
import shutil
#from malleefowl.process import WPSProcess
import malleefowl.process 
class Process(malleefowl.process.WPSProcess):
    def __init__(self):
        # init process
        abstractML =("The process takes in configuration files and a link"
                     +" to a to check file. The output is the log file for now.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityControl", 
            title="Quality Control",
            version = "0.1",
            metadata=[],
            abstract=abstractML)
             
        #self.configs = self.addLiteralInput(identifier = "config",
        #                                   title = "Configuration files", 
        #                                   default="configuration file here",
        #                                   type=type(""),
        #                                   minOccurs = 1,#required
        #                                   #maxOccurs = 1,
        #                                   )
        #self.files = self.addLiteralInput(identifier="file links", 
        #                                   title="Links to files", 
        #                                   default="Links here",
        #                                   type=types.StringType,
        #                                   minOccurs = 1,#required
        #                                   maxOccurs = 1,
        #                                   )

        self.projectData = self.addLiteralInput(
            identifier="PROJECT_DATA",
            title="PROJECT_DATA",
            default="Root of the data to be processed",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.select = self.addLiteralInput(
            identifier="SELECT",
            title="SELECT",
            default=".*",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        #TODO turn into a list of parameters
        self.optionalParameters = self.addLiteralInput(
            identifier="OPTIONAL_PARAMETERS",
            title="Optional parameters",
            default="",
            type=types.StringType,
            minOccurs=0,
            maxOccurs=100,
            )
             

        self.ncFileIn = self.addComplexInput(
            identifier="ncFile",
            title="nc file",
            abstract="The file to be analysed",
            )
        self.ncFileOut = self.addComplexOutput(
            identifier="ncFile",
            title="nc file",
            abstract="The file to be analysed",
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
        self.qcInfo = self.addLiteralOutput(identifier="QualityControlOutput",
                                           title ="Quality Control Output",
                                           abstract ="Console output of the Quality Check.",
                                           default="No output found",
                                           type = type("Hello World!"),
                                          )
        #self.CORDEX_qcconf = ["APPLY_MAXIMUM_DATE_RANGE","PROJECT_TABLE_PREFIX=pt"]

        #temporary helper literals
        self.qcScriptPath= self.addLiteralInput(
            identifier="QCScriptPath",
            title="Quality Control Script path",
            abstract="Path where the Quality Control scripts are located.",
            default="/home/tk/sandbox/qc/QC-0.4/scripts",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.qcCall = self.addLiteralOutput(
            identifier="qcCall",
            title="qcCall:",
            default="No call",
            type=types.StringType,
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
        #hardcoded test. For now only returns a 0 for successfully executed.
        qcManger = self.qcScriptPath.getValue()+"/qcManager"
#        qcMangerOutput = os.system("/home/tk/sandbox/qc/QC-0.4/scripts/qcManager -f /home/tk/sandbox/qc/QC-0.4/myExample/my-test.task -E CHECK_MODE=data") 
        optionsString = " -E_PROJECT_DATA=/home/tk/sandbox/ncfiles"#+self.projectData.getValue()
        optionsString +=" -E_SELECT="+self.select.getValue()
        optionsString +=" -E_QC_RESULTS=/home/tk/sandbox/qc/QC-0.4/myExample/results"
        optionsString +=" -E_PROJECT=CORDEX"
        #optionsString +=" -E_CHECK_MODE=data"
        options = self.optionalParameters.getValue()
        for val in options:
            optionsString += " -E_"+str(val)
        shutil.copy(self.ncFileIn.getValue(),"/home/tk/sandbox/ncfiles/test/test.nc")
        self.ncFileOut.setValue(self.ncFileIn.getValue())
        self.qcCall.setValue(qcManger+optionsString)    
        qcMangerOutput = os.system(qcManger+optionsString)
        self.qcInfo.setValue(qcMangerOutput)      

 
        self.output.setValue(file1)
        self.allOk.setValue(self.evaluateResults())
        file1.close()
        return #If execute() returns anything but null, it is considered as error and exception is called

    """ Return true if the Quality Check finds no errors.
        TODO: Implement it
    """
    def evaluateResults(self):
        return True
            
                 
