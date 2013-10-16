
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

        #Optional parameters that do not require additonal arguments
        self.boolOptParam= ['','APPLY_MAXIMUM_DATE_RANGE','SHOW_CONF','SHOW_EXP']
        #Optional parameters requiring additional arguments
        self.argOptionalParameters=['NEXT','SELECT', 'LOCK']

        self.projectName = self.addLiteralInput(
            identifier="PROJECT",
            title="PROJECT",
            default="CORDEX",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.projectData= self.addLiteralInput(
            identifier="PROJECT_DATA",
            title="PROJECT_DATA",
            abstract = "Root directory of the to check data on the server.",
            default="/home/tk/sandbox/ncfiles",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.QCResults= self.addLiteralInput(
            identifier="QC_RESULTS",
            title="QC_RESULTS",
            abstract = "Result directory",
            default="/home/tk/sandbox/results",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.qcScriptPath= self.addLiteralInput(
            identifier="QCScriptPath",
            title="Quality Control Script path",
            abstract="Path where the Quality Control scripts are located.",
            default="/home/tk/sandbox/qc/QC-0.4/scripts",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
 
        #self.select = self.addLiteralInput(
        #    identifier="SELECT",
        #    title="SELECT",
        #    default=".*",
        #    type=types.StringType,
        #    minOccurs=1,
        #    maxOccurs=1,
        #    )

        self.boolOptionalParameters = self.addLiteralInput(
            identifier="OPTIONAL_PARAMETERS_NOARGS",
            title="optional parameters without arguments",
            default="",
            allowedValues= self.boolOptParam,
            type=types.StringType,
            minOccurs=0,
            maxOccurs=100,
            )
             
        self.availableOptionalParameters = self.addLiteralInput(
            identifier="AVAILABLE_OPTIONAL_PARAMETERS",
            title="Available optional parameters",
            default="",
            abstract="A list of useable parameters in the following fields.",
            allowedValues= self.argOptionalParameters,
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.optionalParameters = self.addLiteralInput(
            identifier="ARGS_OPTIONAL_PARAMETERS",
            title="Available optional parameters",
            default="",
            abstract="A list of useable parameters in the following fields.",
            type=types.StringType,
            minOccurs=0,
            maxOccurs=100,
            )

        #self.ncFileIn = self.addComplexInput(
        #    identifier="ncFile",
        #    title="nc file",
        #    abstract="The file to be analysed",
        #    )
        #self.ncFileOut = self.addComplexOutput(
        #    identifier="ncFile",
        #    title="nc file",
        #    abstract="The file to be analysed",
        #    )

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

        self.qcCall = self.addLiteralOutput(
            identifier="qcCall",
            title="qcCall",
            default="No call",
            type=types.StringType,
            )
        self.logfileOut = self.addComplexOutput(
            identifier="ProcessLog",
            title="Log of the Quality Control web process.",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

                              
                              
    def execute(self):
        
        self.status.set(msg="Initiate process", percentDone=5, propagate=True)
        #filename=str(datetime.datetime.now()).replace(":","-").replace(" ","_").replace(".","_")
        output_ext = "log"
        #filename+=output_ext
        filename = self.mktempfile(suffix='.' + output_ext)
        #file1 = file(filename,"w")
        #file1.write("The file name is:"+filename)
        #file1.write("Some random text")

        logfile = file(self.mktempfile(suffix=".txt"))
        qcManger = self.qcScriptPath.getValue()+"/qcManager"
        optionsString = " -E_PROJECT_DATA="+self.projectData.getValue()
        #optionsString +=" -E_SELECT="+self.select.getValue()
        optionsString +=" -E_QC_RESULTS="+self.QCResults.getValue()
        optionsString +=" -E_PROJECT="+self.projectName.getValue()
        #optionsString +=" -E_CHECK_MODE=data"
        options = self.optionalParameters.getValue()
        for option in options:
            valid = False
            #if the the option parameter starts with a valid option add the parameter
            for val in self.argOptionalParameters:
                if(option[0:len(val)]==val):
                    valid=True
                    break
            if valid:
                optionsString += " -E_"+str(option)
            else:
                logfile.write("Not a valid parameter: "+option)
        #shutil.copy(self.ncFileIn.getValue(),"/home/tk/sandbox/ncfiles/test/test.nc")
        #self.ncFileOut.setValue(self.ncFileIn.getValue())
        self.qcCall.setValue(qcManger+optionsString)    
        self.status.set(msg="Running Quality Control", percentDone=20, propagate=True)
        qcMangerOutput = os.system(qcManger+optionsString)
        self.qcInfo.setValue(qcMangerOutput)      

 
        self.logfileOut.setValue(logfile)
        self.allOk.setValue(self.evaluateResults())
        #file1.close()
        logfile.close()
        return #If execute() returns anything but null, it is considered as error and exception is called

    """ Return true if the Quality Check finds no errors.
        TODO: Implement it
    """
    def evaluateResults(self):
        return True
            
                 
