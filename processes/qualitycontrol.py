
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
class NoFileProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        # init process
        abstractML =("The process takes in configuration files and a link"
                     +" to a to check file. The output is the log file for now.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityControlNTF", 
            title="Quality Control without taskfile",
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
            title="Optional parameters with arguments",
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
            
                 
class TaskFileProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        # init process
        abstractML =("The process takes in configuration files and a link"
                     +" to a to check file. The output is the log file for now.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityControlTF", 
            title="Quality Control with taskfile",
            version = "0.1",
            metadata=[],
            abstract=abstractML)
             

        self.qcScriptPath= self.addLiteralInput(
            identifier="QCScriptPath",
            title="Quality Control Script path",
            abstract="The path must be on the processing server.",
            default="/home/tk/sandbox/qc/QC-0.4/scripts",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
 

        self.taskFile = self.addLiteralInput(
            identifier="TASK_FILE",
            title="Task file on the server",
            default="/home/tk/sandbox/exampleqc/qc-test.task",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.project = self.addLiteralInput(
            identifier="PROJECT",
            title="Project",
            default="CORDEX",
            allowedValues=["CORDEX","NONE"],
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.args = self.addLiteralInput(
            identifier="OPTIONAL_ARGUMENTS",
            title="optional arguments",
            abstract="e.g. -E_SHOW_CONF -E_CLEAR",
            default="",
            type=types.StringType,
            minOccurs=0,
            maxOccurs=1,
            )
             

        self.allOk = self.addLiteralOutput(identifier="allOk",
                                           title ="Everything is ok",
                                           abstract ="True if the Quality Check did not find errors.",
                                           default=False,
                                           type = types.BooleanType,
                                          )
        self.qcInfo = self.addLiteralOutput(identifier="QualityControlOutput",
                                           title ="Quality Control Output",
                                           abstract ="Console output of the Quality Check.",
                                           default="No output found",
                                           type = types.StringType,
                                          )

        self.qcCall = self.addLiteralOutput(
            identifier="qcCall",
            title="qcCall",
            default="No call",
            type=types.StringType,
            )
        self.failCount = self.addLiteralOutput(
            identifier="failCount",
            title="Fail count",
            default=0,
            type=types.IntType,
            )
        self.omitCount = self.addLiteralOutput(
            identifier="omitCount",
            title="Omit count",
            default=0,
            type=types.IntType,
            )
        self.passCount = self.addLiteralOutput(
            identifier="passCount",
            title="Pass count",
            default=0,
            type=types.IntType,
            )
        self.logfilesOut = self.addComplexOutput(
            identifier="ProcessLog",
            title="Log of the Quality Control web process.",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

                              
                              
    def execute(self):
        
        self.status.set(msg="Initiate process", percentDone=5, propagate=True)
        output_ext = "log"

        qcManager = self.qcScriptPath.getValue()+"/qcManager"
        taskfileName = self.taskFile.getValue()
        optionsString = " -f "+taskfileName
        #an empty field in the web app returns <colander.null> in the Version from 17.10.2013
        if(self.args.getValue() != "<colander.null>"):
            optionsString += " "+self.args.getValue()
        optionsString+=" -P "+self.project.getValue()
        self.qcCall.setValue(qcManager+optionsString)    
        self.status.set(msg="Running Quality Control", percentDone=20, propagate=True)
        qcManagerOutput = os.system(qcManager+optionsString)
        self.qcInfo.setValue(qcManagerOutput)      
         
        #After the processing search for the log files
        if(qcManagerOutput == 0):
            path = self.get_QC_RESULTS(taskfileName)
            if(path!=""):
                logfile = self.mktempfile(suffix=".txt")
                (failCount,omitCount,passCount) = self.mergeAndCountLogs(path,logfile)
                self.logfilesOut.setValue(logfile)
                self.failCount.setValue(failCount)
                self.omitCount.setValue(omitCount)
                self.passCount.setValue(passCount)
 
        return

    """ search for the first QC_RESULTS line that is not a comment and return its value"""
    def get_QC_RESULTS(self,taskfileName):
        path = ""
        taskfile = open(taskfileName)
        filedata = taskfile.readlines()
        taskfile.close()
        qcres = [row for row in filedata if "QC_RESULTS" in row and row.lstrip()[0]!='#']
        if(len(qcres) >0):#just to be sure that a QC_RESULTS variable exists in the file.
            qcresults=qcres[0].strip(' ').rstrip('\n')
            qcrsplit = qcresults.split('=')
            path = qcrsplit[1]
        return path


    def mergeAndCountLogs(self,path,targetfileName):
        logFile = open(targetfileName,'w')
        (fails,omits,passes) = (0,0,0) 
        check_logs_Path= path+"/check_logs/"
        dirList=os.listdir(check_logs_Path)
        fullPath_LogList = [check_logs_Path+k for k in dirList if '.log' in k]
        for log in fullPath_LogList:
            logFile.write("----FILE "+log+" CONTAINS:----\n")
            tempFile= file(log,'r')
            lines = tempFile.readlines()
            
            for line in lines:
                """ Search for the lines with CHECK:: e.g.
                    CHECK:: meta data: FAIL,    time: PASS,     data: PASS
                    0       1    2     3        4     5         6     7
                    And count the FAIL, PASS and OMIT.
                    They always have the same formating, which allows for a simple parsing."""
                if(line[:7]=="CHECK::"):
                    lrs = line.replace('\t',' ').split(' ')
                    resList=[lrs[3].rstrip(','),lrs[5].rstrip(','),lrs[7].rstrip('\n')]
                    for word in resList:
                        if(word=="PASS"):
                            passes+=1
                        elif(word=="OMIT"):
                            omits+=1
                        elif(word=="FAIL"):
                            fails+=1
                        #else:
                        #    logFile.write("***MISS***:"+str(word)+" "+str(type(word)))
                logFile.write(line)
            tempFile.close()
        logFile.close()
        return (fails,omits,passes)
