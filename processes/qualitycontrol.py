
"""
Quality Control process. It calls the Quality Control scripts and returns the 
log file for now.
Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
#import datetime
import os
import subprocess 
import time
#from malleefowl.process import WPSProcess
import malleefowl.process 
class NoFileProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        # init process
        abstractML=("The process takes in a single file and optional parameters"
                   +" to run a quality check. The summary shows how many FAIL, OMIT and PASS occured.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityControlNTF", 
            title="Quality Control for single files",
            version = "0.1",
            metadata=[],
            abstract=abstractML)
               
        self.argOptionalParameters=["APPLY_MAXIMUM_DATE_RANGE",]
                
        self.projectName = self.addLiteralInput(
            identifier="PROJECT",
            title="PROJECT",
            default="CORDEX",
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
 
        self.fileurl= self.addLiteralInput(
            identifier="fileurl",
            title="NetCDF file to analyse",
            default="http://www.unidata.ucar.edu/software/netcdf/examples/sresa1b_ncar_ccsm3_0_run1_200001.nc",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.optionalParameters = self.addLiteralInput(
            identifier="OPTIONAL_PARAMETERS",
            title="optional parameters",
            default="",
            type=types.StringType,
            minOccurs=0,
            maxOccurs=1,
            )
             
        self.availableOptionalParameters = self.addLiteralInput(
            identifier="AVAILABLE_OPTIONAL_PARAMETERS",
            title="Available optional parameters",
            default="",
            abstract="A list of useable parameters.",
            allowedValues= self.argOptionalParameters,
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
        self.tempPath = self.addLiteralInput(
            identifier="Path_for_temporary_storage",
            title="Path for temporary storage",
            default = "/home/tk/sandbox/temp",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )


        self.qcInfo = self.addLiteralOutput(
            identifier="QualityControlOutput",
            title ="Quality Control Output",
            abstract ="Console output of the Quality Check.",
            default="No output found",
            type = types.StringType,
            )
        #self.CORDEX_qcconf = ["APPLY_MAXIMUM_DATE_RANGE","PROJECT_TABLE_PREFIX=pt"]

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
        filename = self.mktempfile(suffix='.' + output_ext)
        tempPath = self.tempPath.getValue()
        resultsdir=tempPath+"/results"
        if not os.path.isdir(resultsdir):
            os.makedirs(resultsdir)
        datadir = tempPath+"/data"
        if not os.path.isdir(datadir):
            os.makedirs(datadir)
        ncfileurl = self.fileurl.getValue()
        wgetStatus=os.system("wget "+ncfileurl+" -P "+datadir)
        qcManager = self.qcScriptPath.getValue()+"/qcManager"
        optionsString = " -E_PROJECT_DATA="+datadir#+self.projectData.getValue()
        optionsString +=" -E_QC_RESULTS="+resultsdir#self.QCResults.getValue()
        optionsString +=" -P "+self.projectName.getValue()
        optionsString +=" -E_CHECK_MODE=data"
        options = self.optionalParameters.getValue()
        if(options != "<colander.null>"):
            optionsSplit=options.split(" ")
            for option in optionsSplit:
                valid = False
                #if the the option parameter starts with a valid option add the parameter
                for val in self.argOptionalParameters:
                    if(option[0:len(val)]==val):
                        valid=True
                        break
                if valid:
                    optionsString += " -E_"+str(option)
        self.qcCall.setValue(qcManager+optionsString)    
        self.status.set(msg="Running Quality Control", percentDone=20, propagate=True)
        qcManagerOutput = os.system(qcManager+optionsString)
        self.qcInfo.setValue(qcManagerOutput)      
        if(qcManagerOutput == 0):
            path = resultsdir 
            if(path!=""):
                logfile = self.mktempfile(suffix=".txt")
                (failCount,omitCount,passCount) = mergeAndCountLogs(path,logfile)
                self.logfilesOut.setValue(logfile)
                self.failCount.setValue(failCount)
                self.omitCount.setValue(omitCount)
                self.passCount.setValue(passCount)

 
        return #If execute() returns anything but null, it is considered as error and exception is called

            
                 
class TaskFileProcess(malleefowl.process.WPSProcess):
    def __init__(self):
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
            #default="/home/tk/sandbox/exampleqc/qc-test.task",
            default="/home/tk/sandbox/qc/QC-0.4/2exampleqc/qc-test.task", 
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
             
        self.qcInfo = self.addLiteralOutput(
            identifier="QualityControlOutput",
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

        self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        output_ext = "log"

        qcManager = self.qcScriptPath.getValue()+"/qcManager"
        taskfileName = self.taskFile.getValue()
        optionsString = " -f "+taskfileName
        #an empty field in the web app returns <colander.null> in the Version from 17.10.2013
        if(self.args.getValue() != "<colander.null>"):
            optionsString += " "+self.args.getValue()
        optionsString+=" -P "+self.project.getValue()
        optionsString+=" --pb"
        self.qcCall.setValue(qcManager+optionsString)    
        self.status.set(msg="Running Quality Control", percentDone=0, propagate=True)
        qcManagerProcess=subprocess.Popen([qcManager,optionsString], stdout=subprocess.PIPE,bufsize=0)
        qcManagerExitCode = barupdate(qcManagerProcess,self.status)
        self.qcInfo.setValue(qcManagerExitCode)      
        #If the quality check finished correctly sarch for the log files an merge them. 
        if(qcManagerExitCode == 0):
            path = get_QC_RESULTS(taskfileName)
            if(path!=""):
                logfile = self.mktempfile(suffix=".txt")
                (failCount,omitCount,passCount) = mergeAndCountLogs(path,logfile)
                self.logfilesOut.setValue(logfile)
                self.failCount.setValue(failCount)
                self.omitCount.setValue(omitCount)
                self.passCount.setValue(passCount)
 
        return

""" HELPER METHODS """

""" search for the last QC_RESULTS line in the taskfile that is not a comment and return its value"""
def get_QC_RESULTS(taskfileName):
    path = ""
    taskfile = open(taskfileName)
    filedata = taskfile.readlines()
    taskfile.close()
    qcres = [row for row in filedata if "QC_RESULTS" in row and row.lstrip()[0]!='#']
    if(len(qcres) >0):#just to be sure that a QC_RESULTS variable exists in the file.
        qcresults=qcres[-1].strip(' ').rstrip('\n')
        qcrsplit = qcresults.split('=')
        path = qcrsplit[1]
    return path


def mergeAndCountLogs(path,targetfileName):
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
                They always have the same format, which allows for a simple parsing."""
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

"""
    Updates the progress bar based on the output of the program.
    Currently the output consists of current file and number of files on each line.
"""
def barupdate(qcManagerProcess,status):
    poll = qcManagerProcess.poll()
    while poll is None:
        line = qcManagerProcess.stdout.readline()
        poll = qcManagerProcess.poll() 
        if not line:
           break
        splitLine=line.rstrip('n').split(' ')
        current = float(splitLine[0])
        end = float(splitLine[1])
        perc = int(current*100.0/end)
        status.set(msg="Checking Files", percentDone=perc, propagate=True)
    return poll
