
"""
Quality Control process. It calls the Quality Control scripts and returns the 
log file for now.
Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
import os
import subprocess 
import malleefowl.process 
import processes.qc.Yaml2Xml as y2x

            
                 
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
 
        self.data_node= self.addLiteralInput(
            identifier="data_node",
            title="Data node",
            default="ipcc-ar5.dkrz.de",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.index_node= self.addLiteralInput(
            identifier="index_node",
            title="Index node",
            default="esgf-data.dkrz.de",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.replica= self.addLiteralInput(
            identifier="replica",
            title="Replica",
            default="false",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.latest= self.addLiteralInput(
            identifier="latest",
            title="Latest",
            default="true",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.access=self.addLiteralInput(
            identifier="access",
            title="Access",
            default="HTTPServer",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.metadata_format=self.addLiteralInput(
            identifier="metadata_format",
            title="metadata_format",
            default="THREDDS",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.xmlOutputPath= self.addLiteralInput(
            identifier="xmlOutputPath",
            title="Output path for the generated xml files.",
            abstract = "For now only one path for all result files is used",
            default="/home/tk/sandbox/xmlresults/",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.taskFile = self.addLiteralInput(
            identifier="TASK_FILE",
            title="Task file on the server",
            #default="/home/tk/sandbox/exampleqc/qc-test.task",
            default="/home/tk/sandbox/qc-yaml/test3/qc_CORDEX.task", 
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
        self.fixedCount = self.addLiteralOutput(
            identifier="fixedCount",
            title="Fixed count",
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

        self.createdFilesLog = self.addComplexOutput(
            identifier="CreatedFilesLog",
            title="Log of the created files with PID.",
            metadata=[],
            formats=[{"mimeType":"text/html"}],
            asReference=True,
            )
        self.hasIssues = self.addLiteralOutput(
            identifier="hasIssues",
            title="There is something wrong with the checked files.",
            default = False,
            type=types.BooleanType,
            )
                              
                              
    def execute(self):
        self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        datan = self.data_node.getValue()
        indexn = self.index_node.getValue()
        xmlo = self.xmlOutputPath.getValue()
        replica= self.replica.getValue()
        latest= self.latest.getValue()
        mdf = self.metadata_format.getValue()
        access= self.access.getValue()
        self.yamltoxml = y2x.Yaml2Xml(datan,indexn,access,xmlo,replica,latest,mdf)

        qcManager = self.qcScriptPath.getValue()+"/qcManager"
        taskfileName = self.taskFile.getValue()
        optionsString = " -f "+taskfileName
        #an empty field in the web app returns <colander.null> in the Version from 17.10.2013
        if(self.args.getValue() != "<colander.null>"):
            optionsString += " "+self.args.getValue()
        optionsString+=" -P "+self.project.getValue()
        optionsString+=" --pb --yaml"
        self.qcCall.setValue(qcManager+optionsString)    
        self.status.set(msg="Running Quality Control", percentDone=0, propagate=True)
        qcManagerProcess=subprocess.Popen([qcManager,optionsString], stdout=subprocess.PIPE,bufsize=0)
        qcManagerExitCode = barupdate(qcManagerProcess,self.status)
        self.qcInfo.setValue(qcManagerExitCode)      
        #If the quality check finished correctly sarch for the log files an merge them. 
        if(qcManagerExitCode == 0):
            hasIssues = False
            path = get_QC_RESULTS(taskfileName)
            if(path!=""):
                logfilenames = getLogfileNames(path)
                log = self.mktempfile(suffix=".txt")
                logfile = open(log,'w')
                for log in logfilenames:
                    self.yamltoxml.clear()
                    self.yamltoxml.loadFile(log)
                    self.yamltoxml.toXML()
                    
                    errors = self.yamltoxml.showAllErrors()
                    if(len(errors) > 0):
                        logfile.write(errors)
                        hasIssues = True
                    
                logfile.close()
                
                logcr = self.mktempfile(suffix=".html")
                logcreated = open(logcr,'w')
                logcreated.write(self.yamltoxml.getCreatedFilenames())
                logcreated.close()
                self.logfilesOut.setValue(logfile)
                self.createdFilesLog.setValue(logcreated)
                gcdict = self.yamltoxml.GLOBALCHECKSUMMARY
                self.failCount.setValue(gcdict["fail"])
                self.omitCount.setValue(gcdict["omit"])
                self.passCount.setValue(gcdict["pass"])
                self.fixedCount.setValue(gcdict["fixed"])
                self.hasIssues.setValue(hasIssues)
 
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

def getLogfileNames(path):
    check_logs_Path = path+"/check_logs/"
    dirList=os.listdir(check_logs_Path)
    fullPath_LogList = [check_logs_Path+k for k in dirList if '.log' in k]
    return fullPath_LogList


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
        #There might be empty lines or lines that are not pb lines. The try simplifies the
        #checking if the entries exist and if they are numbers. It is assumed that it only
        #throws an exception rarely. 
        try:
            current = float(splitLine[0])
            end = float(splitLine[1])
            perc = int(current*100.0/end)
            status.set(msg="Checking Files", percentDone=perc, propagate=True)
        except:
            pass
    return poll
