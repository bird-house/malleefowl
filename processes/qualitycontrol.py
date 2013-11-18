
"""
Quality Control process. It calls the QC and post processes the results of the analysis.

Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
import os
import subprocess 
import malleefowl.process 
import processes.qc.yaml2xml as y2x

            
                 
class TaskFileProcess(malleefowl.process.WPSProcess):
    """Process to run a qualtiy check on the specified data.  

    According to the PyWPS documentation minOccurs and maxOccurs default to 1. 
    Therefore they are ommited if they are 1.
    """
    def __init__(self):
        abstract_ml =("The process takes in configuration files and a link"
                     +" to a to check file. The output is the log file for now.")
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityControl", 
            title="Quality Control with taskfile",
            version = "0.1",
            metadata=[],
            abstract=abstract_ml)
            
        self.qc_script_path= self.addLiteralInput(
            identifier="qc_script_path",
            title="Quality Control Script path",
            abstract="The path must be on the processing server.",
            default="/home/tk/sandbox/qc/QC-0.4/scripts",
            type=types.StringType,
            )
 
        self.data_node= self.addLiteralInput(
            identifier="data_node",
            title="Data node",
            default="ipcc-ar5.dkrz.de",
            type=types.StringType,
            )

        self.index_node= self.addLiteralInput(
            identifier="index_node",
            title="Index node",
            default="esgf-data.dkrz.de",
            type=types.StringType,
            )

        self.replica= self.addLiteralInput(
            identifier="replica",
            title="Replica",
            default="false",
            type=types.StringType,
            )
            
        self.latest= self.addLiteralInput(
            identifier="latest",
            title="Latest",
            default="true",
            type=types.StringType,
            )

        self.access=self.addLiteralInput(
            identifier="access",
            title="Access",
            default="HTTPServer",
            type=types.StringType,
            )

        self.metadata_format=self.addLiteralInput(
            identifier="metadata_format",
            title="metadata_format",
            default="THREDDS",
            type=types.StringType,
            )

        self.xml_output_path= self.addLiteralInput(
            identifier="xml_output_path",
            title="Output path for the generated xml files.",
            abstract = "For now only one path for all result files is used",
            default="/home/tk/sandbox/xmlresults/",
            type=types.StringType,
            )

        self.task_file = self.addLiteralInput(
            identifier="task_file",
            title="Task file on the server",
            #default="/home/tk/sandbox/exampleqc/qc-test.task",
            default="/home/tk/sandbox/qc-yaml/test3/qc_CORDEX.task", 
            type=types.StringType,
            )

        self.project = self.addLiteralInput(
            identifier="project",
            title="Project",
            default="CORDEX",
            allowedValues=["CORDEX","NONE"],
            type=types.StringType,
            )
        
        self.database_location = self.addLiteralInput(
            identifier="database_location",
            title="Location of the database",
            default = "/home/tk/sandbox/databases/pidinfo.db",
            type=types.StringType,
            )

        self.args = self.addLiteralInput(
            identifier="args",
            title="optional arguments",
            default="",
            type=types.StringType,
            minOccurs=0,
            maxOccurs=1,
            )

             
        self.qc_info = self.addLiteralOutput(
            identifier="qc_info",
            title ="Quality Control Output",
            abstract ="Console output of the Quality Check.",
            default="No output found",
            type = types.StringType,
            )

        self.qc_call = self.addLiteralOutput(
            identifier="qc_call",
            title="qc_call",
            default="No call",
            type=types.StringType,
            )
        self.fail_count = self.addLiteralOutput(
            identifier="fail_count",
            title="Fail count",
            default=0,
            type=types.IntType,
            )
        self.omit_count = self.addLiteralOutput(
            identifier="omit_count",
            title="Omit count",
            default=0,
            type=types.IntType,
            )
        self.pass_count = self.addLiteralOutput(
            identifier="pass_count",
            title="Pass count",
            default=0,
            type=types.IntType,
            )
        self.fixed_count = self.addLiteralOutput(
            identifier="fixed_count",
            title="Fixed count",
            default=0,
            type=types.IntType,
            )
        self.logfiles_out = self.addComplexOutput(
            identifier="logfiles_out",
            title="Log of the Quality Control web process.",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

        self.created_files_log = self.addComplexOutput(
            identifier="created_files_log",
            title="Log of the created files with PID.",
            metadata=[],
            formats=[{"mimeType":"text/html"}],
            asReference=True,
            )
        self.has_issues = self.addLiteralOutput(
            identifier="has_issues",
            title="There is something wrong with the checked files.",
            default = False,
            type=types.BooleanType,
            )
                              
                              
    def execute(self):
        self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        input_list = [self.data_node,self.index_node,self.access,self.xml_output_path,self.replica,
                      self.latest,self.metadata_format,self.database_location]
        input_parameters = []
        for literal_input in input_list:
            input_parameters.append(literal_input.getValue())
        self.yaml_to_xml = y2x.Yaml2Xml(*input_parameters)
        #datan = self.data_node.getValue()
        #indexn = self.index_node.getValue()
        #xmlo = self.xml_output_path.getValue()
        #replica= self.replica.getValue()
        #latest= self.latest.getValue()
        #mdf = self.metadata_format.getValue()
        #access= self.access.getValue()
        #dbloc = self.database_location.getValue()
        #self.yaml_to_xml = y2x.Yaml2Xml(datan,indexn,access,xmlo,replica,latest,mdf,dbloc)

        qc_manager = self.qc_script_path.getValue()+"/qcManager"
        task_file_name = self.task_file.getValue()
        options_string = " -f "+task_file_name
        #an empty field in the web app returns <colander.null> in the Version from 17.10.2013
        if(self.args.getValue() != "<colander.null>"):
            options_string += " "+self.args.getValue()
        options_string+=" -P "+self.project.getValue()
        options_string+=" --pb --yaml"
        self.qc_call.setValue(qc_manager+options_string)    
        self.status.set(msg="Running Quality Control", percentDone=0, propagate=True)
        qc_manager_process=subprocess.Popen([qc_manager,options_string],stdout=subprocess.PIPE,bufsize=0)
        qc_manager_exit_code = barupdate(qc_manager_process,self.status)
        self.qc_info.setValue(qc_manager_exit_code)      
        #If the quality check finished correctly sarch for the log files an merge them. 
        if(qc_manager_exit_code == 0):
            has_issues = False
            path = get_QC_RESULTS(task_file_name)
            if(path!=""):
                logfilenames = getLogfileNames(path)
                log = self.mktempfile(suffix=".txt")
                logfile = open(log,'w')
                for log in logfilenames:
                    self.yaml_to_xml.clear()
                    self.yaml_to_xml.load_file(log)
                    self.yaml_to_xml.run()
                    
                    errors = self.yaml_to_xml.show_all_errors()
                    if(len(errors) > 0):
                        logfile.write(errors)
                        has_issues = True
                logfile.close()
                
                logcr = self.mktempfile(suffix=".html")
                logcreated = open(logcr,'w')
                #logcreated.write(self.yaml_to_xml.get_created_filenames())
                logcreated.close()
                self.logfiles_out.setValue(logfile)
                self.created_files_log.setValue(logcreated)
                gcdict = self.yaml_to_xml.global_count_by_checkresult
                self.fail_count.setValue(gcdict["fail"])
                self.omit_count.setValue(gcdict["omit"])
                self.pass_count.setValue(gcdict["pass"])
                self.fixed_count.setValue(gcdict["fixed"])
                self.has_issues.setValue(has_issues)
 
        return

""" HELPER METHODS """

""" search for the last QC_RESULTS line in the taskfile that is not a comment and return its value"""
def get_QC_RESULTS(task_file_name):
    path = ""
    taskfile = open(task_file_name)
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
    dir_list=os.listdir(check_logs_Path)
    fullPath_LogList = [check_logs_Path+k for k in dir_list if '.log' in k]
    return fullPath_LogList


"""
    Updates the progress bar based on the output of the program.
    Currently the output consists of current file and number of files on each line.
"""
def barupdate(qc_manager_process,status):
    poll = qc_manager_process.poll()
    while poll is None:
        line = qc_manager_process.stdout.readline()
        poll = qc_manager_process.poll() 
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
