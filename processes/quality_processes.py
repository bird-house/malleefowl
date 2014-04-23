"""
Quality Control processes.

Author: Tobias Kipp (kipp@dkrz.de)
Creation date: 21.01.2014
"""
import types
import malleefowl.process 
import qc_processes.qcprocesses as qcprocesses
import pidmanager.pidmanager as pidmanager
#import qc_processes.directory2datasetyaml as directory2datasetyaml
from pywps import config

import os
from malleefowl import tokenmgr
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

curdir = os.path.dirname(__file__)
climdapsabs = os.path.abspath(os.path.join(curdir,".."))

#DATABASE_LOCATION for mongodb
DATABASE_LOCATION = {"host": config.getConfigValue("malleefowl","database_location_host"),
                    "port": int(config.getConfigValue("malleefowl","database_location_port")),
                    "databasename": config.getConfigValue("malleefowl","database_location_databasename")
                     }
WORK_DIR = config.getConfigValue("malleefowl", "work_directory")

class UserInitProcess(malleefowl.process.WPSProcess):
    """
    The process is not intended for use with the generic tool
    """
    def __init__(self):

       
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Init",
            title = "Quality Init",
            version = "2014.04.15",
            metadata = [],
            abstract = "If the given directory is valid included files and datasets receive a PID.")

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            abstract = ("Name to access your own processing directory.")
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            default = "Needed_if_not_defaultuser",
            type = types.StringType,
            abstract = "The token authenticates you as the user. defaultuser accepts any token."
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            default = "web1",
            type = types.StringType,
            abstract = ("To run multiple processes in parallel each requires its own directory." +
                        "The directory is placed in your user directory. If defaultuser is choosen, " +
                        "collisions with names are more likely. Be careful when choosing a name."),
            minOccurs = 1, 
            maxOccurs = 1,
            )
                
        self.data_path = self.addLiteralInput(
            identifier = "data_path",
            title = "Root path of the to check data",
            default = os.path.join(climdapsabs,"examples/data/CORDEX"),
            type = types.StringType,
            minOccurs = 1,
            maxOccurs = 1,
            )

        self.project = self.addLiteralInput(
            identifier = "project",
            title = "Project",
            abstract = "Currently only CORDEX is fully supported.",
            default = "CORDEX",
            allowedValues = ["CORDEX"],
            type = types.StringType,
            )
           
        self.all_okay = self.addLiteralOutput(
            identifier = "all_okay",
            title = "The path has a valid structure",
            default = False,
            type = types.BooleanType,
            )

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of this process",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

        self.output = self.addComplexOutput(
            identifier = "output",
            title = "output",
            abstract = "A summary of the output",
            asReference = True,
            formats = [{"mimeType":"text/plain"}],
            metadata = [],
            )

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        data_path = self.data_path.getValue()
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        username = get_username(self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      statusmethod = statmethod,
                                      session_id = self.session_id.getValue(),
                                      work_dir = WORK_DIR
                                      )
        project = self.project.getValue()
        #set the new data
        output = qcp.validate_directory(data_path, project)

        self.all_okay.setValue(output["all_okay"])
        log = open(self.mktempfile(suffix = ".txt"),"w")
        for messages in output["messages"]:
            for message in messages:
                log.write(str(message)+"\n")
        log.close()
        self.process_log.setValue(log)
        #store the data_path
        data_path_file = open(os.path.join(qcp.process_dir,"data_path"),"w") 
        data_path_file.write(data_path)
        data_path_file.close()
        output_f = open(self.mktempfile(suffix = ".txt"),"w")
        output_f.write(str(output))
        output_f.close()
        self.output.setValue(output_f)

        return 


class UserCheckProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Check",
            title = "Quality Check",
            version = "2014.04.15",
            metadata = [],
            abstract = "Runs a quality check on a given folder.")

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            default = "Needed_if_not_defaultuser",
            type = types.StringType,
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            abstract = ("An ID for the current process. Select the one matching to the "+
                        "init process."),
            type = types.StringType,
            )

        self.select = self.addLiteralInput(
            identifier = "select",
            title = "QC SELECT",
            abstract = ("Comma separated list of parts of the path." + 
                        " If at least one of the elements in the list matches with a path in the data" +
                        " directory, its nc files are added to the check." +
                        " It is recommended to put variables into '/' to avoid accidental" +
                        " matches with other path elements." +
                        " The first element of the path does not start with a '/' and the" +
                        " last element does not end with a '/'." +
                        " The wildcard '.*'" +
                        " should be used with care, as the handling of '/' is considered undefined." +
                        " (Assuming the paths exist a valid example is:" +
                        " AFR-44/.*/tas, EUR.*, /fx/)"),
            minOccurs = 0,
            maxOccurs = 1,
            type = types.StringType,
            )

        self.lock = self.addLiteralInput(
            identifier = "lock",
            title = "QC LOCK",
            abstract = ("Works similar to select, but prevents the given paths being added."+
                        " Lock is stronger than select. (e.g. select tas and lock AFR-44 "+
                        " checks all tas that are not in AFR-44.)"),
            minOccurs = 0,
            maxOccurs = 1,
            type = types.StringType,
            )

        self.project = self.addLiteralInput(
            identifier = "project",
            title = "The project used.",
            abstract = "Currently only CORDEX is fully supported.",
            default = "CORDEX",
            allowedValues = ["CORDEX"],
            type = types.StringType,
            )
        
        self.qc_call_exit_code = self.addLiteralOutput(
            identifier = "qc_call_exit_code",
            title = "qcManager exit code",
            abstract = "Exit code of the quality control tool.",
            type = types.StringType,
            )

        self.qc_call = self.addLiteralOutput(
            identifier = "qc_call",
            title = "qc_call",
            type = types.StringType,
            )

        self.qc_svn_version = self.addLiteralOutput(
            identifier = "qc_svn_version",
            title = "QC_SVN_Version",
            type = types.StringType,
            )

        self.error_messages = self.addLiteralOutput(
            identifier = "error_messages",
            title = "Errors",
            type = types.StringType,
            )
        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of this process",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

    def execute(self):

        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)

        username = get_username(self)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      session_id = self.session_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )
        #load the data_path
        data_path_file = open(os.path.join(qcp.process_dir,"data_path"),"r") 
        data_path = data_path_file.readline()
        data_path_file.close()
        selects = self.select.getValue()
        if selects == '<colander.null>' or selects == None:
            selects =  ""
        locks = self.lock.getValue()
        if locks == '<colander.null>' or locks == None:
            locks =  ""
        
        output = qcp.quality_check(
                               project_data_dir = data_path,
                               selects = selects,
                               locks = locks,
                               project = self.project.getValue(),
                               )

        self.qc_call_exit_code.setValue(output["qc_call_exit_code"])
        self.qc_call.setValue(output["qc_call"])
        self.qc_svn_version.setValue(output["QC_SVN_Version"])
        self.error_messages.setValue(str(output["stderr"]))
        #write logfile
        log = open(self.mktempfile(suffix = ".txt"),"w")
        log.write("Using QC tool with SVN version: "+ str(output["QC_SVN_Version"])+ "\n")
        for message in output["stderr"]:
            log.write(str(message)+"\n")
        if len(output["stderr"]) == 0:
            log.write("Finished without errors\n")
        log.close()
        self.process_log.setValue(log)


        return

class UserInitWithYamlLogsProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
        identifier = "QC_Init_With_Yamllogs",
        title = "Quality Initialize with YAML log files of checks",
        version = "2014.04.15",
        metadata = [],
        )

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            type = types.StringType,
            default = "Needed_if_not_defaultuser",
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            default = "checkdone",
            abstract = "An ID for the current process. Select the one matching to the quality check.",
            type = types.StringType,
            )

        self.yamllogs = self.addLiteralInput(
            identifier = "yamllogs",
            title = "YAML log files from the quality check",
            abstract = "The locations of the YAML log files.",
            minOccurs = 1,
            maxOccurs = 200,#just as arbitrary limit
            type = types.StringType,
            )

        self.prefix_old = self.addLiteralInput(
            identifier = "prefix_old",
            title = "Old data path prefix",
            abstract = "The prefix of the data path in the provided YAML logfiles.",
            minOccurs = 0,
            default = "",
            type = types.StringType,
            )

        self.prefix_new = self.addLiteralInput(
            identifier = "prefix_new",
            title = "New data path prefix",
            abstract = "The prefix of the data path on this machine.",
            default = "",
            minOccurs = 0,
            type = types.StringType,
            )

        self.all_okay = self.addLiteralOutput(
            identifier = "all_okay",
            title = "No rule violations",
            type = types.BooleanType,
            )

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of this process.",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        #load inputs
        username = get_username(self) 
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(
                username = username,
                session_id = self.session_id.getValue(),
                statusmethod = statmethod,
                work_dir = WORK_DIR
                )
        yamllogs = self.yamllogs.getValue() #yamllogs is a list by definition
        prefix_old = self.prefix_old.getValue()
        prefix_new = self.prefix_new.getValue()
        if prefix_old == '<colander.null>' or prefix_old == None:
            prefix_old =  ""
        if prefix_new == '<colander.null>' or prefix_new == None:
            prefix_new =  ""
        #Create a clean directory to work in
        qcp.remove_process_dir()
        #run the method
        output = qcp.init_with_yamllogs(yamllogs, prefix_old, prefix_new)
        #write outputs
        self.all_okay.setValue(output["all_okay"])
        process_log_file = open(self.mktempfile(suffix = ".txt"), "w")
        process_log_file.write(output["process_log"])
        process_log_file.close()
        self.process_log.setValue(process_log_file)





class UserEvalProcess(malleefowl.process.WPSProcess):
    """
    The process runs the qc_processes QualityControl method and 
    handles the progress bar depending on the output of the method.
    """
    def __init__(self):


        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Evaluate",
            title = "Quality Evaluate",
            version = "2014.04.23",
            metadata = [],
            abstract = "Evaluates the quality check and generates metadata and quality files")


        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            type = types.StringType,
            default = "Needed_if_not_defaultuser",
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            abstract = "An ID for the current process. Select the one matching to the quality check.",
            type = types.StringType,
            )


        self.data_node = config.getConfigValue("malleefowl", "data_node")

        self.index_node = config.getConfigValue("malleefowl", "index_node")

        self.access = config.getConfigValue("malleefowl", "access")
        self.metadata_format = config.getConfigValue("malleefowl", "metadata_format")
        self.replica = self.addLiteralInput(
            identifier = "replica",
            title = "Replica",
            minOccurs=0,
            maxOccurs=1,
            type = types.BooleanType,
            )
            
        self.latest = self.addLiteralInput(
            identifier = "latest",
            title = "Latest",
            minOccurs=0,
            maxOccurs=1,
            default = True,
            type = types.BooleanType,
            )

        
        self.found_tags = self.addLiteralOutput(
            identifier = "found_tags",
            title = "found_tags",
            type = types.StringType,
            )
        self.fail_count = self.addLiteralOutput(
            identifier = "fail_count",
            title = "Fail count",
            type = types.IntType,
            )
        self.omit_count = self.addLiteralOutput(
            identifier = "omit_count",
            title = "Omit count",
            type = types.IntType,
            )
        self.pass_count = self.addLiteralOutput(
            identifier = "pass_count",
            title = "Pass count",
            type = types.IntType,
            )
        self.fixed_count = self.addLiteralOutput(
            identifier = "fixed_count",
            title = "Fixed count",
            type = types.IntType,
            )

        self.has_issues = self.addLiteralOutput(
            identifier = "has_issues",
            title = "There is something wrong with the checked files.",
            type = types.BooleanType,
            )

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of this process.",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

        self.to_publish_qc_files = self.addComplexOutput(
            identifier = "to_publish_qc_files",
            title = "QC files that can be published",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

        self.to_publish_metadata_files = self.addComplexOutput(
            identifier = "to_publish_metadata_files",
            title = "Metadata files that can be published",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

        self.found_pids = self.addComplexOutput(
            identifier = "found_pids",
            title = "The pids found",
            metadata = [],
            formats = [{"mimeType": "text/plain"}],
            asReference = True,
            )

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)

        username = get_username(self) 

        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      session_id = self.session_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )

        def _gather_pids_from_yaml_document(yaml_document):
            from malleefowl import wpsclient
            result = wpsclient.execute(
                service = self.service_url,#assuming the PIDManager WPS processes run on the same WPS
                identifier = "PIDs_from_yaml_document",
                inputs = [('yaml_document', yaml_document)],
                outputs = [('pids', False)]
                )
            return(result[0]["data"][0])
            #import yaml
            #pm = pidmanager.PIDManager(database_location = DATABASE_LOCATION)
            #yaml_content = yaml.safe_load(yaml_document)
            #pids = pm.get_pids_dict_from_yaml_content(yaml_content)
            #return(str(pids))
        
        output = qcp.evaluate_quality_check(
                          data_node = self.data_node,
                          index_node = self.index_node,
                          access = self.access,
                          metadata_format = self.metadata_format,
                          replica = self.replica.getValue(),
                          latest = self.latest.getValue(),
                          gather_pids_from_yaml_document = _gather_pids_from_yaml_document
                          )

        self.fail_count.setValue(output["fail_count"])
        self.pass_count.setValue(output["pass_count"])
        self.omit_count.setValue(output["omit_count"])
        self.fixed_count.setValue(output["fixed_count"])
        self.has_issues.setValue(output["has_issues"])
        self.found_tags.setValue(output["found_tags"])
        found_pids = open(self.mktempfile(),"w")
        found_pids.write(output["found_pids"])
        self.found_pids.setValue(found_pids)

        process_log = _create_server_copy_of_file(output["process_log"],self)
        self.process_log.setValue(process_log)
        to_publish_qc_files_log = _create_server_copy_of_file(output["to_publish_qc_files_log"],self)
        self.to_publish_qc_files.setValue(to_publish_qc_files_log)
        to_publish_metadata_files_log = _create_server_copy_of_file(
            output["to_publish_metadata_files_log"],self)
        self.to_publish_metadata_files.setValue(to_publish_metadata_files_log)
        return

class UserQualityPublishProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Publish_Quality", 
            title = "Quality Publish Quality-XML User",
            version = "2014.04.22",
            metadata = [],
            abstract = abstract_ml)
           
        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            type = types.StringType,
            default = "Needed_if_not_defaultuser",
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            abstract = ("An ID for the current process. Select the one matching to the evaluation."),
            type = types.StringType,
            )

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of the process containing system calls that equal the actions performed.",
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )
    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        username = get_username(self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      session_id = self.session_id.getValue(),
                                      )
        output = qcp.qualitypublisher(
                          publish_method="swift",
                          subdir = "qualityxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )

        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        self.process_log.setValue(process_log)

        return

class UserMetaPublisherProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Publish_Meta", 
            title = "Quality Publish Metadata-XML",
            version = "2014.04.23",
            metadata = [],
            abstract = abstract_ml)
           
        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            type = types.StringType,
            default = "Needed_if_not_defaultuser",
            )


        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            abstract = ("An ID for the current process. Select the one matching to the evaluation."),
            type = types.StringType,
            )

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of the process containing system calls that equal the actions performed.",
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )
        #currently only for use with switftclient
        self.wget_string = self.addLiteralOutput(
            identifier = "wget_string",
            title = "wget download command",
            type = types.StringType,
            default = "Not set",
            )

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        username = get_username(self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      session_id = self.session_id.getValue(),
                                      )
        output = qcp.metadatapublisher(
                          publish_method="swift",
                          subdir = "metaxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )
        #TODO: for testing implemented here
        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        to_publish_meta = os.path.join(qcp.process_dir,"to_publish_metadata_files.log")
        swift_meta_url = ("https://cloud.dkrz.de/v1/dkrz_b35af79a-ed58-4431-bc04-b2d1395d2073/" +
                          "QCResults/metaxml/")
        wget_string = "wget --no-check-certificate -N "
        f = open(to_publish_meta, "r")
        lines = f.readlines()
        for line in lines:
            line = line.rstrip("\n")
            basename = os.path.basename(line)
            wget_string += swift_meta_url + basename + " "

        self.wget_string.setValue(wget_string)
        #end TODO


        self.process_log.setValue(process_log)

        return

class UserCleanupProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Cleanup", 
            title = "Quality Cleanup",
            version = "2014.04.23",
            metadata = [],
            abstract = "Remove data by Session ID or your complete work data.")
           
        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            type = types.StringType,
            default = "Needed_if_not_defaultuser",
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            abstract = ("IDs for the to remove work data."),
            minOccurs = 1,
            type = types.StringType,
            )
        
    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        
        username = get_username(self)
        session_id = self.session_id.getValue()
        qcp = qcprocesses.QCProcesses(
                                  username = username,
                                  statusmethod = statmethod,
                                  work_dir = WORK_DIR,
                                  session_id = session_id,
                                  )
        qcp.remove_process_dir()
        self.status.set(msg = "Finished", percentDone = 100, propagate = True)
        return


class PIDManagerFileProcess(malleefowl.process.WPSProcess):
    """
    The process provides a PID for a given local file name and server file name. 
    """
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "PID_for_file",
            title = "Get a PID for a file",
            version = "2014.04.22",
            metadata = [],
            abstract = "Get a PID for a given local file and the url it will be available at.")

        self.local_filename = self.addLiteralInput(
                identifier = "local_filename",
                title = "Local filename",
                minOccurs = 1,
                maxOccurs = 1,
                default = (climdapsabs + "/examples/data/CORDEX/AFR-44/CLMcom/MPI-ESM-LR/historical/" +
                           "r0i0p0/CCLM4-8-17/v1/fx/orog/orog_AFR-44_MPI-ESM-" + 
                           "LR_historical_r1i1p1_CCLM_4-8-17_fx.nc"),
                type = types.StringType,
                )

        self.pid_resolver_url = self.addLiteralInput(
                identifier = "pid_resolver_url",
                title = "PID resolver url",
                minOccurs = 1,
                maxOccurs = 1,
                default = ("ipcc-ar5.dkrz.de/thredds/fileServer/cordex/AFR-44/CLMcom/MPI-ESM-LR/" + 
                           "historical/r0i0p0/CCLM4-8-17/v1/fx/orog/orog_AFR-44_MPI-ESM-LR_historical" +
                           "_r1i1p1_CCLM_4-8-17_fx.nc"),
                type = types.StringType,
                )
        self.database_location = DATABASE_LOCATION
        self.additional_identifier_element = self.addLiteralInput(
                identifier = "additional_identifier_element",
                title = "Additional identifier element",
                default = "CORDEX-",
                type = types.StringType,
                abstract = "Allows to add a string to the PID, to make it distinguishable",
                )
        self.port = self.addLiteralInput(
                identifier = "port",
                title = "Handle server port",
                default = "8090",
                type = types.StringType,
                )
        self.prefix = self.addLiteralInput(
                identifier = "prefix",
                title = "Handle server prefix",
                default = "10876",
                type = types.StringType,
                )

        self.path = self.addLiteralInput(
                identifier = "path",
                title = "Handle server path",
                default =  "/handle-rest-0.1.1/",
                type = types.StringType,
                )

        self.with_first_run = True
        self.pid = self.addLiteralOutput(
                identifier = "pid",
                title = "Found PID",
                type = types.StringType,
                )

    def execute(self):
        self.pidmanager = pidmanager.PIDManager(
                database_location = self.database_location,
                additional_identifier_element = self.additional_identifier_element.getValue(),
                port = self.port.getValue(),
                prefix = self.prefix.getValue(),
                path = self.path.getValue(),
                with_first_run = self.with_first_run)
        pid_resolver_url = self.pid_resolver_url.getValue()
        local_filename = self.local_filename.getValue()
        pid, known = self.pidmanager.get_pid_file(local_filename, pid_resolver_url)
        self.pid.setValue(pid)

class PIDManagerDatasetProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "PID_for_dataset",
            title = "Get a PID for a dataset",
            version = "2014.04.22",
            metadata = [],
            abstract = "Get a PID for a dataset title and the comma separated list of PIDs in it.")

        self.ds_title = self.addLiteralInput(
                identifier = "ds_title",
                title = "Dataset title",
                minOccurs = 1,
                maxOccurs = 1,
                #default = "cordex.AFR-44.CLMcom.MPI-ESM-LR.historical.r0i0p0.CCLM4-8-17-v1.fx.orog",
                type = types.StringType,
                )

        self.dataset_pids = self.addLiteralInput(
                identifier = "dataset_pids",
                title = "Dataset file PIDs list",
                minOccurs = 1,
                maxOccurs = 1,
                #default = "10876/CORDEX-5p8d-09bx-u4qg-xhhx",
                abstract = "The PIDs in the dataset",
                type = types.StringType,
                )

        self.database_location = DATABASE_LOCATION
        self.additional_identifier_element = self.addLiteralInput(
                identifier = "additional_identifier_element",
                title = "Additional identifier element",
                default = "CORDEX-",
                type = types.StringType,
                abstract = "Allows to add a string to the PID, to make it distinguishable",
                )
        self.port = self.addLiteralInput(
                identifier = "port",
                title = "Handle server port",
                default = "8090",
                type = types.StringType,
                )
        self.prefix = self.addLiteralInput(
                identifier = "prefix",
                title = "Handle server prefix",
                default = "10876",
                type = types.StringType,
                )

        self.path = self.addLiteralInput(
                identifier = "path",
                title = "Handle server path",
                default =  "/handle-rest-0.1.1/",
                type = types.StringType,
                )

        self.with_first_run = True
        self.pid = self.addLiteralOutput(
                identifier = "pid",
                title = "Found PID",
                type = types.StringType,
                )

    def execute(self):
        self.pidmanager = pidmanager.PIDManager(
                database_location = self.database_location,
                additional_identifier_element = self.additional_identifier_element.getValue(),
                port = self.port.getValue(),
                prefix = self.prefix.getValue(),
                path = self.path.getValue(),
                with_first_run = self.with_first_run)
        ds_title  = self.ds_title.getValue()
        #the string of comma separated pids must be converted to a list
        dataset_pids = self.dataset_pids.getValue()
        dspids = dataset_pids.split(",")
        dataset_file_pids = [x.strip() for x in dspids]
        pid, known = self.pidmanager.get_pid_dataset(ds_title, dataset_file_pids)
        self.pid.setValue(pid)

#The process seems to be unneeded. The place where it might be usefull uses a yaml document.
#TODO: Left here commented till a final decission is made.
#class PIDManagerPathCORDEXProcess(malleefowl.process.WPSProcess):
#    def __init__(self):
#        malleefowl.process.WPSProcess.__init__(self,
#            identifier = "PIDManager_Path_CORDEX",
#            title = "Get PIDs for a path using the CORDEX specification.",
#            version = "2014.03.11",
#            metadata = [],
#            )
#        
#        self.path = self.addLiteralInput(
#                identifier = "path",
#                title = "Root project data path",
#                default = os.path.join(climdapsabs,"examples/data/CORDEX"),
#                type = types.StringType,
#                minOccurs = 1,
#                maxOccurs = 1,
#                )
#        self.file_regexp = self.addLiteralInput(
#                identifier = "file_regexp",
#                title = "Regular expression to filter files",
#                default = "*.nc",
#                type = types.StringType,
#                abstract = ("The syntax is '*' for any number of random characters. '.' is the normal" +
#                            "textual dot. (e.g. */fx/*.nc includes all .nc files in a fx directory")
#                )
#
#        self.file_regexp_out = self.addLiteralOutput(
#                identifier = "file_regexp_out",
#                title = "Used regular expression in re format",
#                type = types.StringType,
#                )
#
#        self.pids = self.addLiteralOutput(
#                identifier = "pids",
#                title = "Found PIDs for the path",
#                type = types.StringType,
#                )
#
#    def execute(self):
#        d2dy = directory2datasetyaml.Directory2DatasetYaml()#defaults are good for CORDEX
#        tempfile = self.mktempfile()
#        regexp_raw = self.file_regexp.getValue()
#        #. has to be escaped and * has to be replaced by .*
#        regexp = regexp_raw.replace(".","\.").replace("*",".*")
#        self.file_regexp_out.setValue(regexp)
#        d2dy.create_yaml(path = self.path.getValue(), yaml_fn = tempfile, 
#                file_regexp = regexp)
#        pm = pidmanager.PIDManager(DATABASE_LOCATION)
#        pids = pm.get_pids_from_yaml_file(tempfile)
#        self.pids.setValue(str(pids))

class PIDManagerPIDsFromYamlDocumentProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "PIDs_from_yaml_document",
            title = "Get PIDs from a string representation of a yaml file",
            version = "2014.03.24",
            metadata = [],
            )
        
        self.yaml_document = self.addComplexInput(
                identifier = "yaml_document",
                title = "yaml_document",
                minOccurs = 1,
                maxOccurs = 1,
                metadata=[],
                maxmegabites=2,
                formats=[{"mimeType":"text/yaml"}],
                )

        self.pids = self.addLiteralOutput(
                identifier = "pids",
                title = "Found PIDs for the path",
                type = types.StringType,
                )

    def execute(self):
        import yaml
        pm = pidmanager.PIDManager(database_location = DATABASE_LOCATION)
        filename = self.yaml_document.getValue(asFile=False)
        g = open(filename, "r")
        filecontent = g.read()
        yaml_content = yaml.safe_load(filecontent)
        pids = pm.get_pids_dict_from_yaml_content(yaml_content)
        self.pids.setValue(str(pids))
        
class GetSessionIdsUser(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "Get_Session_IDs",
            title = "Get users Session_IDs",
            version =  "2014.04.15",
            metadata = [],
            )

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            abstract = ("Name to access your own processing directory.")
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            default = "Needed_if_not_defaultuser",
            type = types.StringType,
            abstract = "The token authenticates you as the user. defaultuser accepts any token."
            )

        self.session_ids = self.addLiteralOutput(
            identifier = "session_ids",
            title = "Session IDs separated by '/'",
            type = types.StringType,
            )

    def execute(self):
        username = get_username(self)
        session_ids = get_user_sessionids(username) 
        self.session_ids.setValue("/".join(session_ids))
       
class GetExampleDirectory(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "Get_Example_Directory",
            title = "Get example directory",
            version =  "2014.04.15",
            metadata = [],
            )

        self.example_directory = self.addLiteralOutput(
            identifier = "example_directory",
            title = "The example directory",
            type = types.StringType,
            )

    def execute(self):
        self.example_directory.setValue(config.getConfigValue("malleefowl", "example_directory"))


class QCProcessChain(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Chain",
            title = "Run the QC chain",
            version =  "2014.04.22",
            metadata = [],
            )

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            default = "defaultuser",
            type = types.StringType,
            abstract = ("Name to access your own processing directory.")
            )

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            default = "Needed_if_not_defaultuser",
            type = types.StringType,
            abstract = "The token authenticates you as the user. defaultuser accepts any token."
            )

        self.session_id = self.addLiteralInput(
            identifier = "session_id",
            title = "Session ID",
            default = "web1",
            type = types.StringType,
            abstract = ("To run multiple processes in parallel each requires its own directory." +
                        "The directory is placed in your user directory. If defaultuser is choosen, " +
                        "collisions with names are more likely. Be careful when choosing a name."),
            minOccurs = 1, 
            maxOccurs = 1,
            )

        self.project = self.addLiteralInput(
            identifier = "project",
            title = "Project",
            abstract = "Currently only CORDEX is fully supported.",
            default = "CORDEX",
            allowedValues = ["CORDEX"],
            type = types.StringType,
            )

        self.irods_home = self.addLiteralInput(
            identifier = "irods_home",
            title = "iRods Home",
            type = types.StringType,
            default = "qc_dummy_DKRZ",#TODO remove after testing
            )
       
        self.irods_collection = self.addLiteralInput(
            identifier = "irods_collection",
            title = "iRods collection",
            type = types.StringType,
            default = "qc_test_20140416",#TODO remove after testing
            )
        
        self.select = self.addLiteralInput(
            identifier = "select",
            title = "QC SELECT",
            minOccurs = 0,
            type = types.StringType,
            abstract = "Comma separated list of values for the qc tools SELECT variable.",
            )

        self.lock = self.addLiteralInput(
            identifier = "lock",
            title = "QC LOCK",
            type = types.StringType,
            minOccurs = 0,
            abstract = "Comma separated list of values for the qc tools LOCK variable.",
            )

        self.replica = self.addLiteralInput(
            identifier = "replica",
            title = "Replica",
            type = types.BooleanType,
            )
        
        self.latest = self.addLiteralInput(
            identifier = "latest",
            title = "Latest",
            type = types.BooleanType,
            default = True,
            )

        self.publish_metadata = self.addLiteralInput(
            identifier = "publish_metadata",
            title = "Publish metadata",
            type = types.BooleanType,
            default = True,
            )

        self.publish_quality = self.addLiteralInput(
            identifier = "publish_quality",
            title = "Publish quality data",
            type = types.BooleanType,
            default = True,
            )

        self.cleanup = self.addLiteralInput(
            identifier = "cleanup",
            title = "Clean afterwards",
            abstract = "Removes the work data after the other steps have finished.",
            type = types.BooleanType,
            )

        self.service = self.addLiteralInput(
            identifier = "service",
            title = "Service",
            abstract = "The address of the WPS server, where the used methods are available.",
            default = self.service_url,
            type = types.StringType,
            )
       
        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of the process containing system calls that equal the actions performed.",
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )

        

    def execute(self):
        from owslib.wps import WebProcessingService, monitorExecution 
        #alternative : from malleefowl import wpsclient see unit_test.test_qualityprocesses.py
        """
        For each process an WPS call is made using the owslib. 

        from the WebProcessingService SourceDoc:
        execute(self, identifier, inputs, output=None, request=None, response=None):
            identifier: the requested process identifier
            inputs: list of process inputs as (key, value) tuples 
            output: optional identifier for process output reference 
        """
        service = self.service.getValue()
        username = get_username(self)
        token = self.token.getValue()
        project = self.project.getValue()
        irods_home = self.irods_home.getValue()
        irods_collection = self.irods_collection.getValue()
        session_id = self.session_id.getValue()
        select = self.select.getValue()
        lock = self.lock.getValue()
        if select == '<colander.null>' or select == None:
            select =  ""
        if lock == '<colander.null>' or lock == None:
            lock =  ""
        #Due to using minOccurs=0 the not checked will be treated as None
        #If it is checked it will return boolean True.
        #Somehow strings are expected instead of booleans for the wps calls 
        replica = self.replica.getValue() == True
        replica = str(replica)
        latest = self.latest.getValue() == True
        latest = str(latest)
        #the enable flags are fine with boolean. However if the datatype is another it will set False.
        publish_quality = self.publish_quality.getValue() == True 
        publish_metadata = self.publish_metadata.getValue() == True
        cleanup = self.cleanup.getValue() == True
        #weighting of steps. Higher value is longer expected time to run.
        step_weights = { "irods_rsync": 5, "QC_Init" : 1, "QC_Check": 8, "QC_Evaluate": 3, 
                         "QC_Publish_Meta": 1, "QC_Publish_Quality": 1, "QC_Cleanup": 1}
        #which steps are used
        steps_used = ["irods_rsync", "QC_Init", "QC_Check", "QC_Evaluate"]
        if publish_quality:
            steps_used.append("QC_Publish_Quality")
        if publish_metadata:
            steps_used.append("QC_Publish_Meta")
        if cleanup:
            steps_used.append("QC_Cleanup")
        #set up the status 
        current = 0
        end = 0
        for step in steps_used:
            end += step_weights[step]
        #get the wps
        wps = WebProcessingService(url = service)

        process_log = open(self.mktempfile(suffix = ".txt"), "w")

        #################
        #Run iRods rsync#
        #################
        process_log.write("Running iRods rsync\n")
        logger.debug("Running iRods rsync\n")
        process_log.write("*******************\n")
        identifier = "org.malleefowl.irods.rsync"
        inputs = [("token", token), ("home", irods_home), ("collection", irods_collection)]
        outputs = [("output", False)]
        statusmethod("Running " + identifier, current, end, self) 
        execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
        monitorExecution(execution, sleepSecs=1)
        #Get results after irods finished.
        #output contains the path from irods.
        data_path = execution.processOutputs[0].data[0]# it only should return one path
        if project == "CORDEX":
           #By project specification(?TODO) the directory with the data must be named CORDEX
           #So if it is not the last part then add it.
           path_end = "/CORDEX"
           if data_path[-len(path_end):] != path_end:
               data_path += "/CORDEX"
        process_log.write("data_path = " + data_path + "\n") 
        current += step_weights["irods_rsync"]
        #############
        #Run QC_Init#
        #############
        process_log.write("\n")
        process_log.write("Running QC_Init\n")
        logger.debug("Running QC_Init\n")
        process_log.write("***************\n")
        identifier = "QC_Init"
        inputs = [("username", username), ("token", token), ("session_id", session_id),
                  ("data_path", data_path), ("project", project)]
        outputs = [("all_okay", False), ("process_log", True)]
        statusmethod("Running " + identifier, current, end, self) 
        execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
        monitorExecution(execution, sleepSecs=1)
        for item in execution.processOutputs:
            if item.identifier in ["process_log"]: 
                data = item.reference
            else:#all_okay only returns a single value. The data list will only have one element.
                data = item.data[0]
            process_log.write(str(item.identifier)+" = " + data + "\n")
        current += step_weights["QC_Init"]
        ##############
        #Run QC_Check#
        ##############
        process_log.write("\n")
        process_log.write("Running QC_Check\n")
        process_log.write("****************\n")
        identifier = "QC_Check"
        inputs = [("username", username), ("token", token), ("session_id", session_id),
                  ("project", project)]
        if select != "":
            inputs.append(("select", select))
        if lock != "":
            inputs.append(("lock", lock))
        outputs = [("qc_call", False), ("qc_call_exit_code", False), ("error_messages", False),
                   ("qc_svn_version", False), ("process_log", True)]
        statusmethod("Running " + identifier, current, end, self) 
        execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
        monitorExecution(execution, sleepSecs=1)
        for item in execution.processOutputs:
            if item.identifier in ["process_log"]: 
                data = item.reference
            else:#For each item the data list will only have one element.
                data = item.data[0]
            process_log.write(str(item.identifier)+" = " + data + "\n")
        current += step_weights["QC_Check"]
        #################
        #Run QC_Evaluate#
        #################
        process_log.write("\n")
        process_log.write("Running QC_Evaluate\n")
        process_log.write("*******************\n")
        identifier = "QC_Evaluate"
        inputs = [("username", username), ("token", token), ("session_id", session_id),
                  ("replica", replica), ("latest", latest)]
        outputs = [("found_tags", False), ("fail_count", False), ("pass_count", False),
                   ("omit_count", False), ("fixed_count", False), ("has_issues", False),
                   ("found_pids", True),
                   ("process_log", True), ("to_publish_metadata_files", True), 
                   ("to_publish_qc_files", True), ]
        statusmethod("Running " + identifier, current, end, self) 
        execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
        monitorExecution(execution, sleepSecs=1)
        outputsTrueIdentifier = [oid for (oid, ref) in outputs if ref]
        for item in execution.processOutputs:
            if item.identifier in outputsTrueIdentifier:
                data = item.reference
            else:#For each item the data list will only have one element.
                data = item.data[0] 
            process_log.write(str(item.identifier)+" = " + data + "\n")
        current += step_weights["QC_Evaluate"]
        #####################
        #Run QC_Publish_Meta#
        #####################
        if publish_metadata:
            process_log.write("\n")
            process_log.write("Running QC_Publish_Meta\n")
            process_log.write("***********************\n")
            identifier = "QC_Publish_Meta"
            inputs = [("username", username), ("token", token), ("session_id", session_id)]
            outputs = [("process_log", True), ("wget_string", False)]
            statusmethod("Running " + identifier, current, end, self) 
            execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
            monitorExecution(execution, sleepSecs=1)
            outputsTrueIdentifier = [oid for (oid, ref) in outputs if ref]
            for item in execution.processOutputs:
                if item.identifier in outputsTrueIdentifier:
                    data = item.reference
                else:#For each item the data list will only have one element.
                    data = item.data[0] 
                process_log.write(str(item.identifier)+" = " + data + "\n")
            current += step_weights["QC_Publish_Meta"]
        else:
            process_log.write("\n")
            process_log.write("QC_Publish_Meta was disabled.\n")
        ########################
        #Run QC_Publish_Quality#
        ########################
        if publish_quality:
            process_log.write("\n")
            process_log.write("Running QC_Publish_Quality\n")
            process_log.write("**************************\n")
            identifier = "QC_Publish_Quality"
            inputs = [("username", username), ("token", token), ("session_id", session_id)]
            outputs = [("process_log", True)]
            statusmethod("Running " + identifier, current, end, self) 
            execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
            monitorExecution(execution, sleepSecs=1)
            outputsTrueIdentifier = [oid for (oid, ref) in outputs if ref]
            for item in execution.processOutputs:
                if item.identifier in outputsTrueIdentifier:
                    data = item.reference
                else:#For each item the data list will only have one element.
                    data = item.data[0] 
                process_log.write(str(item.identifier)+" = " + data + "\n")
            current += step_weights["QC_Publish_Quality"]
        else:
            process_log.write("\n")
            process_log.write("QC_Publish_Quality was disabled.\n")
        ################
        #Run QC_Cleanup#
        ################
        if cleanup:
            process_log.write("\n")
            process_log.write("Running QC_Cleanup\n")
            process_log.write("******************\n")
            identifier = "QC_Cleanup"
            inputs = [("username", username), ("token", token), ("session_id", session_id)]
            outputs = []
            statusmethod("Running " + identifier, current, end, self) 
            execution = wps.execute(identifier = identifier, inputs = inputs, output = outputs)
            monitorExecution(execution, sleepSecs=1)
            process_log.write("Finished QC_Cleanup\n")
            current += step_weights["QC_Cleanup"]
        else:
            process_log.write("\n")
            process_log.write("QC_Cleanup was disabled.\n")
        #finish
        process_log.write("\n")
        process_log.write("Finished processing.\n")
        process_log.close()
        self.process_log.setValue(process_log)


##################
# Helper methods #
##################

def statusmethod(msg, current, end, wpsprocess):
    """
    :param current: The current counter
    :param end: The end counter
    :param wpsprocess: The process that needs to update the statusbar.
    """
    #workaround division 0
    if int(end) == 0:
        end = 1
    wpsprocess.status.set(msg = msg, percentDone = float(current)*100.0/float(end), propagate = True)

def _create_server_copy_of_file(filename, wpsprocess):
    serverfile = open(wpsprocess.mktempfile(suffix = ".txt"), "w")
    localfile = open(filename, "r")
    serverfile.write(localfile.read())
    localfile.close()
    serverfile.close()
    return serverfile

def get_user_dir(user):
    return os.path.join(climdapsabs, "var", "qc_cache", user)

def get_user_sessionids(user):
    """
    It is assumed that no processing directories are moved into the work directory by hand or
    other tools than the qc-processes.
    """
    path = get_user_dir(user)
    history_fn = os.path.join(path, "session_id.history")
    history = []
    if os.path.isfile(history_fn):
        with open(history_fn, "r") as hist:
            lines = hist.readlines()
            for line in lines:
                history.append(line.rstrip("\n"))
    existing_history = [x for x in history if os.path.isdir(os.path.join(path, x))] 
    return existing_history

def get_username(obj):
    username = obj.username.getValue().replace("(at)","@")
    token = obj.token.getValue()
    try:#If the token matches to any userid the userid is returned
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
    except:#If no userid is known by the token an exception is thrown. Use defaultuser then.
        userid = "defaultuser"   
    #workaround
    username1 = username.replace("@","_")
    userid1 = userid.replace("@","_")
    if username1 != userid1:
        username = "defaultuser"
    return username

