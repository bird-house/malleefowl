"""
Quality Control processes.

Author: Tobias Kipp (kipp@dkrz.de)
Creation date: 21.01.2014
"""
import types
import malleefowl.process 
import qc_processes.qcprocesses as qcprocesses
import qc_processes.pidmanager as pidmanager
import qc_processes.directory2datasetyaml as directory2datasetyaml
from pywps import config

import os
from malleefowl import tokenmgr
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

curdir = os.path.dirname(__file__)
climdapsabs = os.path.abspath(os.path.join(curdir,".."))

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
            version = "2014.03.24",
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

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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
                                      parallel_id = self.parallel_id.getValue(),
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
            version = "2014.03.24",
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

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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

    def execute(self):

        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)

        username = get_username(self)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      parallel_id = self.parallel_id.getValue(),
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


        return

class UserInitWithYamlLogsProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
        identifier = "QC_Init_With_Yamllogs",
        title = "Quality Initialize with YAML log files of checks",
        version = "2014.03.25",
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

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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
                parallel_id = self.parallel_id.getValue(),
                statusmethod = statmethod,
                work_dir = WORK_DIR
                )
        logger.debug(username)
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
        process_log_file = open(self.mktempfile(),"w")
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
            version = "2014.03.24",
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

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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
                                      parallel_id = self.parallel_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )

        def _gather_pids_from_yaml_document(yaml_document):
            import urllib
            url = ("http://localhost:8090/wps?service=WPS&request=Execute&version=1.0.0" +
                   "&identifier=PIDs_from_yaml_document&DataInputs=yaml_document=" +
                   str(yaml_document) + "&rawdataoutput=pids")
            pid_file = urllib.urlopen(url)
            content = pid_file.read()
            return content

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
            identifier = "QC_QualityPublisher", 
            title = "Quality Publish Quality-XML User",
            version = "2014.03.24",
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

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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
                                      parallel_id = self.parallel_id.getValue(),
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
            identifier = "QC_MetaPublisher", 
            title = "Quality Publish Metadata-XML",
            version = "2014.03.24",
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


        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
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
                                      parallel_id = self.parallel_id.getValue(),
                                      )
        output = qcp.metadatapublisher(
                          publish_method="swift",
                          subdir = "metaxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )

        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        self.process_log.setValue(process_log)

        return

class UserRemoveDataProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_RemoveData", 
            title = "Quality Clean up",
            version = "2014.03.24",
            metadata = [],
            abstract = "Remove data by Parallel ID or your complete work data.")
           
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

        self.parallel_ids = self.addLiteralInput(
            identifier = "parallel_ids",
            title = "Parallel IDs",
            abstract = ("IDs for the to remove work data."),
            minOccurs = 0,
            type = types.StringType,
            )
        
    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        
        username = get_username(self)
        parallel_ids = self.parallel_ids.getValue()
        if parallel_ids == None:
            parallel_ids = []
        if isinstance(parallel_ids, str):
            parallel_ids = parallel_ids.split(",")
        cur = 0
        end = len(parallel_ids)

        for par_id in parallel_ids:
            qcp = qcprocesses.QCProcesses(
                                      username = username,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      parallel_id = par_id,
                                      )
            qcp.remove_process_dir()
            percent = cur*100.0/end#end is not 0, as there is at least one element in parallel_ids
            self.status.set(msg = "Finished", percentDone = percent, propagate = True)
            cur += 1
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
            version = "2014.03.06",
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

        self.server_filename = self.addLiteralInput(
                identifier = "server_filename",
                title = "Server filename",
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
        self.pid = self.addComplexOutput(
                identifier = "pid",
                title = "Found PID",
                formats=[{"mimeType":"text/plain"}],
                asReference = True,
                )

    def execute(self):
        self.pidmanager = pidmanager.PIDManager(
                database_location = self.database_location,
                additional_identifier_element = self.additional_identifier_element.getValue(),
                port = self.port.getValue(),
                prefix = self.prefix.getValue(),
                path = self.path.getValue(),
                with_first_run = self.with_first_run)
        server_filename = self.server_filename.getValue()
        local_filename = self.local_filename.getValue()
        output = self.pidmanager.get_pid_file(local_filename, server_filename)
        outputfile = open(self.mktempfile(),"w")
        outputfile.write(output)
        outputfile.close()
        self.pid.setValue(outputfile)

class PIDManagerDatasetProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "PID_for_dataset",
            title = "Get a PID for a dataset",
            version = "2014.03.06",
            metadata = [],
            abstract = "Get a PID for a dataset title and the comma separated list of PIDs in it.")

        self.ds_title = self.addLiteralInput(
                identifier = "ds_title",
                title = "Dataset title",
                minOccurs = 1,
                maxOccurs = 1,
                default = "cordex.AFR-44.CLMcom.MPI-ESM-LR.historical.r0i0p0.CCLM4-8-17-v1.fx.orog",
                type = types.StringType,
                )

        self.dataset_pids = self.addLiteralInput(
                identifier = "dataset_pids",
                title = "Dataset file PIDs list",
                minOccurs = 1,
                maxOccurs = 1,
                default = "10876/CORDEX-5p8d-09bx-u4qg-xhhx",
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
        self.pid = self.addComplexOutput(
                identifier = "pid",
                title = "Found PID",
                formats=[{"mimeType":"text/plain"}],
                asReference = True,
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
        output = self.pidmanager.get_pid_dataset(ds_title, dataset_file_pids)
        outputfile = open(self.mktempfile(),"w")
        outputfile.write(output)
        outputfile.close()
        self.pid.setValue(outputfile)

class PIDManagerPathCORDEXProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "PIDManager_Path_CORDEX",
            title = "Get PIDs for a path using the CORDEX specification.",
            version = "2014.03.11",
            metadata = [],
            )
        
        self.path = self.addLiteralInput(
                identifier = "path",
                title = "Root project data path",
                default = os.path.join(climdapsabs,"examples/data/CORDEX"),
                type = types.StringType,
                minOccurs = 1,
                maxOccurs = 1,
                )
        self.file_regexp = self.addLiteralInput(
                identifier = "file_regexp",
                title = "Regular expression to filter files",
                default = "*.nc",
                type = types.StringType,
                abstract = ("The syntax is '*' for any number of random characters. '.' is the normal" +
                            "textual dot. (e.g. */fx/*.nc includes all .nc files in a fx directory")
                )

        self.file_regexp_out = self.addLiteralOutput(
                identifier = "file_regexp_out",
                title = "Used regular expression in re format",
                type = types.StringType,
                )

        self.pids = self.addLiteralOutput(
                identifier = "pids",
                title = "Found PIDs for the path",
                type = types.StringType,
                )

    def execute(self):
        d2dy = directory2datasetyaml.Directory2DatasetYaml()#defaults are good for CORDEX
        tempfile = self.mktempfile()
        regexp_raw = self.file_regexp.getValue()
        #. has to be escaped and * has to be replaced by .*
        regexp = regexp_raw.replace(".","\.").replace("*",".*")
        self.file_regexp_out.setValue(regexp)
        d2dy.create_yaml(path = self.path.getValue(), yaml_fn = tempfile, 
                file_regexp = regexp)
        pm = pidmanager.PIDManager(DATABASE_LOCATION)
        pids = pm.get_pids_from_yaml_file(tempfile)
        self.pids.setValue(str(pids))

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

def get_user_parallelids(user):
    """
    It is assumed that no processing directories are moved into the work directory by hand or
    other tools than the qc-processes.
    """
    path = get_user_dir(user)
    history_fn = os.path.join(path, "parallel_id.history")
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
    userid1 = username.replace("@","_")
    if username1 != userid1:
        username = "defaultuser"
    logger.debug("For username " + username + " returning " + userid)
    return username

