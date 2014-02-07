"""
Quality Control processes.

Author: Tobias Kipp (kipp@dkrz.de)
Creation date: 21.01.2014
"""
import types
import malleefowl.process 
import qc_processes.qcprocesses as qcprocesses

import os
import logging
logger = logging.getLogger(__name__)
#hdlr = logging.FileHandler("/var/tmp/quality_processes.log")
#formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
#hdlr.setFormatter(formatter)
#logger.addHandler(hdlr) 
#logger.setLevel(logging.DEBUG)

curdir = os.path.dirname(__file__)
climdapsabs = os.path.abspath(os.path.join(curdir,"../../.."))

DATABASE_LOCATION = os.path.join(climdapsabs,"examples/pidinfo.db")
WORK_DIR = os.path.join(climdapsabs,"var/qc_cache/")


DATA = {}
fn = os.path.join(os.path.dirname(__file__),"quality_processes.conf")
logger.debug("qp: Loading data from file: "+fn)
execfile(fn,DATA)
logger.debug("qp: Loaded file to DATA variable")

class PidGenerationProcess(malleefowl.process.WPSProcess):
    """
    The process checks a path to be valid for processing and generates a PID
    for each new found .nc file and for each dataset that differs from the 
    previously found ones. This relies on the local database storing the previously
    found files and datasets.
    """
    def __init__(self):

        self.database_location = DATABASE_LOCATION
       
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_PID_Generation",
            title = "PID generation",
            version = "2014.02.07",
            metadata = [],
            abstract = "If the given directory is valid included files and datasets receive a PID.")

        self.data_path = self.addLiteralInput(
            identifier = "datapath",
            title = "Root path of the to index data.",
            default = os.path.join(climdapsabs,"examples/data/CORDEX"),
            type = types.StringType,
            minOccurs = 1,
            maxOccurs = 1,
            )
           
        self.database_location = DATABASE_LOCATION

        self.data_node = DATA["data_node"]

        self.is_valid = self.addLiteralOutput(
            identifier = "isvalid",
            title = "The path has a valid structure.",
            default = False,
            type = types.BooleanType,
            )


        self.errors = self.addLiteralOutput(
            identifier = "errors",
            title = "Error messages",
            default = "",
            type = types.StringType,
            )

        self.data_path_out = self.addLiteralOutput(
            identifier = "data_path",
            title = "Data path",
            type = types.StringType)
        
        self.data_node_out = self.addLiteralOutput(
            identifier = "data_node",
            title = "Data node",
            type = types.StringType)

        self.new_files_counter = self.addLiteralOutput(
            identifier = "new_files_counter",
            title = "New PIDs generates for n files.",
            type = types.IntType)

        self.new_datasets_counter = self.addLiteralOutput(
            identifier = "new_datasets_counter",
            title = "New PIDs generates for n datasets.",
            type = types.IntType)

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        data_path = self.data_path.getValue()
        data_node = self.data_node
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        param_dict = dict(data_path = data_path,
                          data_node = data_node,
                          )

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )
        output = qcp.pid_generation(**param_dict)

        self.is_valid.setValue(output["valid"])
        self.errors.setValue(output["errors"])
        self.data_node_out.setValue(data_node)
        self.data_path_out.setValue(data_path)
        self.new_datasets_counter.setValue(output["new_datasets_counter"])
        self.new_files_counter.setValue(output["new_files_counter"])

        return 


class QualityCheckProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        self.database_location = DATABASE_LOCATION

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Quality_Check",
            title = "Quality Check",
            version = "2014.02.07",
            metadata = [],
            abstract = "Runs a quality check on a given folder.")

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. If multiple processes are running in parallel "
                      +"choose an unused one."),
            default = "web1",
            type = types.StringType,
            )
        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            abstract = ("The username is used to prevent two users working in the same directory."
                        +" Currently this is a work around until the username can be derived "
                        +"automatically."),
            default = "defaultuser",
            type = types.StringType,
            )
        self.project_data_dir = self.addLiteralInput(
            identifier = "project_data_dir",
            title = "To analyse data path",
            abstract = "A local path",
            default = os.path.join(climdapsabs,"examples/data/CORDEX"),
            type = types.StringType,
            )

        self.args = self.addLiteralInput(
            identifier = "args",
            title = "Additional QC parameters",
            abstract = "Using options of the QC tool. (e.g. -E_SELECT .*)",
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


        self.clean_process_dir = self.addLiteralInput(
            identifier = "clean_process_dir",
            title = "clean work",
            abstract = ("Remove data from the working directory. Quality Check skips already checked"+ 
              " files. After clean up it will check all files."),
            type = types.BooleanType,
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

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        param_dict = dict(project_data_dir = self.project_data_dir.getValue(),
                          args = self.args.getValue(),
                          project = self.project.getValue(),
                          )

        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username.getValue(),
                                      parallel_id = self.parallel_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )
        if self.clean_process_dir.getValue() == True:
            qcp.clean_process_dir()

        output = qcp.quality_check(**param_dict)
        self.qc_call_exit_code.setValue(output["qc_call_exit_code"])
        self.qc_call.setValue(output["qc_call"])
        self.qc_svn_version.setValue(output["QC_SVN_Version"])

        return

class EvaluateQualityCheckProcess(malleefowl.process.WPSProcess):
    """
    The process runs the qc_processes QualityControl method and 
    handles the progress bar depending on the output of the method.
    """
    def __init__(self):

        logger.debug("qp: eval init ")
        self.database_location = DATABASE_LOCATION

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Evaluate_Quality_Check",
            title = "Evaluate Quality Check",
            version = "2014.02.07",
            metadata = [],
            abstract = "Evaluates the quality check and generates metadata and quality files")

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. If multiple processes are running in parallel "
                      +"choose an unused one."),
            default = "web1",
            type = types.StringType,
            )
        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            abstract = ("The username is used to prevent two users working in the same directory."
                        +" Currently this is a work around until the username can be derived "
                        +"automatically."),
            default = "defaultuser",
            type = types.StringType,
            )

        logger.debug("qp: eval Loading DATA parameters")
        #logger.debug(str(DATA))
        self.data_node = DATA.get("data_node")

        self.index_node = DATA.get("index_node")

        self.access = DATA.get("access")
        self.metadata_format = DATA.get("metadata_format")
        logger.debug("qp: eval loading DATA parameters as default")
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

        logger.debug("qp: eval define outputs")
        
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
            title = "QC files that need to be published",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )
        logger.debug("qp: eval finished init")


    def execute(self):
        logger.debug("qp: execute EvaluateQualityCheckProcess")
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        param_dict = dict(
                          data_node = self.data_node,
                          index_node = self.index_node,
                          access = self.access,
                          metadata_format = self.metadata_format,
                          replica = self.replica.getValue(),
                          latest = self.latest.getValue(),
                          )

        logger.debug("qp: eval init qcp")
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username.getValue(),
                                      parallel_id = self.parallel_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )

        logger.debug("qp: eval run evaluate")
        output = qcp.evaluate_quality_check(**param_dict)

        logger.debug("qp: eval setting outputs")
        self.fail_count.setValue(output["fail_count"])
        self.pass_count.setValue(output["pass_count"])
        self.omit_count.setValue(output["omit_count"])
        self.fixed_count.setValue(output["fixed_count"])
        self.has_issues.setValue(output["has_issues"])
        self.found_tags.setValue(output["found_tags"])

        process_log = _create_server_copy_of_file(output["process_log"],self)
        self.process_log.setValue(process_log)
        to_publish_qc_files_log = _create_server_copy_of_file(output["to_publish_qc_files_log"],self)
        self.to_publish_qc_files.setValue(to_publish_qc_files_log)
        return


class QualityPublisherProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        self.database_location = DATABASE_LOCATION
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_QualityPublisher", 
            title = "Publish generated quality XML files.",
            version = "2014.02.07",
            metadata = [],
            abstract = abstract_ml)
           
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. The processes folder contains the to upload files"
                      +" list."),
            default = "web1",
            type = types.StringType,
            )

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            abstract = ("The username is used to prevent two users working in the same directory."
                        +" Currently this is a work around until the username can be derived "
                        +"automatically."),
            default = "defaultuser",
            type = types.StringType,
            )
            

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of the process containing system calls.",
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )
    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        param_dict = dict(publish_method="swift",
                          subdir = "qualityxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      parallel_id = self.parallel_id.getValue(),
                                      )
        output = qcp.qualitypublisher(**param_dict)

        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        self.process_log.setValue(process_log)

        return

class MetaPublisherProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        self.database_location = DATABASE_LOCATION
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_QualityPublisher", 
            title = "Publish generated metadata XML files.",
            version = "2014.02.07",
            metadata = [],
            abstract = abstract_ml)
           
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. The processes_dir contains the to upload files"
                      +" list."),
            default = "web1",
            type = types.StringType,
            )

        self.username = self.addLiteralInput(
            identifier = "username",
            title = "Username",
            abstract = ("The username is used to prevent two users working in the same directory."
                        +" Currently this is a work around until the username can be derived "
                        +"automatically."),
            default = "defaultuser",
            type = types.StringType,
            )
            

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of the process containing system calls.",
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )
    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        param_dict = dict(publish_method="swift",
                          subdir = "metaxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      parallel_id = self.parallel_id.getValue(),
                                      )
        output = qcp.metadatapublisher(**param_dict)

        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        self.process_log.setValue(process_log)

        return
##################
# Helper methods #
##################

def statusmethod(msg,current,end,wpsprocess):
    """
    :param current: The current counter
    :param end: The end counter
    :param wpsprocess: The process that needs to update the statusbar.
    """
    #workaround division 0
    if int(end) == 0:
        end = 1
    wpsprocess.status.set(msg = msg, percentDone = float(current)*100.0/float(end),propagate = True)

def _create_server_copy_of_file(filename,wpsprocess):
    serverfile = open(wpsprocess.mktempfile(suffix = ".txt"),"w")
    localfile = open(filename,"r")
    serverfile.write(localfile.read())
    localfile.close()
    serverfile.close()
    return serverfile


