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

curdir = os.path.dirname(__file__)
climdapsabs = os.path.abspath(os.path.join(curdir,"../../.."))

DATABASE_LOCATION = os.path.join(climdapsabs,"examples/pidinfo.db")
WORK_DIR = os.path.join(climdapsabs,"var/qc_cache/")


DATA = {}
fn = os.path.join(os.path.dirname(__file__),"quality_processes.conf")
logger.debug("qp: Loading data from file: "+fn)
execfile(fn,DATA)
logger.debug("qp: Loaded file to DATA variable")

class DirectoryValidatorProcess(malleefowl.process.WPSProcess):
    """
    The process checks a path to be valid for processing and generates a PID
    for each new found .nc file and for each dataset that differs from the 
    previously found ones. This relies on the local database storing the previously
    found files and datasets.
    """
    def __init__(self):

       
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_DirectoryValidator",
            title = "Quality Initialization",
            version = "2014.02.27",
            metadata = [],
            abstract = "If the given directory is valid included files and datasets receive a PID.")

        self.username = "defaultuser"

        selectable_parallelids = get_user_parallelids(self.username)
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            default = "web1",
            abstract = ("An identifier used to avoid processes running on the same directory." +
                        "Using an existing one will remove all data inside its work directory."+
                        "The existing Parallel IDs are: "+", ".join(sorted(selectable_parallelids))
                        ),
            type = types.StringType,
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

        self.project = self.addLiteralInput(
            identifier = "project",
            title = "Project",
            abstract = "Currently only CORDEX is fully supported.",
            default = "CORDEX",
            allowedValues = ["CORDEX"],
            type = types.StringType,
            )


    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        data_path = self.data_path.getValue()
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        qcp = qcprocesses.QCProcesses(DATABASE_LOCATION,
                                      username = self.username,
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

        return 


class QualityCheckProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        self.database_location = DATABASE_LOCATION

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Quality_Check",
            title = "Quality Check",
            version = "2014.03.04",
            metadata = [],
            abstract = "Runs a quality check on a given folder.")

        self.username = "defaultuser"

        selectable_parallelids = get_user_parallelids(self.username)
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. Select the one matching to the directory "+
                        "requirements check process."),
            allowedValues = selectable_parallelids,
            type = types.StringType,
            )


        self.select = self.addLiteralInput(
            identifier = "select",
            title = "QC SELECT",
            abstract = ("Comma separated list of parts of the path." + 
                        " If at least one of the elements in the list matches with a path in the data" +
                        " directory, its nc files are added to the check." +
                        " added. At least one '/' must in per list element." +
                        " The first element of the path does not start with a '/' and the" +
                        " last element does not end with a '/'." +
                        " The wildcard '.*'" +
                        " should be used with care, to avoid unexpected results." +
                        " (Assuming the paths exist a valid example is:" +
                        " AFR-44/.*/tas, EUR.*/, /fx/)"),
            minOccurs = 0,
            maxOccurs = 1,
            type = types.StringType,
            )

        self.lock = self.addLiteralInput(
            identifier = "lock",
            title = "QC LOCK",
            abstract = ("Works similar to select, but prevents the given paths being added."+
                        " Lock is stronger than select. (e.g. select /tas and lock AFR-44/ "+
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

        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username,
                                      parallel_id = self.parallel_id.getValue(),
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR
                                      )
        #load the data_path
        data_path_file = open(os.path.join(qcp.process_dir,"data_path"),"r") 
        data_path = data_path_file.readline()
        data_path_file.close()
        selects = self.select.getValue()
        if selects == '<colander.null>':
            selects =  ""
        locks = self.lock.getValue()
        if locks == '<colander.null>':
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
            title = "Quality Evaluate",
            version = "2014.02.27",
            metadata = [],
            abstract = "Evaluates the quality check and generates metadata and quality files")

        self.username = "defaultuser"

        selectable_parallelids = get_user_parallelids(self.username)
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = "An ID for the current process. Select the one matching to the quality check.",
            allowedValues = selectable_parallelids,
            type = types.StringType,
            )


        logger.debug("qp: eval Loading DATA parameters")
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
                                      username = self.username,
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
        to_publish_metadata_files_log = _create_server_copy_of_file(
            output["to_publish_metadata_files_log"],self)
        self.to_publish_metadata_files.setValue(to_publish_metadata_files_log)
        return


class QualityPublisherProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        self.database_location = DATABASE_LOCATION
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_QualityPublisher", 
            title = "Quality Publish Quality-XML",
            version = "2014.02.27",
            metadata = [],
            abstract = abstract_ml)
           
        self.username = "defaultuser"    

        selectable_parallelids = get_user_parallelids(self.username)
        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. Select the one matching to the evaluation."),
            allowedValues = selectable_parallelids,
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
        param_dict = dict(publish_method="swift",
                          subdir = "qualityxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username,
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
            identifier = "QC_MetaPublisher", 
            title = "Quality Publish Metadata-XML",
            version = "2014.02.27",
            metadata = [],
            abstract = abstract_ml)
           
        self.username = "defaultuser"

        selectable_parallelids = get_user_parallelids(self.username)

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. Select the one matching to the evaluation."),
            allowedValues = selectable_parallelids,
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
        param_dict = dict(publish_method="swift",
                          subdir = "metaxml",
                          keyfile = os.path.join(climdapsabs,"stvariables.conf")
                         )
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      parallel_id = self.parallel_id.getValue(),
                                      )
        output = qcp.metadatapublisher(**param_dict)

        process_log = _create_server_copy_of_file(output["process_log_name"],self)
        self.process_log.setValue(process_log)

        return

class RemoveDataProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        self.database_location = DATABASE_LOCATION

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_RemoveData", 
            title = "Quality Clean up",
            version = "2014.02.27",
            metadata = [],
            abstract = "Remove data by Parallel ID or your complete work data.")
           
        self.username = "defaultuser"

        selectable_parallelids = get_user_parallelids(self.username)

        self.parallel_ids = self.addLiteralInput(
            identifier = "parallel_ids",
            title = "Parallel IDs",
            abstract = ("IDs for the to remove work data."),
            allowedValues = selectable_parallelids,
            minOccurs = 0,
            maxOccurs = len(selectable_parallelids),
            type = types.StringType,
            )
        
        self.remove_user_dir = self.addLiteralInput(
            identifier = "remove_user_dir",
            title = "Remove all work data",
            abstract = ("Every data generated by the quality processes will be deleted. Use this only" +
                        " if you really mean it."),
            type = types.BooleanType,
            )

    def execute(self):
        self.status.set(msg = "Initiate process", percentDone = 0, propagate = True)
        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)

        remove_all = self.remove_user_dir.getValue()
        if remove_all:
            qcp = qcprocesses.QCProcesses(self.database_location,
                                      username = self.username,
                                      statusmethod = statmethod,
                                      work_dir = WORK_DIR,
                                      parallel_id = "deleteall",
                                      )
            qcp.remove_user_dir()
        else:
            parallel_ids = self.parallel_ids.getValue()
            if parallel_ids == None:
                parallel_ids = []
            cur = 0
            end = len(parallel_ids)
            for par_id in parallel_ids:
                qcp = qcprocesses.QCProcesses(self.database_location,
                                          username = self.username,
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
