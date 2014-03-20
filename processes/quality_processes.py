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

import os
from malleefowl import tokenmgr, utils
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

curdir = os.path.dirname(__file__)
climdapsabs = os.path.abspath(os.path.join(curdir,".."))

#DATABASE_LOCATION = os.path.join(climdapsabs,"examples/pidinfo.db")


DATA = {}
fn = os.path.join(os.path.dirname(__file__),"quality_processes.conf")

#logger.debug("qp: Loading data from file: "+fn)
execfile(fn,DATA)
#logger.debug("qp: Loaded file to DATA variable")

DATABASE_LOCATION = DATA["database_location"]
# "climdaps"
WORK_DIR = DATA["work_directory"]
#os.path.join(climdapsabs,"var/qc_cache/")

class UserDirectoryProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "UserDirectory",
            title = "UserDirectory",
            version = "2014.03.12",
            metadata = [],
            abstract = "Get a directory for the user.")

        self.token = self.addLiteralInput(
                identifier = "token",
                title = "Token",
                abstract = "The token that was generated for you",
                type = types.StringType,
                minOccurs = 1,
                maxOccurs = 1,
                )

        self.userdir = self.addLiteralOutput(
                identifier = "userdir",
                title = "User directory",
                type = types.StringType,
                )


    def execute(self):
        token = self.token.getValue()
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
        userid = userid.replace('@', '_')
        self.userdir.setValue(userid)


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
        #logger.debug("DSTITLE:"+ds_title)
        #the string of comma separated pids must be converted to a list
        dataset_pids = self.dataset_pids.getValue()
        dspids = dataset_pids.split(",")
        #logger.debug("DSPIDS:"+str(dspids))
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

#class PIDManagerPathSimpleProcess(malleefowl.process.WPSProcess):
#    def __init__(self):
#        malleefowl.process.WPSProcess.__init__(self,
#            identifier = "PIDManager_Path_simple",
#            title = "Get PIDs for a path using a generic pattern.",
#            version = "2014.03.11",
#            metadata = [],
#            )
#        
#        self.path = self.addLiteralInput(
#                identifier = "path",
#                title = "Root project data path",
#                default = os.path.join(climdapsabs,"examples/no_nc_files_pids"),
#                type = types.StringType,
#                minOccurs = 1,
#                maxOccurs = 1,
#                )
#        self.dataset_depth = self.addLiteralInput(
#                identifier = "dataset_depth",
#                title = "datset_depth",
#                default = 0,
#                type = types.IntType,
#                abstract = ("The depth of directories that form a dataset. (e.g. There are " +
#                            " three directories a, b and c in the tree /x/y/z/{a,b,c}. With " +
#                            " depth = 0 the datasets are x.y.z.a , x.y.z.b and x.y.z.c." +
#                            " For depth = 3 the only dataset is x)"),
#                )
#
#        self.file_regexp = self.addLiteralInput(
#                identifier = "file_regexp",
#                title = "Regular expression to filter files",
#                default = "*.document",
#                type = types.StringType,
#                abstract = ("The syntax is '*' for any number of random characters. '.' is the normal" +
#                            "textual dot. (e.g. */fx/*.nc includes all .nc files in a fx directory")
#                )
#
#        self.additional_identifier_element = self.addLiteralInput(
#                identifier = "additional_identifier_element",
#                title = "additional_identifier_element",
#                type = types.StringType,
#                default = "CORDEX-SIMPLE-",
#                abstract = ("The generated PIDS consist of prefix/additional_identifier_element" +
#                            " random string")
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
#        def _dataset_directory_to_title_simple(data_root, path):
#            from qc_processes.stringmethods import lremove
#            sub_path = lremove(path, data_root + "/")
#            return sub_path.replace("/",".")
#        d2dy = directory2datasetyaml.Directory2DatasetYaml(
#                dataset_directory_to_title = _dataset_directory_to_title_simple,
#                dataset_depth = self.dataset_depth.getValue())
#        tempfile = self.mktempfile()
#        regexp_raw = self.file_regexp.getValue()
#        #. has to be escaped and * has to be replaced by .*
#        regexp = regexp_raw.replace(".","\.").replace("*",".*")
#        self.file_regexp_out.setValue(regexp)
#        d2dy.create_yaml(path = self.path.getValue(), yaml_fn = tempfile, 
#                file_regexp = regexp)
#        pm = pidmanager.PIDManager(DATABASE_LOCATION, 
#                additional_identifier_element = self.additional_identifier_element.getValue())
#        pids = pm.get_pids_from_yaml_file(tempfile)
#        self.pids.setValue(str(pids))


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
        #logger.debug(self.username)

        qcp = qcprocesses.QCProcesses(
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

        def statmethod(cur,end):
            statusmethod("Running",cur,end,self)
        qcp = qcprocesses.QCProcesses(
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


        self.data_node = DATA.get("data_node")

        self.index_node = DATA.get("index_node")

        self.access = DATA.get("access")
        self.metadata_format = DATA.get("metadata_format")
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
        qcp = qcprocesses.QCProcesses(
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

        qcp = qcprocesses.QCProcesses(
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

        qcp = qcprocesses.QCProcesses(
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
            qcp = qcprocesses.QCProcesses(
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
                qcp = qcprocesses.QCProcesses(
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


###########################
# Processes with username #
# Not intended for use    #
# with the generic        #
# interface               #
###########################
class RestflowLocalFile(malleefowl.process.WPSProcess):
    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "RestflowLocalFile",
            title = "RestflowLocalFile",
            version = "2014.03.17",
            metadata = [],
            )

        self.filename = self.addLiteralInput(
            identifier = "filename",
            title = "filename",
            type = types.StringType,
            minOccurs = 1,
            maxOccurs = 1,
            )
        
    
    
    def execute(self):
        from malleefowl import restflow
        status = lambda msg, percent: self.show_status(msg, percent)
        filename = self.filename.getValue()
        restflow.run(filename, timeout=20000, status_callback=status)


def get_username(obj):
    username = obj.username.getValue().replace("(at)","@")
    token = obj.token.getValue()
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
    if username != userid:
        username == "defaultuser"
    return username

class UserInitProcess(malleefowl.process.WPSProcess):
    """
    The process is not intended for use with the generic tool
    """
    def __init__(self):

       
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Init_User",
            title = "QualityProcesses initialization with username",
            version = "2014.03.17",
            metadata = [],
            abstract = "If the given directory is valid included files and datasets receive a PID.")

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
            )

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            default = "web1",
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
            identifier = "QC_Check_User",
            title = "Quality Check",
            version = "2014.03.17",
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
            type = types.StringType,
            )

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = ("An ID for the current process. Select the one matching to the directory "+
                        "requirements check process."),
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

class UserEvalProcess(malleefowl.process.WPSProcess):
    """
    The process runs the qc_processes QualityControl method and 
    handles the progress bar depending on the output of the method.
    """
    def __init__(self):


        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Eval_User",
            title = "Quality Evaluate with username",
            version = "2014.03.17",
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
            )

        self.parallel_id = self.addLiteralInput(
            identifier = "parallel_id",
            title = "Parallel ID",
            abstract = "An ID for the current process. Select the one matching to the quality check.",
            type = types.StringType,
            )


        self.data_node = DATA.get("data_node")

        self.index_node = DATA.get("index_node")

        self.access = DATA.get("access")
        self.metadata_format = DATA.get("metadata_format")
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

        output = qcp.evaluate_quality_check(
                          data_node = self.data_node,
                          index_node = self.index_node,
                          access = self.access,
                          metadata_format = self.metadata_format,
                          replica = self.replica.getValue(),
                          latest = self.latest.getValue(),
                          )

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

class UserQualityPublishPublish(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_QualityPublisher_User", 
            title = "Publish Quality-XML User",
            version = "2014.03.18",
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

class MetaPublisherUserProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml = ("Read trough a file containing one filename per line and publish it.")

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_MetaPublisher_User", 
            title = "Quality Publish Metadata-XML User",
            version = "2014.03.18",
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
        logger.debug("QC_META_PUBLISHER:" + username)
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

class RemoveDataUserProcess(malleefowl.process.WPSProcess):
    def __init__(self):

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_RemoveData_User", 
            title = "Quality Clean up User",
            version = "2014.03.18",
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
            logger.debug("Removing " + par_id)
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
