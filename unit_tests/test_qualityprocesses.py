from nose.tools import assert_equal, assert_true
from pywps import config
import os
from processes.quality_processes import UserInitProcess
from processes.quality_processes import UserCheckProcess
from processes.quality_processes import UserEvalProcess
from processes.quality_processes import UserInitWithYamlLogsProcess
from processes.quality_processes import UserRemoveDataProcess
from nose.plugins.attrib import attr
from __init__ import SERVICE
from malleefowl import wpsclient

WORK_DIR = config.getConfigValue("malleefowl", "work_directory")

@attr('slow')
def test_chains():
    #init
    result_init = wpsclient.execute(
        service = SERVICE,
        identifier = "QC_Init",
        inputs = [('parallel_id', 'unittest')],
        outputs = [('all_okay', True), ('process_log', True)]
        )
    #The process generates a file named data_path. 
    data_path_file = os.path.join(WORK_DIR, "defaultuser", "unittest", "data_path")
    assert_true(os.path.exists(data_path_file))
    #The check takes 13 seconds and is therefore commented out. It can be tested
    #by running the wizard for the check version  with default settings
    result_check = wpsclient.execute(
        service = SERVICE,
        identifier = "QC_Check",
        inputs = [('parallel_id', 'unittest')],
        outputs = [('qc_call_exit_code', False), ('qc_call', True), ('qc_svn_version', True), 
                   ('error_messages', True)]
        )
    qc_call_exit_code = result_check[0]["data"][0]
    assert_equal(qc_call_exit_code, '0')
    #eval
    result_evaluate = wpsclient.execute(
        service = SERVICE,
        identifier = "QC_Evaluate",
        inputs = [('parallel_id', 'unittest')],
        outputs = [('fail_count', False), ('pass_count', False), ('omit_count', False), 
                   ('fixed_count', False)]
        )
    fail_count = result_evaluate[0]["data"][0]
    pass_count = result_evaluate[1]["data"][0]
    omit_count = result_evaluate[2]["data"][0]
    fixed_count = result_evaluate[3]["data"][0]
    assert_equal(fail_count, '5')
    assert_equal(pass_count, '5')
    assert_equal(omit_count, '2')
    assert_equal(fixed_count, '0')
    #run the alternative init. Use the logfiles from the check.
    log_path  = os.path.join(WORK_DIR, "defaultuser", "unittest", "qc_results", "check_logs")
    log1 = os.path.join(log_path,"AFR-44_CLMcom_MPI-ESM-LR_historical_r0i0p0.log")
    log2 = os.path.join(log_path,"AFR-44_CLMcom_MPI-ESM-LR_historical_r1i1p1.log")
    wpsclient.execute(
        service = SERVICE,
        identifier = "QC_Init_With_Yamllogs",
        inputs = [('parallel_id', 'unittest2'), ('yamllogs', log1) , ('yamllogs', log2)],
        outputs = []
        ) 
    wpsclient.execute(
        service = SERVICE,
        identifier = "QC_Evaluate",
        inputs = [('parallel_id', 'unittest2')],
        outputs = [('fail_count', False), ('pass_count', False), ('omit_count', False), 
                   ('fixed_count', False)]
        )
    #Both evaluates should have generated the same files.
    xml_files_path_1 = os.path.join(WORK_DIR, "defaultuser", "unittest", "xml_output")
    xml_files_path_2 = os.path.join(WORK_DIR, "defaultuser", "unittest2", "xml_output")
    xml_files_1 = sorted(os.listdir(xml_files_path_1))
    xml_files_2 = sorted(os.listdir(xml_files_path_2))
    assert_equal(xml_files_1, xml_files_2)
    for basename in xml_files_2:
        f = open(os.path.join(xml_files_path_1, basename),"r")
        g = open(os.path.join(xml_files_path_2, basename),"r")
        assert_equal(f.read(),g.read())
    #publish skipped for chain test
    ##remove the test directories
    wpsclient.execute(
        service = SERVICE,
        identifier = "QC_RemoveData",
        inputs = [('parallel_ids', 'unittest'), ('parallel_ids', 'unittest2')],
        outputs = []
        )


