"""
Quality Control processes.

Author: Tobias Kipp (kipp@dkrz.de)
Creation date: 21.01.2014
"""
import types
import malleefowl.process 
import qc_processes.qcprocesses as qcprocesses

from multiprocessing import Process, Pipe, Queue

class QualityControlProcess(malleefowl.process.WPSProcess):
    """
    The process runs the qc_processes QualityControl method and 
    handles the progress bar depending on the output of the method.
    """
    def __init__(self):

        self.pipe = Pipe()
        status_conn, qc_conn = self.pipe
        self.printmethod = qc_conn.send
        self.parallel_id = "web1"#TODO set as parameter
        self.database_location = "/home/tk/sandbox/databases/pidinfo.db"#TODO relative to climdaps.
       

        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QC_Quality_Control",
            title="Quality Control using qc_processes",
            version="2014.01.20",
            metadata=[],
            abstract="Runs a quality check on a given folder and generates metadata and quality files")

        self.project_data_dir = self.addLiteralInput(
            identifier="project_data_dir",
            title="To analyse data path",
            abstract="A local path",
            default="/home/tk/sandbox/qc-yaml/data3/CORDEX",
            type=types.StringType,
            )

        self.args = self.addLiteralInput(
            identifier = "args",
            title="Additional QC parameters",
            abstract = "Using options of the QC tool. (e.g. -E_SELECT .*)",
            #default = str("--pb "),
            minOccurs = 0,
            maxOccurs = 1,
            type=types.StringType,
            )

        self.project = self.addLiteralInput(
            identifier="project",
            title="The project used.",
            abstract="Currently only CORDEX is fully supported.",
            default="CORDEX",
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
        
        self.qc_call_exit_code = self.addLiteralOutput(
            identifier="qc_call_exit_code",
            title ="qcManager exit code",
            abstract ="Exit code of the quality control tool.",
            type = types.StringType,
            )

        self.qc_call = self.addLiteralOutput(
            identifier="qc_call",
            title="qc_call",
            type=types.StringType,
            )
        self.fail_count = self.addLiteralOutput(
            identifier="fail_count",
            title="Fail count",
            type=types.IntType,
            )
        self.omit_count = self.addLiteralOutput(
            identifier="omit_count",
            title="Omit count",
            type=types.IntType,
            )
        self.pass_count = self.addLiteralOutput(
            identifier="pass_count",
            title="Pass count",
            type=types.IntType,
            )
        self.fixed_count = self.addLiteralOutput(
            identifier="fixed_count",
            title="Fixed count",
            type=types.IntType,
            )

        self.has_issues = self.addLiteralOutput(
            identifier="has_issues",
            title="There is something wrong with the checked files.",
            type=types.BooleanType,
            )

        self.process_log = self.addComplexOutput(
            identifier="process_log",
            title="Log of this process.",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
        self.to_publish_qc_files = self.addComplexOutput(
            identifier="to_publish_qc_files",
            title="QC files that need to be published",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )


    def execute(self):
        self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        output=dict()
        param_dict = dict(project_data_dir = self.project_data_dir.getValue(),
                          args = self.args.getValue(),
                          project= self.project.getValue(),
                          data_node=self.data_node.getValue(),
                          index_node=self.index_node.getValue(),
                          access=self.access.getValue(),
                          metadata_format = self.metadata_format.getValue(),
                          replica = self.replica.getValue(),
                          latest = self.latest.getValue(),
                          queue = Queue())

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      self.parallel_id,
                                      printmethod=self.printmethod,
                                      work_dir = "/home/tk/sandbox/climdaps/var/qc_cache/"
                                      )
        _run_process(qcp.quality_control,kwargs=param_dict,wpsprocess=self)

        output = param_dict["queue"].get()
        self.qc_call_exit_code.setValue(output["qc_info"])
        self.qc_call.setValue(output["qc_call"])
        self.fail_count.setValue(output["fail_count"])
        self.pass_count.setValue(output["pass_count"])
        self.omit_count.setValue(output["omit_count"])
        self.fixed_count.setValue(output["fixed_count"])
        self.has_issues.setValue(output["has_issues"])

        process_log = _create_server_copy_of_file(output["process_log"],self)
        self.process_log.setValue(process_log)
        to_publish_qc_files_log = _create_server_copy_of_file(output["to_publish_qc_files_log"],self)
        self.to_publish_qc_files.setValue(to_publish_qc_files_log)



        return

##################
# Helper methods #
##################

def _run_process(target,kwargs,wpsprocess):
    status_conn, qc_conn = wpsprocess.pipe
    p = Process(target=target,kwargs = kwargs)
    p.start()
    run = True
    while run:
        val = status_conn.recv()
        stat = "status:"
        if(val[:len(stat)]==stat):
            sval = val.split(" ")
            if(len(sval)==3):
               try:
                   cur = float(sval[1])
                   end = float(sval[2])
                   wpsprocess.status.set(msg="runnig", percentDone=cur*100.0/end,propagate=True)
                   if(cur==end):
                       run = False
               except:
                   pass#it is not of "status: current max" format. Ignore it.
    

def _create_server_copy_of_file(filename,wpsprocess):
    serverfile =  open(wpsprocess.mktempfile(suffix=".txt"),"w")
    localfile = open(filename,"r")
    serverfile.write(localfile.read())
    localfile.close()
    serverfile.close()
    return serverfile


