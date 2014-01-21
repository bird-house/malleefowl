"""
Quality Control processes.

Author: Tobias Kipp (kipp@dkrz.de)
Creation date: 21.01.2014
"""
import types
import malleefowl.process 
import qc_processes.qcprocesses as qcprocesses

from multiprocessing import Process, Pipe

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
        self.database_location = "/home/tk/sandbox/databases/pidinfo.db"#TODO fixed relative to climdaps.

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
            default = str("--pb "),
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
        


    def execute(self):
        self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        param_dict = dict(project_data_dir = self.project_data_dir.getValue(),
                          args = self.args.getValue(),
                          project= self.project.getValue(),
                          data_node=self.data_node.getValue(),
                          index_node=self.index_node.getValue(),
                          access=self.access.getValue(),
                          metadata_format = self.metadata_format.getValue(),
                          replica = self.replica.getValue(),
                          latest = self.latest.getValue())

        qcp = qcprocesses.QCProcesses(self.database_location,
                                      self.parallel_id,
                                      printmethod=self.printmethod,
                                      work_dir = "/home/tk/sandbox/climdaps/var/qc_cache/"
                                      )
        _run_process(qcp.quality_control,kwargs=param_dict,wpsprocess=self)
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
    
