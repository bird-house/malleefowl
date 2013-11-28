"""
The PID Generator process searches a given path for a valid structure and then generates 
a PID for each file and dataset. 

Date: 14.11.2013
Author: Tobias Kipp (kipp@dkrz.de)
"""
import malleefowl.process 
import types
import processes.qc.pidgenerator as pidgenerator

class PIDGenerationProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml =("The process searches a given path for a valid structure and then "+
                      "generates a PID ofr each file and dataset.")
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "pidgenerator", 
            title="Generate PIDs for given path.",
            version = "0.1",
            metadata=[],
            abstract=abstract_ml)
            
        self.data_path= self.addLiteralInput(
            identifier="datapath",
            title="Root path of the to index data.",
            default="/home/tk/sandbox/qc-yaml/data8/",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )
           
        self.database_location = self.addLiteralInput(
            identifier = "dblocation",
            title = "Location of the database",
            default = "/home/tk/sandbox/databases/pidinfo.db",
            type=types.StringType,
            minOccurs=1,
            maxOccurs=1,
            )

        self.data_node = self.addLiteralInput(
            identifier = "data_node",
            title = "Data node",
            default = "ipcc-ar5.dkrz.de",
            type=types.StringType,
            )

        self.is_valid =self.addLiteralOutput(
            identifier = "isvalid",
            title = "The path has a valid structure.",
            default = False,
            type=types.BooleanType,
            )


        self.errors = self.addLiteralOutput(
            identifier = "errors",
            title = "Error messages",
            default = "",
            type=types.StringType,
            )

    def execute(self):
        #self.status.set(msg="Initiate process", percentDone=0, propagate=True)
        pg = pidgenerator.PidGenerator(self.database_location.getValue(),self.data_node.getValue())
        valid = pg.create_pids(self.data_path.getValue())
        self.is_valid.setValue(valid)
        errortext = "\n".join(pg.errors)
        self.errors.setValue(errortext)

        
