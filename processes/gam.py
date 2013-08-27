"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
from malleefowl.process import WorkflowProcess
import subprocess

class GamProcess(WorkflowProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkflowProcess.__init__(self, 
            identifier = "de.csc.gam",
            title="Gerneralized Additive Model",
            version = "0.1",
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            # TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Calculation of species distribution",
            grassLocation = False)

        # Literal Input Data
        # ------------------

        self.pa_in = self.addComplexInput(
            identifier="pa_in",
            title="CSV file PA-Data",
            abstract="example: http://localhost:8090/files/ICP_DATA_Fsylv_Pabie_2.csv",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=10,
            formats=[{"mimeType":"text/csv"}],
            )
       
        self.ref_in = self.addLiteralInput(
            identifier="ref_data",
            title="reference data experiment",
            abstract="Data Experiment Name of the reference data experiment",
            default="AFR-44_MPI-ESM-LR_historical_r1i1p1_MPI-RCSM-v2012_v1_day_.nc",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.pred_in = self.addLiteralInput(
            identifier="pred_data",
            title="prediction data experiment",
            abstract="Data Experiment Name of the prediction data experiment",
            default="AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_.nc",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )
     
        self.path_in = self.addLiteralInput(
            identifier="path_in",
            title="Path to Folder",
            abstract="Path to Folder containing input data",
            default="/home/main/data/",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )            
            
        self.individualBBoxIn = self.addLiteralInput(
            identifier="individualbbox",
            title="Individual BBox",
            abstract="This is a BBox: (minx,miny,maxx,maxy)",
            default="0,-90,180,90",
            type=type(''),
            minOccurs=0,
            maxOccurs=1,
            )

        self.start_date_ref = self.addLiteralInput(
            identifier="start_date_ref",
            title="Start reference",
            abstract="This is a start Date",
            default="2006-01-01",
            type=type(date(1971,01,01)),
            minOccurs=1,
            maxOccurs=1,
            )

        self.end_date_ref = self.addLiteralInput(
            identifier="end_date_ref",
            title="End reference",
            abstract="This is a end Date",
            default="2010-12-31",
            type=type(date(2000,12,31)),
            minOccurs=1,
            maxOccurs=1,
            )

        self.start_date_pred = self.addLiteralInput(
            identifier="start_date_pred",
            title="Start prediction",
            abstract="This is a start Date",
            default="2006-01-01",
            type=type(date(2071,01,01)),
            minOccurs=1,
            maxOccurs=1,
            )

        self.end_date_pred = self.addLiteralInput(
            identifier="end_date_pred",
            title="End prediction",
            abstract="This is a end Date",
            default="2010-12-31",
            type=type(date(2100,12,31)),
            minOccurs=1,
            maxOccurs=1,
            )
            
         #self.stringChoiceIn = self.addLiteralInput(
            #identifier="stringChoice",
            #title="String Choice",
            #abstract="Choose a string",
            #default="one",
            #type=type(''),
            #minOccurs=0,
            #maxOccurs=1,
            #allowedValues=['one', 'two', 'three']
            #)
            
        self.climin1 = self.addLiteralInput(
            identifier="climin1",
            title="temperature in vegetation period",
            abstract="temperture in vegetaion period",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=[0,1,2,3,4,5,6,7,8,9,10,11,12]
            )
            
        self.climin2 = self.addLiteralInput(
            identifier="climin2",
            title="precipitation in vegetation period",
            abstract="precipitation in vegetaion period",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.climin3 = self.addLiteralInput(
            identifier="climin3",
            title="temperture in dormancy",
            abstract="precipitation in dormancy",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )

        self.climin4 = self.addLiteralInput(
            identifier="climin4",
            title="Dummy",
            abstract="temperture in vegetaion period",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.climin5 = self.addLiteralInput(
            identifier="climin5",
            title="precipitation in spruting time",
            abstract="precipitation in spruting time",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.climin6 = self.addLiteralInput(
            identifier="climin6",
            title="temperture in spruting time",
            abstract="temperture in spruting time",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
            )
            
        self.dummy_out = self.addLiteralOutput(
            identifier="dummy_out",
            title="Dummy Out",
            abstract="This is a dummy out",
            #default="0,-90,180,90",
            type=type(''),
            )

        self.ref_out = self.addComplexOutput(
            identifier="ref_out",
            title="out file reference period",
            abstract="out file reference period",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         

        self.pred_out = self.addComplexOutput(
            identifier="pred_out",
            title="out file prediction period",
            abstract="out file prediction period",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
         
    def execute(self):
        # from os import curdir, path
        # nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/dkrz/gam_job.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(), self.end_date_in.getValue()], stdout=True)
        # literals
        # subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])
        self.file_out_ref.setValue("/home/main/wps_data/gam_ref.nc")
        self.file_out_pred.setValue("/home/main/wps_data/gam_pred.nc")
        self.file_out_pdf.setValue("/home/main/wps_data/gam.pdf")
        
        
