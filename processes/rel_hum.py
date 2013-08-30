"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
from malleefowl.process import WorkflowProcess
import subprocess

class RelHumProcess(WorkflowProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkflowProcess.__init__(self, 
            identifier = "de.csc.relhum",
            title="Specific to relative humidity",
            version = "0.1",
            #storeSupported = "true",   # async
            #statusSupported = "true",  # retrieve status, needs to be true for async 
            ## TODO: what can i do with this?
            metadata=[
                {"title":"Foobar","href":"http://foo/bar"},
                {"title":"Barfoo","href":"http://bar/foo"},
                {"title":"Literal process"},
                {"href":"http://foobar/"}],
            abstract="Just testing a nice script to calculate the relative humidity ...",
            )

        # Literal Input Data
        # ------------------

        self.stringIn = self.addLiteralInput(
            identifier="data_experiment",
            title="Data Experiment Name",
            abstract="Data Experiment Name",
            default="AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc",
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
            
        self.definedBBoxIn = self.addLiteralInput(
            identifier="definedbbox",
            title="Cordex domain BBox",
            abstract="This is a BBox: (minx,miny,maxx,maxy)",
            default='AFR-44',
            allowedValues=['AFR-44', 'EUR-11', 'MENA-44'],
            type=type(''),
            minOccurs=0,
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

        self.start_date_in = self.addLiteralInput(
            identifier="start_date",
            title="Start",
            abstract="This is a start Date",
            default="2006-01-01",
            type=type(date(2006,7,11)),
            minOccurs=1,
            maxOccurs=1,
            )

        self.end_date_in = self.addLiteralInput(
            identifier="end_date",
            title="End",
            abstract="This is a end Date",
            default="2010-12-31",
            type=type(date(2006,7,11)),
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

        self.file_out = self.addComplexOutput(
            identifier="file_out",
            title="Out File",
            abstract="out file",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        
        # from os import curdir, path
        # nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()], stdout=True)
        # literals
        # subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])
        self.file_out.setValue("/home/main/wps_data/hurs_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc")
        
        
