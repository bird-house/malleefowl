"""
Processes for rel_hum 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
from malleefowl.process import WorkflowProcess
import subprocess

class ClimInProcess(WorkflowProcess):
    """This process calculates the relative humidity"""

    def __init__(self):
        # definition of this process
        WorkflowProcess.__init__(self, 
            identifier = "de.csc.clim_in",
            title="Calculation of climate indices",
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
        
        self.floatIn = self.addLiteralInput(
            identifier="float",
            title="Base temperature",
            abstract="Threshold for termal vegetation period",
            default="5.6",
            type=type(0.1),
            minOccurs=1,
            maxOccurs=1,
            )
        
        self.climin1 = self.addLiteralInput(
            identifier="climin1",
            title="temperature in vegetation period",
            abstract="temperture in vegetaion period",
            type=type(False),
            minOccurs=1,
            maxOccurs=1,
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
        
        self.dummyBBoxIn = self.addLiteralInput(
           identifier="dummybbox",
           title="Dummy BBox",
           abstract="This is a BBox: (minx,miny,maxx,maxy)",
           default="0,-90,180,90",
           type=type(''),
           minOccurs=0,
           maxOccurs=1,
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
        result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/clim_in", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()], stdout=True)
        # literals
        # subprocess.check_output(["/home/main/sandbox/climdaps/src/ClimDaPs_WPS/processes/dkrz/rel_hum.sh", self.path_in.getValue(), self.stringIn.getValue(), self.individualBBoxIn.getValue(), self.start_date_in.getValue(),   self.end_date_in.getValue()])
        self.file_out.setValue("/home/main/wps_data/hurs_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc")
        
        
