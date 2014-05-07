"""
Processes for visualisation 
Author: Nils Hempelmann (nils.hempelmann@hzg.de)
"""
from datetime import datetime, date
import tempfile
import subprocess
from malleefowl import tokenmgr, utils
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

#from malleefowl.process import WorkerProcess
import malleefowl.process
class VisualisationProcess(malleefowl.process.WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        malleefowl.process.WorkerProcess.__init__(self, 
            identifier = "de.csc.visualisation",
            title="Visualisation of data",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to visualise some variables",
            #extra_metadata={
                  #'esgfilter': 'variable:tas,variable:evspsbl,variable:hurs,variable:pr',  #institute:MPI-M, ,time_frequency:day
                  #'esgquery': 'variable:tas AND variable:evspsbl AND variable:hurs AND variable:pr' # institute:MPI-M AND time_frequency:day 
                  #},
            )
            
        # Literal Input Data
        # ------------------

        self.variableIn = self.addLiteralInput(
             identifier="variable",
             title="Variable",
             abstract="Variable to be expected in the input files",
             default="tas",
             type=type(''),
             minOccurs=0,
             maxOccurs=1,
             )
       
        self.output = self.addComplexOutput(
            identifier="output",
            title="visualisation",
            abstract="Visualisation of single variables",
            formats=[{"mimeType":"application/html"}],
            asReference=True,
            )         
            
    def execute(self):
        
        from netCDF4 import Dataset
        from os import curdir, path
        from datetime import datetime
        import numpy as np
        import cdo 
        from bokeh.plotting import *
        
        cdo = cdo.Cdo()

        output_file("output.html")
        save()
        
        self.show_status('output_file created:' , 5)
        
        hold()
        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
        
        
        ncfiles = self.get_nc_files()
        var = self.variableIn.getValue()

        self.show_status('ncfiles and var : %s , %s ' % (ncfiles, var), 7)
        
        
        for nc in ncfiles:
            
            self.show_status('looping files : %s ' % (nc), 10)
        
            # get timestapms
            rawDate = cdo.showdate(input= nc) # ds.variables['time'][:]
            strDate = [elem.strip().split('  ') for elem in rawDate]
            dt = [datetime.strptime(elem, '%Y-%m-%d') for elem in strDate[0]]
            
            # get vaules
            ds=Dataset(nc)
            data = np.squeeze(ds.variables[var][:])
            meanData = np.mean(data,axis=1)
            ts = np.mean(meanData,axis=1)
            
            # plot into current figure
            line( dt,ts ) # , legend= nc 
            
            save()
        
        self.show_status('timesseries lineplot done.' , 90)
        
        legend().orientation = "bottom_left"
        curplot().title = "Field mean of %s " % var  
        grid().grid_line_alpha=0.3

        window_size = 30
        window = np.ones(window_size)/float(window_size)
        
        save()

        self.show_status('visualisation done', 99)
        self.output.setValue( 'output.html' )

