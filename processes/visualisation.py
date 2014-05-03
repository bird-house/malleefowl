#!/usr/bin/python

"""
Processes for visualisation 
Author: Nils Hempelmann (nils.hempelmann@hzg.de)
"""

from datetime import datetime, date
import tempfile
import subprocess


#from malleefowl.process import WorkerProcess
import malleefowl.process
class VisualisationProcess(malleefowl.process.WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        malleefowl.process.WorkerProcess.__init__(self, 
            identifier = "de.csc.visualisation",
            title="Simple visualisation tool",
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
        import datetime
        import numpy as np
        from bokeh.plotting import *

        output = output_file("stocks.html", title="Phoenix visualisation")
        
        ncfile = self.get_nc_files()

        var = self.variableIn.getValue()
        dataFile=Dataset(ncfile)
        
        rawTime = dataFile.variables['time'][:]
        data = np.squeeze(dataFile.variables[var][:])
        meanData = np.mean(data,axis=1)
        ts = np.mean(meanData,axis=1)
        
        hold()

        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
        
        line( rawTime, ts , legend = var)
        
        curplot().title = "Stock Closing Prices"
        grid().grid_line_alpha=0.3

        window_size = 30
        window = np.ones(window_size)/float(window_size)

        self.status.set(msg="visualisation done", percentDone=90, propagate=True)
        self.output.setValue( output )


        #result_pic = path.join(path.abspath(curdir), "result.png")
        
        #print 'type the name of the ncFile'
        #ncFile = raw_input()
        #print 'type the variable name to be read'
        #var = raw_input()
        #ncFile = self.get_nc_files()[0]
        

        

        #lat = dataFile.variables.get('rlat')
        #lon = dataFile.variables.get('rlon')
        #lons, lats = np.meshgrid(lon, lat)
        ##rawTime = dataFile.variables['time'][:]
        

        
        
        ## setup polyconic basemap
        ## by specifying lat/lon corners and central point.
        ## area_thresh=1000 means don't plot coastline features less
        ## than 1000 km^2 in area.
        #m = Basemap(llcrnrlon=-25.,llcrnrlat=-30,urcrnrlon=62.,urcrnrlat=38.,\
                    #resolution='l',area_thresh=1000.,projection='poly',\
                    #lat_0=0.,lon_0=20.)
        ##m = Basemap(llcrnrlon=lon[0],llcrnrlat=lat[0],urcrnrlon=lon[-1],urcrnrlat=lat[-1],\
                    ##resolution='l',projection='cyl')
                    
        ##clevs = (range(0,20,1))
                    
        #im = m.contourf(lons,lats, meanData,latlon=True) #, cmap=cm.RdYlGn_r, levels=clevs, extend='max'
        #cb = m.colorbar(im,location='right',pad='10%') # ,, ticks=(range(0,20,10)) 
        #cb.set_label('Nice legende',fontsize=16, fontstyle='oblique')

        ##m.drawlsmask(land_color='0.8', ocean_color='b', lsmask=None, lsmask_lons=None, lsmask_lats=None, lakes=True, resolution='l', grid=1.25)
        #m.bluemarble( alpha=0.8 )

        ## draw parallels and meridians.
        #m.drawstates()
        #m.drawparallels(np.arange(-80.,81.,20.), labels = [1, 0, 0, 0],fontsize=16)
        #m.drawmeridians(np.arange(-180.,181.,20.), labels = [0, 0, 0, 1],fontsize=16)
        #m.drawmapboundary(fill_color='aqua')
        #plt.title("Title")
        #plt.savefig(result_pic)
        ## plt.show()

