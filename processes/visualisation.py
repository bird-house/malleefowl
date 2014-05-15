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

        ncfiles = self.get_nc_files()
        var = self.variableIn.getValue()

        self.show_status('ncfiles and var : %s , %s ' % (ncfiles, var), 7)
        
        # define bokeh 
        output_file("output.html")
        save()
        
        self.show_status('output_file created:' , 5)
        
        hold()
        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
      
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
           
        self.show_status('timesseries lineplot done.' , 50)
        
        legend().orientation = "bottom_left"
        curplot().title = "Field mean of %s " % var  
        grid().grid_line_alpha=0.3

        window_size = 30
        window = np.ones(window_size)/float(window_size)
        
        save()
        hold('off')
        
        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
        
        hold()
        
        count = 0
        ma = []
        dates = []
        
        for nc in ncfiles:
            
            self.show_status('looping files : %s ' % (nc), 55)
            # get timestapms
            rawDate = cdo.showdate(input= nc) # ds.variables['time'][:]
            strDate = [elem.strip().split('  ') for elem in rawDate]
            dt = [datetime.strptime(elem, '%Y-%m-%d') for elem in strDate[0]]
            # get vaules
            ds=Dataset(nc)
            data = np.squeeze(ds.variables[var][:])
            meanData = np.mean(data,axis=1)
            ts = np.mean(meanData,axis=1)
            
            # merge to matirx
            
            if (count == 0 ):
                ma = ts 
                dates = dt
                ma = np.append([ma],[(np.empty((len(dates)))*np.NAN)], axis = 0)
                
                #ma = np.append([dt], [ts] , axis = 0 )               
            else :
                #ma = np.append(ma,[ts],axis = 0 )
                # add row for next ensemble menber
                ma = np.append(ma,[(np.empty((len(dates)))*np.NAN)], axis = 0)
                # check date and add value
                #self.show_status('time series to matix  %s' % count , 72)
                for t in range(len(dt)):
                    
                    #logger.debug("in loop")
                    
                    if dt[t] in dates :
                        x  = np.where(dates == dt[t])
                        ma[-1,x] = ts[t]
                        #self.show_status('date exist', 73)
                    else :
                        #self.show_status('date not exits', 73)
                        col = (np.empty( count +2 )*np.NAN)
                        #self.show_status('col %s ' % col, 74)
                        ma = np.c_[ma,col]
                        #self.show_status('ma done'  , 75)
                        dates = np.append(dates,[dt[t]])
                        #self.show_status('dates appended '  , 76)
                        
                        ma[-1,-1] = float(ts[t])
                        #self.show_status('col added ', 77)
                    
                    if (t % 500 == 0):
                        self.show_status('count %s timestep %s len dt %s' % (count, t, len(dt)) , 77)
                    #self.show_status('fill Vaules ' , 75)
            
            count +=1 
            #self.show_status('looping uncertainty plot %s' % count , 75)
        
        # Sorting the dates  
        #ma = ma[ma[0,:].argsort()]
        # calulate mean and percentiles:  
        #self.show_status('matrix created %s ' % ma , 80)
        
        # mask NANs
        
        mdat = np.ma.masked_array(ma ,np.isnan(ma))
        
        #self.show_status('matrix masked %s ' % mdat , 80)
        
        #logger.debug('matrix %s ', mdat.shape) 
        
        
        if (count > 1) : 
            ma_mean = np.mean(mdat,axis=0)
            self.show_status('mean  %s '%  ma_mean.shape, 97 )
            ma_min = np.min(mdat,axis=0)
            ma_max = np.max(mdat,axis=0)
            #ma_sub = np.subtract(ma_max, ma_min)
            #ma_per75 = np.percentile(mdat,75, axis=0)
            #ma_per25 = np.percentile(mdat,25, axis=0)
            self.show_status('ma Vaules %s' % mdat.data , 75)
        else : 
            ma_mean = ma_min = ma_max = mdat
            #self.show_status('ma_max Vaules %s' % ma_max , 75)
        
        #x = []
        #y = []
        #x = np.append(dates,dates)
        #y = np.append(ma_min.data, ma_max.data)

        #patch(x,y, color='grey', alpha=0.8, line_color=None)
        
        line(dates, ma_min , color='grey' ,line_width=1)
        line(dates, ma_max , color='grey' , line_width=2, legend =len(ncfiles)  )
        line(dates, ma_mean , color='red', line_width=3)
        #line(dates, ma_sub + 10  , color='red', line_width=3)
        #line(dates, ma_per25 , color='black', line_width=3)
        #line(dates, ma_per75 , color='grey', line_width=3)
        
        curplot().title = "Mean and Uncertainty of  %s " % var  
        save()
        hold('off')
        
        self.show_status('visualisation done', 99)
        self.output.setValue( 'output.html' )

