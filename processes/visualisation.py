#!/usr/bin/python
import Nio
import datetime
from mpl_toolkits.basemap import Basemap , cm 
import numpy as np
import matplotlib.pyplot as plt
from pylab import * # also for the cmap colors



#print 'type the name of the ncFile'
#ncFile = raw_input()
#print 'type the variable name to be read'
#var = raw_input()
ncFile = "../out/n4_1990-2000.nc"
var = 'n4'

dataFile=Nio.open_file(ncFile,'r')

lat = dataFile.variables['lat'][:]
lon = dataFile.variables['lon'][:]
lons, lats = np.meshgrid(lon, lat)
#rawTime = dataFile.variables['time'][:]
data = np.squeeze(dataFile.variables[var][:])

meanData = np.max(data,axis=0)
  
# setup polyconic basemap
# by specifying lat/lon corners and central point.
# area_thresh=1000 means don't plot coastline features less
# than 1000 km^2 in area.
m = Basemap(llcrnrlon=-25.,llcrnrlat=-30,urcrnrlon=62.,urcrnrlat=38.,\
            resolution='l',area_thresh=1000.,projection='poly',\
            lat_0=0.,lon_0=20.)
#m = Basemap(llcrnrlon=lon[0],llcrnrlat=lat[0],urcrnrlon=lon[-1],urcrnrlat=lat[-1],\
            #resolution='l',projection='cyl')
            
clevs = (range(0,20,1))
            
im = m.contourf(lons,lats, meanData,latlon=True, cmap=cm.RdYlGn_r, levels=clevs, extend='max')
cb = m.colorbar(im,location='right',pad='10%', ticks=(range(0,20,10))) # , 
cb.set_label('Distribution of Anopheles gambiae',fontsize=16, fontstyle='oblique')

#m.drawlsmask(land_color='0.8', ocean_color='b', lsmask=None, lsmask_lons=None, lsmask_lats=None, lakes=True, resolution='l', grid=1.25)
m.bluemarble( alpha=0.8 )

# draw parallels and meridians.
m.drawstates()
m.drawparallels(np.arange(-80.,81.,20.), labels = [1, 0, 0, 0],fontsize=16)
m.drawmeridians(np.arange(-180.,181.,20.), labels = [0, 0, 0, 1],fontsize=16)
m.drawmapboundary(fill_color='aqua')
plt.title("Time slice 1990-2000")
plt.savefig('n4_max_1990-2000_test')
plt.show()
