import nose.tools

from malleefowl import utils

import tempfile

from netCDF4 import Dataset

def test_nc_copy():
    dap_url = "http://bmbf-ipcc-ar5.dkrz.de/thredds/dodsC/cmip5.output1.MPI-M.MPI-ESM-LR.esmHistorical.day.atmos.day.r1i1p1.tas.20120315.aggregation"
    (_, nc_filename) = tempfile.mkstemp(suffix='.nc')
    istart = 10
    istop = 20
    utils.nc_copy(source=dap_url, target=nc_filename, istart=istart, istop=istop)

    nc_file = Dataset(nc_filename)
    nose.tools.ok_('time' in nc_file.dimensions.keys())
    time_dim = nc_file.dimensions.get('time')
    nose.tools.ok_(len(time_dim) == istop - istart, len(time_dim) )
    nose.tools.ok_('tas' in nc_file.variables.keys())
    
