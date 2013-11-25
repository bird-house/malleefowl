import nose.tools
from nose import SkipTest

from malleefowl import utils

import tempfile

from netCDF4 import Dataset


def test_within_date_range():
    timesteps = ["2013-11-01T12:00:00.000Z", "2013-11-01T18:00:00.000Z", "2013-11-02T12:00:00.000Z",
                 "2013-11-08T12:00:00.000Z", "2013-11-09T12:00:00.000Z", "2013-11-12T12:00:00.000Z"]

    result = utils.within_date_range(timesteps, start="2013-11-01T18:00:00Z", end="2013-11-09T12:00:00Z")
    nose.tools.ok_(len(result) == 4, result)

def test_filter_timesteps():
    timesteps = ["2013-11-01T12:00:00.000Z", "2013-11-01T18:00:00.000Z", "2013-11-02T12:00:00.000Z",
                 "2013-11-08T12:00:00.000Z", "2013-11-09T12:00:00.000Z", "2013-11-12T12:00:00.000Z",
                 "2013-12-01T12:00:00.000Z", "2014-01-01T12:00:00.000Z"]
   
    result = utils.filter_timesteps(timesteps, aggregation="hourly")
    nose.tools.ok_(len(result) == 8, result)

    result = utils.filter_timesteps(timesteps, aggregation="daily")
    nose.tools.ok_(len(result) == 7, result)

    result = utils.filter_timesteps(timesteps,
                                    start="2013-11-01T12:00:00Z",
                                    end= "2014-01-01T12:00:00Z",
                                    aggregation="weekly")
    nose.tools.ok_(len(result) == 5, result)

    result = utils.filter_timesteps(timesteps, aggregation="monthly")
    nose.tools.ok_(len(result) == 3, result)

    result = utils.filter_timesteps(timesteps, aggregation="yearly")
    nose.tools.ok_(len(result) == 2, result)

    result = utils.filter_timesteps(timesteps, aggregation="hourly",
                                    start="2013-11-01T18:00:00Z",
                                    end="2013-12-01T12:00:00Z")
    nose.tools.ok_(len(result) == 6, result)

def test_filter_timesteps2():
    timesteps = ['2006-01-01T12:00:00.000Z',
                 '2006-01-02T12:00:00.000Z',
                 '2006-01-03T12:00:00.000Z',
                 '2006-01-04T12:00:00.000Z',
                 '2006-01-05T12:00:00.000Z',
                 '2006-01-06T12:00:00.000Z',
                 '2006-01-07T12:00:00.000Z',
                 '2006-01-08T12:00:00.000Z',
                 '2006-01-09T12:00:00.000Z',
                 '2006-01-10T12:00:00.000Z',
                 '2006-01-11T12:00:00.000Z',
                 '2006-01-12T12:00:00.000Z']
    result = utils.filter_timesteps(timesteps,
                                    start="2006-01-01T12:00:00Z",
                                    end="2006-01-12T12:00:00Z",
                                    aggregation="weekly")
    nose.tools.ok_(len(result) == 3, result)

def test_nc_copy():
    raise SkipTest

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
    
