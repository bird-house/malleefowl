import pytest

from malleefowl import utils

import tempfile
from netCDF4 import Dataset


@pytest.mark.skipif(reason="no way of currently testing this")
def test_esgf_archive_path_cordex():
    url = "http://esgf1.dkrz.de/thredds/fileServer/cordex/cordex/output/WAS-44/MPI-CSC/MPI-M-MPI-ESM-LR/historical/r1i1p1/MPI-CSC-REMO2009/v1/day/tasmax/v20140918/tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc"  # noqa
    local_path = "WAS-44/MPI-CSC/MPI-M-MPI-ESM-LR/historical/r1i1p1/MPI-CSC-REMO2009/v1/day/tasmax/v20140918/tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc"  # noqa
    archive_path = utils.esgf_archive_path(url)
    assert local_path in archive_path


@pytest.mark.skipif(reason="no way of currently testing this")
def test_esgf_archive_path_cmip5():
    url = "http://esgf1.dkrz.de/thredds/fileServer/cmip5/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20111006/tasmax/tasmax_Amon_MPI-ESM-LR_historical_r1i1p1_185001-200512.nc"  # noqa
    local_path = "MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20111006/tasmax/tasmax_Amon_MPI-ESM-LR_historical_r1i1p1_185001-200512.nc"  # noqa
    archive_path = utils.esgf_archive_path(url)
    assert local_path in archive_path


@pytest.mark.skipif(reason="no way of currently testing this")
def test_esgf_archive_path_cmip5_noaa():
    url = "http://esgdata.gfdl.noaa.gov/thredds/fileServer/gfdl_dataroot/NOAA-GFDL/GFDL-CM3/historical/mon/atmos/Amon/r1i1p1/v20110601/tasmax/tasmax_Amon_GFDL-CM3_historical_r1i1p1_200501-200512.nc"  # noqa
    local_path = "NOAA-GFDL/GFDL-CM3/historical/mon/atmos/Amon/r1i1p1/v20110601/tasmax/tasmax_Amon_GFDL-CM3_historical_r1i1p1_200501-200512.nc"  # noqa
    archive_path = utils.esgf_archive_path(url)
    assert archive_path is not None
    assert local_path in archive_path


def test_dupname():
    f = open('/tmp/_test_dupname.nc', 'w')
    f.close()
    newname = utils.dupname('/tmp', '_test_dupname')
    assert '_test_dupname_1' == newname

    import os
    os.remove('/tmp/_test_dupname.nc')
    newname = utils.dupname('/tmp', '_test_dupname')
    assert '_test_dupname' == newname


def test_user_id():
    user_id = utils.user_id("https://esgf-data.dkrz.de/esgf-idp/openid/jule")
    assert user_id == "jule_esgf-data.dkrz.de"

    try:
        user_id = utils.user_id("https://esgf-data.dkrz.de/bla/blu/jule")
        assert False
    except Exception:
        assert True


def test_within_date_range():
    timesteps = ["2013-11-01T12:00:00.000Z", "2013-11-01T18:00:00.000Z", "2013-11-02T12:00:00.000Z",
                 "2013-11-08T12:00:00.000Z", "2013-11-09T12:00:00.000Z", "2013-11-12T12:00:00.000Z"]

    result = utils.within_date_range(timesteps, start="2013-11-01T18:00:00Z", end="2013-11-09T12:00:00Z")
    assert len(result) == 4


def test_filter_timesteps():
    timesteps = ["2013-11-01T12:00:00.000Z", "2013-11-01T18:00:00.000Z", "2013-11-02T12:00:00.000Z",
                 "2013-11-08T12:00:00.000Z", "2013-11-09T12:00:00.000Z", "2013-11-12T12:00:00.000Z",
                 "2013-12-01T12:00:00.000Z", "2014-01-01T12:00:00.000Z"]

    result = utils.filter_timesteps(timesteps, aggregation="hourly")
    assert len(result) == 8

    result = utils.filter_timesteps(timesteps, aggregation="daily")
    assert len(result) == 7

    result = utils.filter_timesteps(timesteps,
                                    start="2013-11-01T12:00:00Z",
                                    end="2014-01-01T12:00:00Z",
                                    aggregation="weekly")
    assert len(result) == 5

    result = utils.filter_timesteps(timesteps, aggregation="monthly")
    assert len(result) == 3

    result = utils.filter_timesteps(timesteps, aggregation="yearly")
    assert len(result) == 2

    result = utils.filter_timesteps(timesteps, aggregation="hourly",
                                    start="2013-11-01T18:00:00Z",
                                    end="2013-12-01T12:00:00Z")
    assert len(result) == 6


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
    assert len(result) == 3


@pytest.mark.skipif(reason="no way of currently testing this")
def test_nc_copy():
    dap_url = "http://bmbf-ipcc-ar5.dkrz.de/thredds/dodsC/cmip5.output1.MPI-M.MPI-ESM-LR.esmHistorical.day.atmos.day.r1i1p1.tas.20120315.aggregation"  # noqa
    (_, nc_filename) = tempfile.mkstemp(suffix='.nc')
    istart = 10
    istop = 20
    utils.nc_copy(source=dap_url, target=nc_filename, istart=istart, istop=istop)

    nc_file = Dataset(nc_filename)
    assert 'time' in nc_file.dimensions.keys()
    time_dim = nc_file.dimensions.get('time')
    assert len(time_dim) == istop - istart
    assert 'tas' in nc_file.variables.keys()
