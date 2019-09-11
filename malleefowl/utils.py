"""
Utility functions for WPS processes.
"""

from netCDF4 import Dataset
import os

from malleefowl import config

import logging
LOGGER = logging.getLogger("PYWPS")


def esgf_archive_path(url):
    from os.path import join, isfile

    archive_path = None

    if 'thredds/fileServer/' in url:
        url_path = url.split('thredds/fileServer/')[1]
        LOGGER.debug('check thredds archive: url_path=%s', url_path)
        # TODO: workaround for different archive paths at esgf data nodes
        if config.archive_node() == config.IPSL_NODE:
            # cmip5/output1 -> CMIP5/output1
            rel_path = url_path.split('/')[0].upper() + '/' + '/'.join(url_path.split('/')[1:])
        elif config.archive_node() == config.DKRZ_NODE:
            # cmip5/cmip5/output1 -> cmip5/output1
            rel_path = '/'.join(url_path.split('/')[1:])
        else:
            # like dkrz
            rel_path = '/'.join(url_path.split('/')[1:])
        for root_path in config.archive_root():
            file_path = join(root_path, rel_path)
            LOGGER.debug('file_path = %s', file_path)
            if isfile(file_path):
                LOGGER.info('found in archive: %s', url)
                archive_path = 'file://' + file_path
                break
        if archive_path is None:
            LOGGER.info('not found in archive: %s', url)
    return archive_path


def dupname(path, filename):
    """
    avoid dupliate filenames
    TODO: needs to be improved
    """
    newname = filename
    count = sum(1 for fname in os.listdir(path) if filename in fname)
    if count > 0:
        return newname + '_' + str(count)
    return newname


def user_id(openid):
    """generate user_id from openid"""

    import re

    ESGF_OPENID_REXP = r'https://(.*)/esgf-idp/openid/(.*)'

    user_id = None
    mo = re.match(ESGF_OPENID_REXP, openid)
    try:
        hostname = mo.group(1)
        username = mo.group(2)
        user_id = "%s_%s" % (username, hostname)
    except Exception:
        raise Exception("unsupported openid")
    return user_id


def within_date_range(timesteps, start=None, end=None):
    from dateutil.parser import parse as date_parser
    start_date = None
    if start is not None:
        start_date = date_parser(start)
    end_date = None
    if end is not None:
        end_date = date_parser(end)
    new_timesteps = []
    for timestep in timesteps:
        candidate = date_parser(timestep)
        # within time range?
        if start_date is not None and candidate < start_date:
            continue
        if end_date is not None and candidate > end_date:
            break
        new_timesteps.append(timestep)
    return new_timesteps


def filter_timesteps(timesteps, aggregation="monthly", start=None, end=None):
    from dateutil.parser import parse as date_parser
    LOGGER.debug("aggregation: %s", aggregation)

    if (timesteps is None or len(timesteps) == 0):
        return []
    timesteps.sort()
    work_timesteps = within_date_range(timesteps, start, end)

    new_timesteps = [work_timesteps[0]]

    for index in range(1, len(work_timesteps)):
        current = date_parser(new_timesteps[-1])
        candidate = date_parser(work_timesteps[index])

        if current.year < candidate.year:
            new_timesteps.append(work_timesteps[index])
        elif current.year == candidate.year:
            if aggregation == "daily":
                if current.timetuple()[7] == candidate.timetuple()[7]:
                    continue
            elif aggregation == "weekly":
                if current.isocalendar()[1] == candidate.isocalendar()[1]:
                    continue
            elif aggregation == "monthly":
                if current.month == candidate.month:
                    continue
            elif aggregation == "yearly":
                if current.year == candidate.year:
                    continue
            # all checks passed
            new_timesteps.append(work_timesteps[index])
        else:
            continue
    return new_timesteps


def nc_copy(source, target, overwrite=True, time_dimname='time', nchunk=10, istart=0, istop=-1, format='NETCDF3_64BIT'):
    """copy netcdf file from opendap to netcdf3 file

     :param overwrite:

          Overwite destination file (default is to raise an error if output file already exists).

     :param format:

          netcdf3 format to use (NETCDF3_64BIT by default, can be set to NETCDF3_CLASSIC)

     :param chunk:

          number of records along unlimited dimension to
          write at once. Default 10. Ignored if there is no unlimited
          dimension. chunk=0 means write all the data at once.

     :param istart:

          number of record to start at along unlimited dimension.
          Default 0.  Ignored if there is no unlimited dimension.

     :param istop:

          number of record to stop at along unlimited dimension.
          Default -1.  Ignored if there is no unlimited dimension.
    """

    nc_in = Dataset(source, 'r')
    nc_out = Dataset(target, 'w', clobber=overwrite, format=format)

    # create dimensions. Check for unlimited dim.
    unlimdimname = False
    unlimdim = None

    # create global attributes.
    LOGGER.info('copying global attributes ...')
    nc_out.setncatts(nc_in.__dict__)
    LOGGER.info('copying dimensions ...')
    for dimname, dim in nc_in.dimensions.items():
        if dim.isunlimited() or dimname == time_dimname:
            unlimdimname = dimname
            unlimdim = dim
            if istop == -1:
                istop = len(unlimdim)
            nc_out.createDimension(dimname, istop - istart)
            LOGGER.debug('unlimited dimension = %s, length = %d', unlimdimname, len(unlimdim))
        else:
            nc_out.createDimension(dimname, len(dim))

    # create variables.
    for varname, ncvar in nc_in.variables.items():
        LOGGER.info('copying variable %s', varname)
        # is there an unlimited dimension?
        if unlimdimname and unlimdimname in ncvar.dimensions:
            hasunlimdim = True
        else:
            hasunlimdim = False
        if hasattr(ncvar, '_FillValue'):
            FillValue = ncvar._FillValue
        else:
            FillValue = None
        var = nc_out.createVariable(varname, ncvar.dtype, ncvar.dimensions, fill_value=FillValue)
        # fill variable attributes.
        attdict = ncvar.__dict__
        if '_FillValue' in attdict:
            del attdict['_FillValue']
        var.setncatts(attdict)
        if hasunlimdim:  # has an unlim dim, loop over unlim dim index.
            # range to copy
            if nchunk:
                start = istart
                stop = istop
                step = nchunk
                if step < 1:
                    step = 1
                for n in range(start, stop, step):
                    nmax = n + nchunk
                    if nmax > istop:
                        nmax = istop
                    LOGGER.debug('copy chunk [%d:%d]', n, nmax)
                    try:
                        var[n - istart:nmax - istart] = ncvar[n:nmax]
                    except Exception:
                        msg = "n=%d nmax=%d istart=%d istop=%d" % (n, nmax, istart, istop)
                        raise Exception(msg)
            else:
                var[0:istop - istart] = ncvar[:]
        else:  # no unlim dim or 1-d variable, just copy all data at once.
            var[:] = ncvar[:]
        nc_out.sync()  # flush data to disk
    # close files.
    nc_out.close()
    nc_in.close()
